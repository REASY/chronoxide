use super::query_reader::{GenericCrossSegmentPlan, NativeTypedCrossSegmentPlan};
use super::*;

pub(super) struct SegmentQuerySessionReader<'a> {
    pub(super) reader: &'a SegmentReader,
    pub(super) context: Option<SegmentQueryContext>,
    pub(super) index_routing_reader: Option<SegmentIndexReader<File>>,
    pub(super) stats: SegmentStoreQuerySessionStats,
    pub(super) profile: SegmentStoreQueryProfile,
    pub(super) chunk_reader: Arc<crate::storage::io::ChunkReader>,
}

const CHUNK_PAYLOAD_COALESCE_MAX_GAP: u64 = 4096;

pub(super) struct SegmentQueryContext {
    pub(super) symbols: Arc<SegmentSymbols>,
    pub(super) index_reader: SegmentIndexReader<File>,
    pub(super) series_reader: Option<SeriesReader<File>>,
    pub(super) chunk_index_reader: Option<ChunkIndexReader>,
    pub(super) chunk_file: Option<Arc<File>>,
    pub(super) chunk_reader: Arc<crate::storage::io::ChunkReader>,
    pub(super) stats: SegmentStoreQuerySessionStats,
    pub(super) profile: SegmentStoreQueryProfile,
}

impl SegmentQueryContext {
    pub(super) fn open(
        reader: &SegmentReader,
        index_reader: Option<SegmentIndexReader<File>>,
    ) -> io::Result<Self> {
        let chunk_reader = Arc::new(crate::storage::io::ChunkReader::new(
            crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 1,
            },
        )?);
        Self::open_with_chunk_reader(reader, index_reader, chunk_reader)
    }

    fn open_with_chunk_reader(
        reader: &SegmentReader,
        index_reader: Option<SegmentIndexReader<File>>,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
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
                profile.index_read_stats = profile
                    .index_read_stats
                    .saturating_add(cached.open_read_stats);
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
            chunk_reader,
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

    pub(super) fn chunk_file(&mut self, reader: &SegmentReader) -> io::Result<&Arc<File>> {
        if self.chunk_file.is_none() {
            let path = reader.file_path(SegmentFile::Chunks);
            self.profile.chunks_file_bytes = self
                .profile
                .chunks_file_bytes
                .saturating_add(file_len(&path)?);
            let start = Instant::now();
            self.chunk_file = Some(Arc::new(reader.open_chunks()?));
            self.profile.chunks_open = self.profile.chunks_open.saturating_add(start.elapsed());
            self.stats.chunks_bin_opens = self.stats.chunks_bin_opens.saturating_add(1);
        }
        Ok(self.chunk_file.as_ref().unwrap())
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
        self.observe_chunk_payload_requests(requests);
        self.read_chunk_payload_batch_physical(reader, requests)
    }

    pub(super) fn observe_chunk_payload_requests(&mut self, requests: &[ChunkPayloadRead]) {
        let mut logical_ranges = Vec::with_capacity(requests.len());
        for request in requests {
            self.profile
                .observe_chunk_payload_read(request.offset, request.len);
            logical_ranges.push((request.offset, request.len));
        }
        self.profile
            .observe_sorted_chunk_payload_ranges(&mut logical_ranges);
    }

    pub(super) fn read_chunk_payload_batch_physical(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<ChunkPayloadBatch> {
        if requests.is_empty() {
            return Ok(ChunkPayloadBatch::empty());
        }

        let file = Arc::clone(self.chunk_file(reader)?);
        let plan = plan_chunk_payload_batch(requests, CHUNK_PAYLOAD_COALESCE_MAX_GAP)?;
        let scheduler = ChunkReadScheduler::new(Arc::clone(&self.chunk_reader));
        let (mut results, scheduler_stats) = scheduler.execute(vec![ChunkReadSchedulerItem {
            segment_ordinal: 0,
            file,
            plan,
            logical_requests: requests.len() as u64,
        }])?;
        self.observe_chunk_read_scheduler(scheduler_stats);
        self.profile.chunk_read = self
            .profile
            .chunk_read
            .saturating_add(scheduler_stats.read_duration);
        let batch = results
            .pop()
            .expect("non-empty chunk scheduler plan must return one result")
            .payloads;
        self.profile.observe_chunk_payload_physical_reads(
            batch.physical_read_count(),
            batch.physical_bytes_read(),
        );
        Ok(batch)
    }

    fn plan_cross_segment_chunk_payload_batch(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<(Arc<File>, ChunkPayloadBatchPlan)> {
        self.observe_chunk_payload_requests(requests);
        let plan = plan_chunk_payload_batch(requests, CHUNK_PAYLOAD_COALESCE_MAX_GAP)?;
        let file = Arc::clone(self.chunk_file(reader)?);
        Ok((file, plan))
    }

    fn observe_cross_segment_chunk_payload_read(
        &mut self,
        duration: Duration,
        plan: &ChunkPayloadBatchPlan,
    ) {
        self.profile.chunk_read = self.profile.chunk_read.saturating_add(duration);
        self.profile.observe_chunk_payload_physical_reads(
            plan.physical_read_count(),
            plan.physical_bytes_read(),
        );
    }

    fn observe_chunk_read_scheduler(&mut self, stats: ChunkReadSchedulerStats) {
        let profile = &mut self.profile.chunk_read_scheduler;
        profile.executions = profile.executions.saturating_add(stats.executions);
        match stats.backend {
            Some(ChunkReadSchedulerBackend::Pread) => {
                profile.pread_decisions = profile.pread_decisions.saturating_add(1)
            }
            Some(ChunkReadSchedulerBackend::IoUring) => {
                profile.io_uring_decisions = profile.io_uring_decisions.saturating_add(1)
            }
            None => {}
        }
        profile.logical_requests = profile
            .logical_requests
            .saturating_add(stats.logical_requests);
        profile.physical_spans = profile.physical_spans.saturating_add(stats.physical_spans);
        profile.backend_submissions = profile
            .backend_submissions
            .saturating_add(stats.backend_submissions);
        profile.sqes_submitted = profile.sqes_submitted.saturating_add(stats.sqes_submitted);
        profile.submission_depth_sum = profile
            .submission_depth_sum
            .saturating_add(stats.submission_depth_sum);
        profile.submission_depth_max = profile.submission_depth_max.max(stats.submission_depth_max);
        profile.submission_depth_1 = profile
            .submission_depth_1
            .saturating_add(stats.submission_depth_1);
        profile.submission_depth_2_3 = profile
            .submission_depth_2_3
            .saturating_add(stats.submission_depth_2_3);
        profile.submission_depth_4_7 = profile
            .submission_depth_4_7
            .saturating_add(stats.submission_depth_4_7);
        profile.submission_depth_8_plus = profile
            .submission_depth_8_plus
            .saturating_add(stats.submission_depth_8_plus);
        profile.in_flight_bytes = profile.in_flight_bytes.saturating_add(stats.physical_bytes);
        profile.peak_in_flight_bytes = profile.peak_in_flight_bytes.max(stats.peak_in_flight_bytes);
    }

    pub(super) fn prefetch_chunk_range(
        &mut self,
        reader: &SegmentReader,
        offset: u64,
        len: u64,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let start = Instant::now();
        let mut file = self.chunk_file(reader)?.try_clone()?;
        prefetch_file_range(&mut file, offset, len, scratch)?;
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
) -> io::Result<Result<Vec<ResolvedEqualityMatcher>, SegmentPruneReason>> {
    let mut equality_matchers = Vec::new();
    for matcher in matchers {
        let NormalizedMatcher::Eq { name, value } = matcher else {
            continue;
        };
        let Some(name_sym) = context.symbols.lookup(name) else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let Some(value_sym) = context.symbols.lookup(value) else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let Some(selection) = context
            .index_reader
            .select_exact_postings(name_sym, value_sym)?
        else {
            return Ok(Err(SegmentPruneReason::MissingEquality));
        };
        let postings = selection.metadata();
        if !postings.time_range.overlaps(start_ms, end_ms) {
            return Ok(Err(SegmentPruneReason::MatcherTimeRange));
        }
        equality_matchers.push(ResolvedEqualityMatcher {
            name_sym,
            value_sym,
            postings,
            selection,
        });
    }
    equality_matchers.sort_by_key(|matcher| matcher.postings.byte_len);
    Ok(Ok(equality_matchers))
}

pub(super) fn has_positive_equality_matcher(matchers: &[NormalizedMatcher]) -> bool {
    matchers
        .iter()
        .any(|matcher| matches!(matcher, NormalizedMatcher::Eq { .. }))
}

impl<'a> SegmentQuerySessionReader<'a> {
    pub(super) fn open(
        reader: &'a SegmentReader,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> Self {
        Self {
            reader,
            context: None,
            index_routing_reader: None,
            stats: SegmentStoreQuerySessionStats::default(),
            profile: SegmentStoreQueryProfile::default(),
            chunk_reader,
        }
    }

    pub(super) fn context(&mut self) -> io::Result<&mut SegmentQueryContext> {
        if self.context.is_none() {
            let index_reader = self.index_routing_reader.take();
            self.context = Some(SegmentQueryContext::open_with_chunk_reader(
                self.reader,
                index_reader,
                Arc::clone(&self.chunk_reader),
            )?);
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
            self.profile.index_read_stats = self
                .profile
                .index_read_stats
                .saturating_add(cached.open_read_stats);
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
        segment_ordinal: usize,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
        projected_label_cache: &mut ProjectedLabelCache,
        cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
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
            segment_ordinal,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
            label_cache,
            projected_label_cache,
            cache_call,
        )
    }

    pub(super) fn plan_generic_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<GenericCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(GenericCrossSegmentPlan::empty(selector.projection.clone()));
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(GenericCrossSegmentPlan::empty(selector.projection.clone()));
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.plan_generic_cross_segment_with_context(
            context,
            &matchers,
            &selector.projection,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(super) fn query_native_histogram_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<Vec<PromqlHistogramSeries>> {
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
        reader.query_native_histogram_normalized_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(super) fn plan_native_histogram_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.plan_native_histogram_cross_segment_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(super) fn plan_native_exponential_histogram_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<NativeTypedCrossSegmentPlan> {
        let matchers = selector.normalized_matchers();
        if self.context.is_none() && has_positive_equality_matcher(&matchers) {
            if let Some(plan) = self
                .plan_positive_equality_matchers_from_routing_index(&matchers, start_ms, end_ms)?
            {
                match plan {
                    Ok(()) => {}
                    Err(SegmentPruneReason::MissingEquality) => {
                        budget.observe_segment_skipped_by_missing_equality();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                    Err(SegmentPruneReason::MatcherTimeRange) => {
                        budget.observe_segment_skipped_by_matcher_time_range();
                        return Ok(NativeTypedCrossSegmentPlan {
                            series: Vec::new(),
                            payload_requests: Vec::new(),
                        });
                    }
                }
            }
        }
        let reader = self.reader;
        let context = self.context()?;
        reader.plan_native_exponential_histogram_cross_segment_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
        )
    }

    pub(super) fn query_native_exponential_histogram_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        label_cache: &mut SeriesLabelCache,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
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
        reader.query_native_exponential_histogram_normalized_with_context(
            context,
            &matchers,
            start_ms,
            end_ms,
            budget,
            label_cache,
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
        if plan_positive_equality_matchers(context, &matchers, start_ms, end_ms)?.is_err() {
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

struct CrossSegmentGenericRead {
    segment_ordinal: usize,
    generic_plan: GenericCrossSegmentPlan,
    payload_plan: ChunkPayloadBatchPlan,
    file: Arc<File>,
}

fn execute_cross_segment_generic_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentGenericRead>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
    projected_label_cache: &mut ProjectedLabelCache,
) -> io::Result<Vec<SegmentQueryResult>> {
    if group.is_empty() {
        return Ok(Vec::new());
    }
    let scheduler = ChunkReadScheduler::new(chunk_reader);
    let scheduler_items = group
        .iter()
        .map(|planned| ChunkReadSchedulerItem {
            segment_ordinal: planned.segment_ordinal,
            file: Arc::clone(&planned.file),
            plan: planned.payload_plan.clone(),
            logical_requests: planned.generic_plan.payload_requests.len() as u64,
        })
        .collect();
    let (payload_results, scheduler_stats) = scheduler.execute(scheduler_items)?;
    let first_segment_ordinal = group[0].segment_ordinal;
    segments[first_segment_ordinal]
        .context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .observe_chunk_read_scheduler(scheduler_stats);
    let mut duration_unassigned = Some(scheduler_stats.read_duration);
    let mut results = Vec::new();
    for (planned, payload_result) in group.into_iter().zip(payload_results) {
        if payload_result.segment_ordinal != planned.segment_ordinal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk scheduler changed segment result order",
            ));
        }
        let context = segments[planned.segment_ordinal]
            .context
            .as_mut()
            .expect("cross-segment plan requires an open context");
        context.observe_cross_segment_chunk_payload_read(
            duration_unassigned.take().unwrap_or(Duration::ZERO),
            &planned.payload_plan,
        );
        results.extend(
            segments[planned.segment_ordinal]
                .reader
                .decode_generic_cross_segment_plan(
                    planned.generic_plan,
                    &payload_result.payloads,
                    start_ms,
                    end_ms,
                    budget,
                    projected_label_cache,
                )?,
        );
    }
    Ok(results)
}

struct CrossSegmentNativeRead {
    segment_ordinal: usize,
    native_plan: NativeTypedCrossSegmentPlan,
    payload_plan: ChunkPayloadBatchPlan,
    file: Arc<File>,
}

fn fetch_cross_segment_native_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentNativeRead>,
) -> io::Result<Vec<(usize, NativeTypedCrossSegmentPlan, ChunkPayloadBatch)>> {
    if group.is_empty() {
        return Ok(Vec::new());
    }

    let scheduler = ChunkReadScheduler::new(chunk_reader);
    let scheduler_items = group
        .iter()
        .map(|planned| ChunkReadSchedulerItem {
            segment_ordinal: planned.segment_ordinal,
            file: Arc::clone(&planned.file),
            plan: planned.payload_plan.clone(),
            logical_requests: planned.native_plan.payload_requests.len() as u64,
        })
        .collect();
    let (payload_results, scheduler_stats) = scheduler.execute(scheduler_items)?;
    let first_segment_ordinal = group[0].segment_ordinal;
    segments[first_segment_ordinal]
        .context
        .as_mut()
        .expect("cross-segment plan requires an open context")
        .observe_chunk_read_scheduler(scheduler_stats);
    let mut duration_unassigned = Some(scheduler_stats.read_duration);
    let mut fetched = Vec::with_capacity(group.len());
    for (planned, payload_result) in group.into_iter().zip(payload_results) {
        if payload_result.segment_ordinal != planned.segment_ordinal {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk scheduler changed segment result order",
            ));
        }
        let context = segments[planned.segment_ordinal]
            .context
            .as_mut()
            .expect("cross-segment plan requires an open context");
        context.observe_cross_segment_chunk_payload_read(
            duration_unassigned.take().unwrap_or(Duration::ZERO),
            &planned.payload_plan,
        );
        fetched.push((
            planned.segment_ordinal,
            planned.native_plan,
            payload_result.payloads,
        ));
    }
    Ok(fetched)
}

fn execute_cross_segment_native_histogram_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentNativeRead>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
) -> io::Result<Vec<PromqlHistogramSeries>> {
    let mut results = Vec::new();
    for (segment_ordinal, native_plan, payloads) in
        fetch_cross_segment_native_reads(segments, chunk_reader, group)?
    {
        results.extend(
            segments[segment_ordinal]
                .reader
                .decode_native_histogram_cross_segment_plan(
                    native_plan,
                    &payloads,
                    start_ms,
                    end_ms,
                    budget,
                )?,
        );
    }
    Ok(results)
}

fn execute_cross_segment_native_exponential_histogram_reads(
    segments: &mut [SegmentQuerySessionReader<'_>],
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    group: Vec<CrossSegmentNativeRead>,
    start_ms: u64,
    end_ms: u64,
    budget: &mut QueryBudget,
) -> io::Result<Vec<PromqlExponentialHistogramSeries>> {
    let mut results = Vec::new();
    for (segment_ordinal, native_plan, payloads) in
        fetch_cross_segment_native_reads(segments, chunk_reader, group)?
    {
        results.extend(
            segments[segment_ordinal]
                .reader
                .decode_native_exponential_histogram_cross_segment_plan(
                    native_plan,
                    &payloads,
                    start_ms,
                    end_ms,
                    budget,
                )?,
        );
    }
    Ok(results)
}

impl<'a> SegmentStoreQuerySession<'a> {
    fn should_use_cross_segment_flow(&self, start_ms: u64, end_ms: u64) -> bool {
        let Some(reader) = self.segments.first().map(|segment| &segment.chunk_reader) else {
            return false;
        };
        if reader.configured_mode() != crate::storage::io::ChunkReadMode::Auto {
            return true;
        }
        self.segments
            .iter()
            .filter(|segment| {
                segment.reader.meta.end_ms >= start_ms && segment.reader.meta.start_ms <= end_ms
            })
            .take(CHUNK_READ_AUTO_MIN_SPANS as usize)
            .count()
            >= CHUNK_READ_AUTO_MIN_SPANS as usize
    }

    pub(super) fn open(store: &'a SegmentStoreReader) -> io::Result<Self> {
        let chunk_reader = Arc::new(crate::storage::io::ChunkReader::new(
            crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 1,
            },
        )?);
        let mut segments = Vec::with_capacity(store.segments.len());
        for segment in &store.segments {
            segments.push(SegmentQuerySessionReader::open(
                segment,
                Arc::clone(&chunk_reader),
            ));
        }
        Ok(Self {
            query_projection_config: store.query_projection_config.clone(),
            segments,
            label_cache: SeriesLabelCache::default(),
            projected_label_cache: ProjectedLabelCache::default(),
            range_scalar_cache_budget_bytes: DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES,
            range_scalar_cache_governor:
                super::range_scalar_cache::process_range_scalar_cache_governor(),
            last_range_scalar_cache_summary: None,
            experimental_cross_segment_chunk_reads: false,
        })
    }

    pub fn set_chunk_read_config(
        &mut self,
        config: crate::storage::io::ChunkReadConfig,
    ) -> io::Result<()> {
        self.set_chunk_reader(Arc::new(crate::storage::io::ChunkReader::new(config)?))
    }

    pub fn set_chunk_reader(
        &mut self,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> io::Result<()> {
        if self
            .segments
            .iter()
            .any(|segment| segment.context.is_some())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk read configuration must be set before opening query contexts",
            ));
        }
        for segment in &mut self.segments {
            segment.chunk_reader = Arc::clone(&chunk_reader);
        }
        Ok(())
    }

    pub fn set_experimental_cross_segment_chunk_reads(&mut self, enabled: bool) {
        self.experimental_cross_segment_chunk_reads = enabled;
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
        self.query_selectors_with_limits_with_cache(selectors, start_ms, end_ms, limits, None)
    }

    pub(super) fn query_selectors_with_limits_with_cache(
        &mut self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        let mut seen_branches = BTreeMap::new();
        for selector in selectors {
            let selector_results = self.query_selector_with_budget_with_cache(
                selector,
                start_ms,
                end_ms,
                &mut budget,
                cache_call.as_deref_mut(),
            )?;
            observe_promql_selector_branch_conflicts(
                &mut seen_branches,
                selector,
                &selector_results,
            )?;
            results.extend(selector_results);
        }
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub(super) fn query_native_histogram_selector_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlHistogramSeries>, QueryStats), PromqlQueryError> {
        if self.experimental_cross_segment_chunk_reads
            && self.should_use_cross_segment_flow(start_ms, end_ms)
        {
            return self.query_native_histogram_selector_cross_segment_with_limits(
                selector, start_ms, end_ms, limits,
            );
        }
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        let label_cache = &mut self.label_cache;
        for segment in &mut self.segments {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_histogram_with_budget(
                        selector,
                        start_ms,
                        end_ms,
                        &mut budget,
                        label_cache,
                    )
                    .map_err(promql_error_from_query_io)?,
            );
        }

        Ok((merge_histogram_query_results(results), budget.stats()))
    }

    fn query_native_histogram_selector_cross_segment_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlHistogramSeries>, QueryStats), PromqlQueryError> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }
        let Some(chunk_reader) = self
            .segments
            .first()
            .map(|segment| Arc::clone(&segment.chunk_reader))
        else {
            return Ok((results, budget.stats()));
        };

        let mut group = Vec::new();
        let mut group_spans = 0u64;
        let mut group_bytes = 0u64;
        let mut deferred_error = None;

        for segment_ordinal in 0..self.segments.len() {
            budget.observe_segment_considered();
            let segment = &self.segments[segment_ordinal];
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            let planned = {
                let segment = &mut self.segments[segment_ordinal];
                segment.plan_native_histogram_cross_segment_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    &mut budget,
                    &mut self.label_cache,
                )
            };
            let native_plan = match planned {
                Ok(plan) => plan,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            if native_plan.payload_requests.is_empty() {
                continue;
            }

            let physical = {
                let segment = &mut self.segments[segment_ordinal];
                let reader = segment.reader;
                let context = segment
                    .context
                    .as_mut()
                    .expect("native histogram plan requires an open context");
                context
                    .plan_cross_segment_chunk_payload_batch(reader, &native_plan.payload_requests)
            };
            let (file, payload_plan) = match physical {
                Ok(physical) => physical,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            let item_spans = payload_plan.physical_read_count();
            let item_bytes = payload_plan.physical_bytes_read();

            if chunk_read_group_would_exceed_bounds(
                group.len(),
                group_spans,
                group_bytes,
                item_spans,
                item_bytes,
            ) {
                results.extend(
                    execute_cross_segment_native_histogram_reads(
                        &mut self.segments,
                        Arc::clone(&chunk_reader),
                        std::mem::take(&mut group),
                        start_ms,
                        end_ms,
                        &mut budget,
                    )
                    .map_err(promql_error_from_query_io)?,
                );
                group_spans = 0;
                group_bytes = 0;
            }

            group_spans = group_spans.saturating_add(item_spans);
            group_bytes = group_bytes.saturating_add(item_bytes);
            group.push(CrossSegmentNativeRead {
                segment_ordinal,
                native_plan,
                payload_plan,
                file,
            });
        }

        results.extend(
            execute_cross_segment_native_histogram_reads(
                &mut self.segments,
                chunk_reader,
                group,
                start_ms,
                end_ms,
                &mut budget,
            )
            .map_err(promql_error_from_query_io)?,
        );
        if let Some(error) = deferred_error {
            return Err(promql_error_from_query_io(error));
        }
        Ok((merge_histogram_query_results(results), budget.stats()))
    }

    pub(super) fn query_native_exponential_histogram_selector_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlExponentialHistogramSeries>, QueryStats), PromqlQueryError> {
        if self.experimental_cross_segment_chunk_reads
            && self.should_use_cross_segment_flow(start_ms, end_ms)
        {
            return self.query_native_exponential_histogram_selector_cross_segment_with_limits(
                selector, start_ms, end_ms, limits,
            );
        }
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        let label_cache = &mut self.label_cache;
        for segment in &mut self.segments {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_exponential_histogram_with_budget(
                        selector,
                        start_ms,
                        end_ms,
                        &mut budget,
                        label_cache,
                    )
                    .map_err(promql_error_from_query_io)?,
            );
        }

        Ok((
            merge_exponential_histogram_query_results(results),
            budget.stats(),
        ))
    }

    fn query_native_exponential_histogram_selector_cross_segment_with_limits(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlExponentialHistogramSeries>, QueryStats), PromqlQueryError> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }
        let Some(chunk_reader) = self
            .segments
            .first()
            .map(|segment| Arc::clone(&segment.chunk_reader))
        else {
            return Ok((results, budget.stats()));
        };

        let mut group = Vec::new();
        let mut group_spans = 0u64;
        let mut group_bytes = 0u64;
        let mut deferred_error = None;

        for segment_ordinal in 0..self.segments.len() {
            budget.observe_segment_considered();
            let segment = &self.segments[segment_ordinal];
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            let planned = {
                let segment = &mut self.segments[segment_ordinal];
                segment.plan_native_exponential_histogram_cross_segment_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    &mut budget,
                    &mut self.label_cache,
                )
            };
            let native_plan = match planned {
                Ok(plan) => plan,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            if native_plan.payload_requests.is_empty() {
                continue;
            }

            let physical = {
                let segment = &mut self.segments[segment_ordinal];
                let reader = segment.reader;
                let context = segment
                    .context
                    .as_mut()
                    .expect("native exponential histogram plan requires an open context");
                context
                    .plan_cross_segment_chunk_payload_batch(reader, &native_plan.payload_requests)
            };
            let (file, payload_plan) = match physical {
                Ok(physical) => physical,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            let item_spans = payload_plan.physical_read_count();
            let item_bytes = payload_plan.physical_bytes_read();

            if chunk_read_group_would_exceed_bounds(
                group.len(),
                group_spans,
                group_bytes,
                item_spans,
                item_bytes,
            ) {
                results.extend(
                    execute_cross_segment_native_exponential_histogram_reads(
                        &mut self.segments,
                        Arc::clone(&chunk_reader),
                        std::mem::take(&mut group),
                        start_ms,
                        end_ms,
                        &mut budget,
                    )
                    .map_err(promql_error_from_query_io)?,
                );
                group_spans = 0;
                group_bytes = 0;
            }

            group_spans = group_spans.saturating_add(item_spans);
            group_bytes = group_bytes.saturating_add(item_bytes);
            group.push(CrossSegmentNativeRead {
                segment_ordinal,
                native_plan,
                payload_plan,
                file,
            });
        }

        results.extend(
            execute_cross_segment_native_exponential_histogram_reads(
                &mut self.segments,
                chunk_reader,
                group,
                start_ms,
                end_ms,
                &mut budget,
            )
            .map_err(promql_error_from_query_io)?,
        );
        if let Some(error) = deferred_error {
            return Err(promql_error_from_query_io(error));
        }
        Ok((
            merge_exponential_histogram_query_results(results),
            budget.stats(),
        ))
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
            let mut segment_profile = segment.profile;
            if let Some(index_reader) = &segment.index_routing_reader {
                segment_profile.index_read_stats = segment_profile
                    .index_read_stats
                    .saturating_add(index_reader.read_stats());
            }
            profile.add(segment_profile);
            if let Some(context) = &segment.context {
                let mut context_profile = context.profile;
                context_profile.index_read_stats = context_profile
                    .index_read_stats
                    .saturating_add(context.index_reader.read_stats());
                profile.add(context_profile);
            }
        }
        profile
    }

    pub fn set_range_scalar_cache_budget_bytes(
        &mut self,
        bytes: u64,
    ) -> Result<(), RangeScalarCacheConfigError> {
        validate_range_scalar_cache_budget_bytes(bytes)?;
        self.range_scalar_cache_budget_bytes = bytes;
        Ok(())
    }

    pub fn last_range_scalar_cache_summary(&self) -> Option<&RangeScalarCacheSummary> {
        self.last_range_scalar_cache_summary.as_ref()
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

    pub fn query_promql_at(
        &mut self,
        query: &str,
        evaluation_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        self.query_promql_at_with_limits(query, evaluation_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_at_with_limits(
        &mut self,
        query: &str,
        evaluation_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let query = parse_query(query)?;
        let mut execution = self.execute_promql_instant_query(&query, evaluation_ms, limits)?;
        execution.results = retimestamp_instant_results(execution.results, evaluation_ms);
        Ok(execution)
    }

    pub fn query_promql_range(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        self.query_promql_range_with_limits(
            query,
            start_ms,
            end_ms,
            step_ms,
            QueryLimits::unlimited(),
        )
        .map(|execution| execution.results)
    }

    pub fn query_promql_range_with_limits(
        &mut self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.last_range_scalar_cache_summary = None;
        let mut cache_call = super::range_scalar_cache::RangeScalarCacheCall::new(
            self.range_scalar_cache_budget_bytes,
            Arc::clone(&self.range_scalar_cache_governor),
        );
        let result = (|| {
            let query = parse_query(query)?;
            validate_promql_range_bounds(start_ms, end_ms, step_ms)?;
            self.execute_validated_promql_range_query(
                &query,
                start_ms,
                end_ms,
                step_ms,
                limits,
                &mut cache_call,
            )
        })();
        self.last_range_scalar_cache_summary = Some(cache_call.finish());
        result
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
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prewarm_promql_instant_query(&aggregation.input, end_ms)
            }
            PromqlQuery::Absent(absent) => self.prewarm_promql_instant_query(&absent.input, end_ms),
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Offset(offset) => self.prewarm_promql_instant_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => Ok(()),
            PromqlQuery::BinaryExpression(expression) => {
                self.prewarm_promql_binary_expression(expression, end_ms)
            }
        }
    }

    fn prewarm_promql_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
    ) -> Result<(), PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
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
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prewarm_promql_instant_query(&aggregation.input, end_ms)
            }
            PromqlQuery::Absent(absent) => self.prewarm_promql_instant_query(&absent.input, end_ms),
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prewarm_selectors(&selectors, range_start_ms, end_ms)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Offset(offset) => self.prewarm_promql_instant_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prewarm_promql_instant_query(&function.input, end_ms)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => Ok(()),
            PromqlQuery::BinaryExpression(expression) => {
                self.prewarm_promql_binary_expression(expression, end_ms)
            }
        }
    }

    fn prewarm_promql_binary_expression(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
    ) -> Result<(), PromqlQueryError> {
        for query in binary_expression_vector_sides(expression) {
            self.prewarm_promql_instant_query(query, end_ms)?;
        }
        Ok(())
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
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prefetch_promql_instant_data_query(&aggregation.input, end_ms, limits)
            }
            PromqlQuery::Absent(absent) => {
                self.prefetch_promql_instant_data_query(&absent.input, end_ms, limits)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Offset(offset) => self.prefetch_promql_instant_data_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
                limits,
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => {
                Ok(QueryDataPrefetchStats::default())
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.prefetch_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    fn prefetch_promql_instant_data_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
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
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Aggregation(aggregation) => {
                self.prefetch_promql_instant_data_query(&aggregation.input, end_ms, limits)
            }
            PromqlQuery::Absent(absent) => {
                self.prefetch_promql_instant_data_query(&absent.input, end_ms, limits)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                self.prefetch_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::InstantFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::ScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Offset(offset) => self.prefetch_promql_instant_data_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
                limits,
            ),
            PromqlQuery::LabelReplace(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::LabelJoin(function) => {
                self.prefetch_promql_instant_data_query(&function.input, end_ms, limits)
            }
            PromqlQuery::Scalar(_) | PromqlQuery::Time | PromqlQuery::VectorFunction(_) => {
                Ok(QueryDataPrefetchStats::default())
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.prefetch_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    fn prefetch_promql_binary_expression(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryDataPrefetchStats, PromqlQueryError> {
        let mut stats = QueryDataPrefetchStats::default();
        for query in binary_expression_vector_sides(expression) {
            stats.merge_from(self.prefetch_promql_instant_data_query(query, end_ms, limits)?);
        }
        stats.query_stats.check_limits(limits)?;
        Ok(stats)
    }

    fn evaluate_promql_vector_function(
        &self,
        function: &PromqlVectorFunction,
        end_ms: u64,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let Some(value) = scalar_expression_value(&function.input, end_ms) else {
            return Err(PromqlQueryError::Invalid(
                "vector() requires a scalar expression".to_string(),
            ));
        };
        Ok(QueryExecution {
            results: evaluate_scalar(value, end_ms),
            stats: QueryStats::default(),
        })
    }

    pub(super) fn execute_validated_promql_range_query(
        &mut self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
        cache_call: &mut super::range_scalar_cache::RangeScalarCacheCall,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut eval_time_ms = start_ms;

        loop {
            let mut execution = self.execute_promql_instant_query_with_cache(
                query,
                eval_time_ms,
                limits,
                Some(&mut *cache_call),
            )?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            results.extend(retimestamp_instant_results(
                std::mem::take(&mut execution.results),
                eval_time_ms,
            ));

            let Some(next_eval_time_ms) = eval_time_ms.checked_add(step_ms) else {
                break;
            };
            if next_eval_time_ms > end_ms {
                break;
            }
            eval_time_ms = next_eval_time_ms;
        }

        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
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
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution =
                    self.execute_promql_instant_query(&offset.input, shifted_end_ms, limits)?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) = self
                    .execute_promql_native_histogram_scalar_range_function(
                        function, end_ms, limits,
                    )?
                {
                    return Ok(execution);
                }
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, read_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation(
                            aggregation,
                            end_ms,
                            limits,
                            None,
                        )?
                {
                    return Ok(execution);
                }
                let mut execution =
                    self.execute_promql_instant_query(&aggregation.input, end_ms, limits)?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_instant_query(&absent.input, end_ms, limits)?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.execute_promql_histogram_fraction(function, end_ms, limits, None)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.execute_promql_histogram_scalar_function(function, end_ms, limits, None)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.execute_promql_histogram_quantile(function, end_ms, limits, None)
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.execute_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    fn execute_promql_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.execute_promql_instant_query_with_cache(query, end_ms, limits, None)
    }

    fn execute_promql_instant_query_with_cache(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_selectors_with_limits_with_cache(
                    &selectors,
                    start_ms,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )
                .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &offset.input,
                    shifted_end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) = self
                    .execute_promql_native_histogram_scalar_range_function(
                        function, end_ms, limits,
                    )?
                {
                    return Ok(execution);
                }
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        read_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation(
                            aggregation,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                {
                    return Ok(execution);
                }
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &aggregation.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &absent.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits_with_cache(
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_cache(
                    &function.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => self.execute_promql_histogram_fraction(
                function,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            ),
            PromqlQuery::HistogramScalarFunction(function) => self
                .execute_promql_histogram_scalar_function(
                    function,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                ),
            PromqlQuery::HistogramQuantile(function) => self.execute_promql_histogram_quantile(
                function,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            ),
            PromqlQuery::BinaryExpression(expression) => self
                .execute_promql_binary_expression_with_cache(
                    expression,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                ),
        }
    }

    fn execute_promql_float_only_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_float_selectors_from_promql(selector.clone())?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_float_only_instant_query(
                    &offset.input,
                    shifted_end_ms,
                    limits,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                let mut execution = self.execute_promql_float_only_instant_query(
                    &aggregation.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&absent.input, end_ms, limits)?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::BinaryExpression(_) => Ok(QueryExecution {
                results: Vec::new(),
                stats: QueryStats::default(),
            }),
        }
    }

    fn execute_promql_histogram_fraction(
        &mut self,
        function: &PromqlHistogramFraction,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self.execute_promql_native_histogram_instant_query(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )? {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_fraction(function, series, end_ms));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_fraction(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_histogram_quantile(
        &mut self,
        function: &PromqlHistogramQuantile,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self.execute_promql_native_histogram_instant_query(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )? {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_quantile(function, series, end_ms));
            }
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_quantile(
                    function, series, end_ms,
                ));
            }
        }

        if saw_native_input {
            let mut classic_execution =
                self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
            stats.merge_from(classic_execution.stats);
            stats.check_limits(limits)?;
            classic_execution.results =
                evaluate_histogram_quantile(function, classic_execution.results, end_ms);
            results.extend(classic_execution.results);
            return Ok(QueryExecution {
                results: merge_query_results(results),
                stats,
            });
        }

        let mut execution = self.execute_promql_instant_query_with_cache(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        execution.results = evaluate_histogram_quantile(function, execution.results, end_ms);
        Ok(execution)
    }

    fn execute_promql_native_histogram_scalar_range_function(
        &mut self,
        function: &PromqlRangeFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        if !matches!(
            function.kind,
            PromqlRangeFunctionKind::Changes | PromqlRangeFunctionKind::Resets
        ) {
            return Ok(None);
        }

        let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some(selector) = native_histogram_selector_from_promql(function.selector.clone())? {
            let (series, native_stats) = self.query_native_histogram_selector_with_limits(
                &selector,
                range_start_ms,
                end_ms,
                limits,
            )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_scalar_range_function(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if let Some(selector) =
            native_exponential_histogram_selector_from_promql(function.selector.clone())?
        {
            let (series, native_stats) = self
                .query_native_exponential_histogram_selector_with_limits(
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_scalar_range_function(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if !saw_native_input {
            return Ok(None);
        }

        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
    }

    fn execute_promql_histogram_scalar_function(
        &mut self,
        function: &PromqlHistogramScalarFunction,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self.execute_promql_native_histogram_instant_query(
            &function.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )? {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_scalar_function(
                function, series, end_ms,
            ));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_scalar_function(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_native_histogram_scalar_aggregation(
        &mut self,
        aggregation: &PromqlAggregation,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        let mut histogram_series = Vec::new();
        let mut exponential_histogram_series = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self.execute_promql_native_histogram_instant_query(
            &aggregation.input,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )? {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                histogram_series = series;
            }
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &aggregation.input,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                exponential_histogram_series = series;
            }
        }

        if !saw_native_input {
            return Ok(None);
        }
        let scalar_execution =
            self.execute_promql_float_only_instant_query(&aggregation.input, end_ms, limits)?;
        stats.merge_from(scalar_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_native_histogram_scalar_aggregation(
            aggregation,
            scalar_execution.results,
            histogram_series,
            exponential_histogram_series,
            end_ms,
        );
        Ok(Some(QueryExecution { results, stats }))
    }

    fn execute_promql_native_histogram_binary_bool_comparison(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        if !expression.return_bool
            || !matches!(
                expression.op,
                PromqlBinaryOp::Eq
                    | PromqlBinaryOp::NotEq
                    | PromqlBinaryOp::Gt
                    | PromqlBinaryOp::Gte
                    | PromqlBinaryOp::Lt
                    | PromqlBinaryOp::Lte
            )
        {
            return Ok(None);
        }

        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        let left_histogram = self.execute_promql_native_histogram_instant_query(
            &expression.left,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let right_histogram = self.execute_promql_native_histogram_instant_query(
            &expression.right,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let left_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.left,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let right_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.right,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;

        let left_histogram_series = if let Some((series, query_stats)) = left_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_histogram_series = if let Some((series, query_stats)) = right_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let left_exponential_series = if let Some((series, query_stats)) = left_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_exponential_series = if let Some((series, query_stats)) = right_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };

        results.extend(evaluate_native_histogram_binary_bool_vector_vector(
            expression,
            left_histogram_series.clone(),
            right_histogram_series.clone(),
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_binary_bool_vector_vector(
                expression,
                left_exponential_series.clone(),
                right_exponential_series.clone(),
                end_ms,
            )?,
        );
        results.extend(evaluate_native_histogram_mixed_binary_bool_vector_vector(
            expression,
            left_histogram_series,
            right_exponential_series,
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_mixed_binary_bool_vector_vector(
                expression,
                left_exponential_series,
                right_histogram_series,
                end_ms,
            )?,
        );

        if !saw_native_input {
            return Ok(None);
        }
        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
    }

    fn execute_promql_native_histogram_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<(Vec<PromqlHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) = native_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_histogram_selector_with_limits(
                    &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self.query_native_histogram_selector_with_limits(
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
                Ok(Some((
                    evaluate_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self.execute_promql_native_histogram_instant_query(
                    &aggregation.input,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Offset(offset) => self.execute_promql_native_histogram_instant_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
                limits,
                cache_call.as_deref_mut(),
            ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_combined_vector_set(
                            expression,
                            left_histogram_series,
                            right_histogram_series,
                            left_exponential_series,
                            right_exponential_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_exponential =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_exponential_histogram_instant_query(
                                &expression.right,
                                end_ms,
                                limits,
                                cache_call.as_deref_mut(),
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
                    stats.check_limits(limits)?;
                    let mut results = evaluate_native_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_exponential_series, right_exponential_stats)) =
                        right_exponential
                    {
                        stats.merge_from(right_exponential_stats);
                        results.extend(evaluate_native_histogram_mixed_binary_vector_vector(
                            expression,
                            left_series,
                            right_exponential_series,
                            end_ms,
                        )?);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand(
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand(
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
        }
    }

    fn execute_promql_native_exponential_histogram_instant_query(
        &mut self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<Option<(Vec<PromqlExponentialHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_exponential_histogram_selector_with_limits(
                    &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self
                    .query_native_exponential_histogram_selector_with_limits(
                        &selector,
                        range_start_ms,
                        end_ms,
                        limits,
                    )?;
                Ok(Some((
                    evaluate_exponential_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query(
                        &aggregation.input,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_exponential_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Offset(offset) => self
                .execute_promql_native_exponential_histogram_instant_query(
                    &offset.input,
                    offset_eval_time_ms(end_ms, offset.offset_ms),
                    limits,
                    cache_call.as_deref_mut(),
                ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?;
                    let left_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_combined_vector_set(
                            expression,
                            left_exponential_series,
                            right_exponential_series,
                            left_histogram_series,
                            right_histogram_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_histogram =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_histogram_instant_query(
                                &expression.right,
                                end_ms,
                                limits,
                                cache_call.as_deref_mut(),
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
                    stats.check_limits(limits)?;
                    let mut results = evaluate_native_exponential_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_histogram_series, right_histogram_stats)) = right_histogram {
                        stats.merge_from(right_histogram_stats);
                        results.extend(
                            evaluate_native_exponential_histogram_mixed_binary_vector_vector(
                                expression,
                                left_series,
                                right_histogram_series,
                                end_ms,
                            )?,
                        );
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand(
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                            cache_call.as_deref_mut(),
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand(
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                    cache_call.as_deref_mut(),
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                        cache_call.as_deref_mut(),
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_exponential_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
        }
    }

    fn execute_promql_scalar_operand(
        &mut self,
        query: &PromqlQuery,
        static_value: Option<f64>,
        end_ms: u64,
        limits: QueryLimits,
        cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<(f64, QueryStats), PromqlQueryError> {
        if let Some(value) = static_value {
            return Ok((value, QueryStats::default()));
        }

        let execution =
            self.execute_promql_instant_query_with_cache(query, end_ms, limits, cache_call)?;
        let value = scalar_query_result_value(&execution.results)?;
        Ok((value, execution.stats))
    }

    fn execute_promql_binary_expression(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        self.execute_promql_binary_expression_with_cache(expression, end_ms, limits, None)
    }

    fn execute_promql_binary_expression_with_cache(
        &mut self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> Result<QueryExecution, PromqlQueryError> {
        if binary_operator_is_set(expression.op) {
            if is_scalar_expression(&expression.left) || is_scalar_expression(&expression.right) {
                return Err(PromqlQueryError::Unsupported(
                    "set binary operators require instant-vector operands".to_string(),
                ));
            }

            let left_execution = self.execute_promql_instant_query_with_cache(
                &expression.left,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let right_execution = self.execute_promql_instant_query_with_cache(
                &expression.right,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let mut stats = left_execution.stats;
            stats.merge_from(right_execution.stats);
            stats.check_limits(limits)?;
            let results = evaluate_binary_vector_set(
                expression,
                left_execution.results,
                right_execution.results,
                end_ms,
            )?;
            return Ok(QueryExecution { results, stats });
        }

        let left_static = scalar_expression_value(&expression.left, end_ms);
        let right_static = scalar_expression_value(&expression.right, end_ms);
        let left_is_scalar = left_static.is_some() || is_scalar_expression(&expression.left);
        let right_is_scalar = right_static.is_some() || is_scalar_expression(&expression.right);

        if !left_is_scalar
            && !right_is_scalar
            && let Some(execution) = self.execute_promql_native_histogram_binary_bool_comparison(
                expression,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?
        {
            return Ok(execution);
        }

        if left_is_scalar && right_is_scalar {
            let (left, mut stats) = self.execute_promql_scalar_operand(
                &expression.left,
                left_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let (right, right_stats) = self.execute_promql_scalar_operand(
                &expression.right,
                right_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            stats.merge_from(right_stats);
            stats.check_limits(limits)?;
            return Ok(QueryExecution {
                results: evaluate_binary_scalar_scalar(expression.op, left, right, end_ms),
                stats,
            });
        }

        if left_is_scalar {
            let (left, mut stats) = self.execute_promql_scalar_operand(
                &expression.left,
                left_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let mut execution = self.execute_promql_instant_query_with_cache(
                &expression.right,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, left, true, end_ms);
            execution.stats = stats;
            return Ok(execution);
        }

        if right_is_scalar {
            let (right, right_stats) = self.execute_promql_scalar_operand(
                &expression.right,
                right_static,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            let mut execution = self.execute_promql_instant_query_with_cache(
                &expression.left,
                end_ms,
                limits,
                cache_call.as_deref_mut(),
            )?;
            execution.stats.merge_from(right_stats);
            execution.stats.check_limits(limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, right, false, end_ms);
            return Ok(execution);
        }

        let left_execution = self.execute_promql_instant_query_with_cache(
            &expression.left,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let right_execution = self.execute_promql_instant_query_with_cache(
            &expression.right,
            end_ms,
            limits,
            cache_call.as_deref_mut(),
        )?;
        let mut stats = left_execution.stats;
        stats.merge_from(right_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_binary_vector_vector(
            expression,
            left_execution.results,
            right_execution.results,
            end_ms,
        )?;
        Ok(QueryExecution { results, stats })
    }

    pub(super) fn query_selector_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_budget_with_cache(selector, start_ms, end_ms, budget, None)
    }

    pub(super) fn query_selector_with_budget_with_cache(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
        mut cache_call: Option<&mut super::range_scalar_cache::RangeScalarCacheCall>,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }
        if self.experimental_cross_segment_chunk_reads
            && cache_call.is_none()
            && self.should_use_cross_segment_flow(start_ms, end_ms)
        {
            return self
                .query_selector_cross_segment_with_budget(selector, start_ms, end_ms, budget);
        }

        let mut results = Vec::new();
        let label_cache = &mut self.label_cache;
        let projected_label_cache = &mut self.projected_label_cache;
        for (segment_ordinal, segment) in self.segments.iter_mut().enumerate() {
            budget.observe_segment_considered();
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            results.extend(segment.query_selector_with_budget(
                selector,
                segment_ordinal,
                start_ms,
                end_ms,
                budget,
                label_cache,
                projected_label_cache,
                cache_call.as_deref_mut(),
            )?);
        }

        Ok(merge_query_results(results))
    }

    fn query_selector_cross_segment_with_budget(
        &mut self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        let Some(chunk_reader) = self
            .segments
            .first()
            .map(|segment| Arc::clone(&segment.chunk_reader))
        else {
            return Ok(Vec::new());
        };
        let mut results = Vec::new();
        let mut group = Vec::new();
        let mut group_spans = 0u64;
        let mut group_bytes = 0u64;
        let mut deferred_error = None;

        for segment_ordinal in 0..self.segments.len() {
            budget.observe_segment_considered();
            let segment = &self.segments[segment_ordinal];
            if segment.reader.meta.end_ms < start_ms || segment.reader.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            let planned = {
                let segment = &mut self.segments[segment_ordinal];
                segment.plan_generic_cross_segment_with_budget(
                    selector,
                    start_ms,
                    end_ms,
                    budget,
                    &mut self.label_cache,
                )
            };
            let generic_plan = match planned {
                Ok(plan) => plan,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            if generic_plan.payload_requests.is_empty() {
                continue;
            }

            let physical = {
                let segment = &mut self.segments[segment_ordinal];
                let reader = segment.reader;
                let context = segment
                    .context
                    .as_mut()
                    .expect("generic plan requires an open context");
                context
                    .plan_cross_segment_chunk_payload_batch(reader, &generic_plan.payload_requests)
            };
            let (file, payload_plan) = match physical {
                Ok(physical) => physical,
                Err(error) => {
                    deferred_error = Some(error);
                    break;
                }
            };
            let item_spans = payload_plan.physical_read_count();
            let item_bytes = payload_plan.physical_bytes_read();
            if chunk_read_group_would_exceed_bounds(
                group.len(),
                group_spans,
                group_bytes,
                item_spans,
                item_bytes,
            ) {
                results.extend(execute_cross_segment_generic_reads(
                    &mut self.segments,
                    Arc::clone(&chunk_reader),
                    std::mem::take(&mut group),
                    start_ms,
                    end_ms,
                    budget,
                    &mut self.projected_label_cache,
                )?);
                group_spans = 0;
                group_bytes = 0;
            }
            group_spans = group_spans.saturating_add(item_spans);
            group_bytes = group_bytes.saturating_add(item_bytes);
            group.push(CrossSegmentGenericRead {
                segment_ordinal,
                generic_plan,
                payload_plan,
                file,
            });
        }

        results.extend(execute_cross_segment_generic_reads(
            &mut self.segments,
            chunk_reader,
            group,
            start_ms,
            end_ms,
            budget,
            &mut self.projected_label_cache,
        )?);
        if let Some(error) = deferred_error {
            return Err(error);
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
    delta_fragments_started: &mut BTreeSet<String>,
) -> (f64, CounterResetHint) {
    if metadata.is_stale() {
        if metadata.temporality == OtlpAggregationTemporality::Delta {
            delta_accumulators.insert(le.to_string(), 0);
            delta_fragments_started.remove(le);
        }
        return (prometheus_stale_nan(), metadata.reset_hint);
    }
    if metadata.temporality == OtlpAggregationTemporality::Delta {
        let accumulator = delta_accumulators.entry(le.to_string()).or_insert(0);
        *accumulator = accumulator.saturating_add(raw);
        let reset_hint = if delta_fragments_started.insert(le.to_string()) {
            CounterResetHint::CounterReset
        } else {
            CounterResetHint::NotCounterReset
        };
        (*accumulator as f64, reset_hint)
    } else {
        (raw as f64, metadata.reset_hint)
    }
}
