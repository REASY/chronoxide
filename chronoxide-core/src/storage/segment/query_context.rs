use super::query_reader::{GenericCrossSegmentPlan, NativeTypedCrossSegmentPlan};
use super::*;
use crate::storage::symbols::SegmentSymbolReader;

mod facade;
mod session;
mod session_execution;
mod session_reader;

pub(super) use facade::*;
pub(super) use session_execution::*;
pub(super) use session_reader::*;

#[derive(Debug, Clone, Copy)]
pub(super) struct QueryStageTimer {
    started: Option<Instant>,
}

impl QueryStageTimer {
    #[inline(always)]
    pub(super) fn start(mode: QueryInstrumentationMode) -> Self {
        Self {
            started: (mode == QueryInstrumentationMode::Detailed).then(Instant::now),
        }
    }

    #[inline(always)]
    pub(super) fn start_if(mode: QueryInstrumentationMode, has_work: bool) -> Self {
        if has_work {
            Self::start(mode)
        } else {
            Self { started: None }
        }
    }

    #[inline(always)]
    pub(super) fn elapsed(self) -> Duration {
        self.started
            .map_or(Duration::ZERO, |started| started.elapsed())
    }
}

pub(super) struct SegmentQuerySessionReader<'a> {
    pub(super) reader: &'a SegmentReader,
    pub(super) facade_context: Option<FacadeSegmentQueryContext>,
    pub(super) context: Option<SegmentQueryContext>,
    pub(super) stats: SegmentStoreQuerySessionStats,
    pub(super) profile: SegmentStoreQueryProfile,
    pub(super) chunk_reader: Arc<crate::storage::io::ChunkReader>,
    pub(super) query_instrumentation_mode: QueryInstrumentationMode,
}

fn entries_in_requested_order<T>(
    series_refs: &[u32],
    mut entries_by_ref: HashMap<u32, T>,
    entry_kind: &str,
) -> io::Result<Vec<(u32, T)>> {
    series_refs
        .iter()
        .map(|series_ref| {
            let entry = entries_by_ref.remove(series_ref).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{entry_kind} reader omitted requested series ref {series_ref}"),
                )
            })?;
            Ok((*series_ref, entry))
        })
        .collect()
}

pub(in crate::storage::segment) struct ChunkPayloadFilePlan {
    pub(in crate::storage::segment) file_id: u8,
    pub(in crate::storage::segment) file: GovernedArtifactReader,
    pub(in crate::storage::segment) plan: ChunkPayloadBatchPlan,
    pub(in crate::storage::segment) logical_requests: u64,
}

pub(super) struct SegmentQueryContext {
    pub(super) symbols: Arc<SegmentSymbolReader<File>>,
    pub(super) index_reader: SegmentIndexReader<File>,
    pub(super) series_reader: Option<SeriesReader<File>>,
    pub(super) chunk_index_reader: Option<ChunkIndexReader>,
    pub(super) chunk_files: [Option<GovernedArtifactReader>; 2],
    pub(super) chunk_reader: Arc<crate::storage::io::ChunkReader>,
    pub(super) stats: SegmentStoreQuerySessionStats,
    pub(super) profile: SegmentStoreQueryProfile,
}

impl SegmentQueryContext {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "retained for schema-6 range-cache regression tests and direct context experiments"
        )
    )]
    pub(super) fn open(reader: &SegmentReader) -> io::Result<Self> {
        let chunk_reader = Arc::new(crate::storage::io::ChunkReader::new(
            crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 1,
                payload_coalesce_max_gap_bytes:
                    crate::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
            },
        )?);
        Self::open_with_chunk_reader(reader, chunk_reader)
    }

    fn open_with_chunk_reader(
        reader: &SegmentReader,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> io::Result<Self> {
        let context_start = Instant::now();
        let mut profile = SegmentStoreQueryProfile::default();
        let cached = reader.cached_index_reader()?;
        if !cached.cache_hit {
            profile.indexes_file_bytes = cached.file_bytes;
            profile.indexes_open = cached.open_elapsed;
        }
        profile.index_read_stats = profile
            .index_read_stats
            .saturating_add(cached.open_read_stats);
        let indexes_puffin_opens = if cached.cache_hit { 0 } else { 1 };
        let index_reader = cached.reader;
        let symbols = reader.cached_symbols()?;
        if !symbols.cache_hit {
            profile.symbols_file_bytes = symbols.file_bytes;
            profile.symbols_read = symbols.open_elapsed;
        }
        profile.symbol_read_stats = profile
            .symbol_read_stats
            .saturating_add(symbols.open_read_stats);
        profile.segment_context_open = context_start.elapsed();
        Ok(Self {
            symbols: symbols.symbols,
            index_reader,
            series_reader: None,
            chunk_index_reader: None,
            chunk_files: [None, None],
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

    fn chunk_file(
        &mut self,
        reader: &SegmentReader,
        file_id: u8,
    ) -> io::Result<&GovernedArtifactReader> {
        let file_index = usize::from(file_id);
        let file = match file_id {
            0 => SegmentFile::Chunks,
            1 => SegmentFile::OooChunks,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk payload file_id must be 0 or 1",
                ));
            }
        };
        if self.chunk_files[file_index].is_none() {
            let artifact = reader
                .registered_metadata
                .reader(file)
                .map_err(io::Error::other)?;
            self.profile.chunks_file_bytes = self
                .profile
                .chunks_file_bytes
                .saturating_add(artifact.len());
            let start = Instant::now();
            self.chunk_files[file_index] = Some(artifact);
            self.profile.chunks_open = self.profile.chunks_open.saturating_add(start.elapsed());
            self.stats.chunks_bin_opens = self.stats.chunks_bin_opens.saturating_add(1);
        }
        Ok(self.chunk_files[file_index].as_ref().unwrap())
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
            let loaded_count = loaded.len();
            let loaded_entries = loaded
                .into_iter()
                .map(|(series_ref, entry)| (series_ref, Arc::new(entry)))
                .collect::<HashMap<_, _>>();
            self.profile.series_entry_read = self
                .profile
                .series_entry_read
                .saturating_add(start.elapsed());
            self.profile.series_entries_read = self
                .profile
                .series_entries_read
                .saturating_add(loaded_count as u64);
            self.profile.series_entry_read_batches =
                self.profile.series_entry_read_batches.saturating_add(1);
            let loaded_entries =
                entries_in_requested_order(&missing_refs, loaded_entries, "series entry")?;

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

        entries_in_requested_order(series_refs, cached_entries, "series entry")
    }

    pub(super) fn read_series_entries_uncached(
        &mut self,
        reader: &SegmentReader,
        series_refs: &[u32],
    ) -> io::Result<Vec<(u32, SeriesEntry)>> {
        if series_refs.is_empty() {
            return Ok(Vec::new());
        }

        let start = Instant::now();
        let mut locator_entries = Vec::new();
        let mut unresolved_refs = Vec::new();
        {
            let cached = reader
                .query_cache
                .series_locators
                .lock()
                .map_err(|_| io::Error::other("segment series locator cache lock poisoned"))?;
            for &series_ref in series_refs {
                if let Some(locator) = cached.get(&series_ref) {
                    locator_entries.push((series_ref, **locator));
                } else {
                    unresolved_refs.push(series_ref);
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
        if !unresolved_refs.is_empty() {
            let (entries, unresolved_bytes_read) = self
                .series_reader(reader)?
                .read_entries_with_bytes(&unresolved_refs)?;
            bytes_read = bytes_read.saturating_add(unresolved_bytes_read);
            loaded.extend(entries);
        }

        self.profile.series_entry_bytes =
            self.profile.series_entry_bytes.saturating_add(bytes_read);
        self.profile.series_entry_read = self
            .profile
            .series_entry_read
            .saturating_add(start.elapsed());
        self.profile.series_entries_read = self
            .profile
            .series_entries_read
            .saturating_add(loaded.len() as u64);
        self.profile.series_entry_read_batches =
            self.profile.series_entry_read_batches.saturating_add(1);

        let entries_by_ref = loaded.into_iter().collect::<HashMap<_, _>>();
        entries_in_requested_order(series_refs, entries_by_ref, "uncached series entry")
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
                    (
                        series_ref,
                        (Arc::new(locator), Arc::new(locator.metadata())),
                    )
                })
                .collect::<HashMap<_, _>>();
            self.profile.series_entry_read = self
                .profile
                .series_entry_read
                .saturating_add(start.elapsed());
            let loaded_entries =
                entries_in_requested_order(&missing_refs, loaded_entries, "series metadata entry")?;

            {
                let mut cached =
                    reader.query_cache.series_metadata.lock().map_err(|_| {
                        io::Error::other("segment series metadata cache lock poisoned")
                    })?;
                for (series_ref, (_, entry)) in &loaded_entries {
                    cached.insert(*series_ref, Arc::clone(entry));
                    cached_entries.insert(*series_ref, Arc::clone(entry));
                }
            }
            {
                let mut cached =
                    reader.query_cache.series_locators.lock().map_err(|_| {
                        io::Error::other("segment series locator cache lock poisoned")
                    })?;
                for (series_ref, (locator, _)) in loaded_entries {
                    cached.insert(series_ref, locator);
                }
            }
        }

        entries_in_requested_order(series_refs, cached_entries, "series metadata entry")
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
        let mut logical_ranges_by_file = [Vec::new(), Vec::new()];
        for request in requests {
            if let Some(logical_ranges) =
                logical_ranges_by_file.get_mut(usize::from(request.file_id))
            {
                logical_ranges.push((request.offset, request.len));
            }
        }
        for logical_ranges in &mut logical_ranges_by_file {
            self.profile
                .observe_chunk_payload_file_reads(logical_ranges);
            self.profile
                .observe_sorted_chunk_payload_ranges(logical_ranges);
        }
    }

    pub(super) fn read_chunk_payload_batch_physical(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<ChunkPayloadBatch> {
        if requests.is_empty() {
            return Ok(ChunkPayloadBatch::empty());
        }

        let plans = self.plan_chunk_payload_file_batches(reader, requests)?;
        let scheduler = ChunkReadScheduler::new(Arc::clone(&self.chunk_reader));
        let scheduler_items = plans
            .iter()
            .map(|planned| ChunkReadSchedulerItem {
                segment_ordinal: 0,
                file_id: planned.file_id,
                file: ChunkReadSchedulerFile::Governed(planned.file.clone()),
                plan: planned.plan.clone(),
                logical_requests: planned.logical_requests,
            })
            .collect();
        let (results, scheduler_stats) = scheduler.execute(scheduler_items)?;
        self.observe_chunk_read_scheduler(scheduler_stats);
        self.profile.chunk_read = self
            .profile
            .chunk_read
            .saturating_add(scheduler_stats.read_duration);
        if results.len() != plans.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk scheduler payload-file result count does not match plans",
            ));
        }
        let mut batch = ChunkPayloadBatch::empty();
        for (planned, result) in plans.iter().zip(results) {
            if result.segment_ordinal != 0 || result.file_id != planned.file_id {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk scheduler changed payload-file result order",
                ));
            }
            self.profile.observe_chunk_payload_physical_reads(
                result.payloads.physical_read_count(),
                result.payloads.physical_bytes_read(),
            );
            batch.append(result.payloads);
        }
        Ok(batch)
    }

    #[expect(
        dead_code,
        reason = "retained schema-6 planner hook for layout comparison experiments"
    )]
    fn plan_cross_segment_chunk_payload_batch(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<Vec<ChunkPayloadFilePlan>> {
        self.observe_chunk_payload_requests(requests);
        self.plan_chunk_payload_file_batches(reader, requests)
    }

    fn plan_chunk_payload_file_batches(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<Vec<ChunkPayloadFilePlan>> {
        let mut by_file = [Vec::new(), Vec::new()];
        for &request in requests {
            let Some(file_requests) = by_file.get_mut(usize::from(request.file_id)) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "chunk payload file_id must be 0 or 1",
                ));
            };
            file_requests.push(request);
        }

        let mut plans = Vec::with_capacity(2);
        for (file_id, requests) in by_file.into_iter().enumerate() {
            if requests.is_empty() {
                continue;
            }
            let file_id = u8::try_from(file_id).expect("two payload files fit u8");
            let plan = plan_chunk_payload_batch(
                &requests,
                self.chunk_reader.payload_coalesce_max_gap_bytes(),
            )?;
            plans.push(ChunkPayloadFilePlan {
                file_id,
                file: self.chunk_file(reader, file_id)?.clone(),
                plan,
                logical_requests: requests.len() as u64,
            });
        }
        Ok(plans)
    }

    #[expect(
        dead_code,
        reason = "retained schema-6 accounting hook for layout comparison experiments"
    )]
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
        profile.pread_decisions = profile
            .pread_decisions
            .saturating_add(stats.pread_decisions);
        profile.io_uring_decisions = profile
            .io_uring_decisions
            .saturating_add(stats.io_uring_decisions);
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
        profile.total_physical_bytes_executed = profile
            .total_physical_bytes_executed
            .saturating_add(stats.physical_bytes);
        profile.peak_in_flight_bytes = profile.peak_in_flight_bytes.max(stats.peak_in_flight_bytes);
    }

    #[expect(
        dead_code,
        reason = "retained schema-6 prefetch hook for layout comparison experiments"
    )]
    pub(super) fn prefetch_chunk_range(
        &mut self,
        reader: &SegmentReader,
        file_id: u8,
        offset: u64,
        len: u64,
        scratch: &mut Vec<u8>,
    ) -> io::Result<()> {
        let start = Instant::now();
        let artifact = self.chunk_file(reader, file_id)?.clone();
        let mut leases =
            GovernedArtifactReader::acquire_file_leases(std::slice::from_ref(&artifact))
                .map_err(metadata_cache_error_to_io)?;
        let lease = leases
            .pop()
            .expect("one governed payload reader returns one lease");
        let read_result = prefetch_governed_file_range(&lease, offset, len, scratch);
        drop(lease);
        if let Err(error) = read_result {
            return Err(metadata_cache_error_to_io(
                artifact.record_scheduled_read_error(error),
            ));
        }
        self.profile.chunk_read = self.profile.chunk_read.saturating_add(start.elapsed());
        self.profile.observe_chunk_payload_physical_reads(1, len);
        Ok(())
    }

    #[expect(
        dead_code,
        reason = "retained schema-6 prewarm hook for layout comparison experiments"
    )]
    pub(super) fn prewarm_query_files(&mut self, reader: &SegmentReader) -> io::Result<()> {
        self.series_reader(reader)?;
        self.chunk_index_reader(reader)?;
        for file_id in [0, 1] {
            let artifact = self.chunk_file(reader, file_id)?.clone();
            drop(
                GovernedArtifactReader::acquire_file_leases(&[artifact])
                    .map_err(metadata_cache_error_to_io)?,
            );
        }
        Ok(())
    }
}

#[expect(
    dead_code,
    reason = "used by the retained schema-6 prefetch experiment hook"
)]
fn prefetch_governed_file_range(
    file: &crate::storage::file_manager::GovernedFileLease,
    offset: u64,
    len: u64,
    scratch: &mut Vec<u8>,
) -> io::Result<()> {
    const PREFETCH_BUFFER_BYTES: usize = 64 * 1024;
    let mut read_offset = offset;
    let mut remaining = len;
    while remaining != 0 {
        let read_len =
            usize::try_from(remaining.min(PREFETCH_BUFFER_BYTES as u64)).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "prefetch range length exceeds usize",
                )
            })?;
        scratch.resize(read_len, 0);
        file.read_exact_at(read_offset, scratch)?;
        read_offset = read_offset.saturating_add(read_len as u64);
        remaining -= read_len as u64;
    }
    Ok(())
}

// Metadata-only segment pruning step. Keep this independent of postings/chunk decoding so
// future scan planners, including a DataFusion TableProvider, can reuse the same decision.
