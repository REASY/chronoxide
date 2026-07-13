use super::query_reader::{GenericCrossSegmentPlan, NativeTypedCrossSegmentPlan};
use super::*;

mod session;
mod session_execution;
mod session_reader;

pub(super) use session_execution::*;
pub(super) use session_reader::*;

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
