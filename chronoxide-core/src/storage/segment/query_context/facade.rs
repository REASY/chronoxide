use super::*;
use crate::storage::segment::metadata_facade::{
    SegmentMetadataReader, SegmentMetadataRoot, SegmentMetadataSession,
};

/// Query-local execution state for the schema-neutral metadata facade.
///
/// Unlike [`SegmentQueryContext`], this context never opens a layout-specific
/// series or chunk-index reader. The metadata session retains the generation
/// guard and all root/cache pins while deferred payload locators are planned.
pub(in crate::storage::segment) struct FacadeSegmentQueryContext {
    pub(in crate::storage::segment) metadata: SegmentMetadataSession,
    pub(in crate::storage::segment) root: SegmentMetadataRoot,
    chunk_files: [Option<GovernedArtifactReader>; 2],
    pub(in crate::storage::segment) chunk_reader: Arc<crate::storage::io::ChunkReader>,
    pub(in crate::storage::segment) stats: SegmentStoreQuerySessionStats,
    pub(in crate::storage::segment) profile: SegmentStoreQueryProfile,
}

impl FacadeSegmentQueryContext {
    pub(in crate::storage::segment) fn open(
        metadata_reader: &SegmentMetadataReader,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> io::Result<Self> {
        let started = Instant::now();
        let metadata = metadata_reader
            .query_session()
            .map_err(metadata_error_to_io)?;
        let root = metadata.bind_roots().map_err(metadata_error_to_io)?;
        Ok(Self {
            metadata,
            root,
            chunk_files: [None, None],
            chunk_reader,
            stats: SegmentStoreQuerySessionStats {
                segment_context_opens: 1,
                ..SegmentStoreQuerySessionStats::default()
            },
            profile: SegmentStoreQueryProfile {
                segment_context_open: started.elapsed(),
                ..SegmentStoreQueryProfile::default()
            },
        })
    }

    pub(in crate::storage::segment) fn read_chunk_payload_batch(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<ChunkPayloadBatch> {
        self.observe_chunk_payload_requests(requests);
        self.read_chunk_payload_batch_physical(reader, requests)
    }

    pub(in crate::storage::segment) fn read_chunk_payload_batch_physical(
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

    pub(in crate::storage::segment) fn plan_cross_segment_chunk_payload_batch(
        &mut self,
        reader: &SegmentReader,
        requests: &[ChunkPayloadRead],
    ) -> io::Result<Vec<ChunkPayloadFilePlan>> {
        self.observe_chunk_payload_requests(requests);
        self.plan_chunk_payload_file_batches(reader, requests)
    }

    pub(in crate::storage::segment) fn observe_cross_segment_chunk_payload_read(
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

    pub(in crate::storage::segment) fn observe_chunk_read_scheduler(
        &mut self,
        scheduler_stats: ChunkReadSchedulerStats,
    ) {
        let profile = &mut self.profile.chunk_read_scheduler;
        profile.executions = profile
            .executions
            .saturating_add(scheduler_stats.executions);
        profile.pread_decisions = profile
            .pread_decisions
            .saturating_add(scheduler_stats.pread_decisions);
        profile.io_uring_decisions = profile
            .io_uring_decisions
            .saturating_add(scheduler_stats.io_uring_decisions);
        profile.logical_requests = profile
            .logical_requests
            .saturating_add(scheduler_stats.logical_requests);
        profile.physical_spans = profile
            .physical_spans
            .saturating_add(scheduler_stats.physical_spans);
        profile.backend_submissions = profile
            .backend_submissions
            .saturating_add(scheduler_stats.backend_submissions);
        profile.sqes_submitted = profile
            .sqes_submitted
            .saturating_add(scheduler_stats.sqes_submitted);
        profile.submission_depth_sum = profile
            .submission_depth_sum
            .saturating_add(scheduler_stats.submission_depth_sum);
        profile.submission_depth_max = profile
            .submission_depth_max
            .max(scheduler_stats.submission_depth_max);
        profile.submission_depth_1 = profile
            .submission_depth_1
            .saturating_add(scheduler_stats.submission_depth_1);
        profile.submission_depth_2_3 = profile
            .submission_depth_2_3
            .saturating_add(scheduler_stats.submission_depth_2_3);
        profile.submission_depth_4_7 = profile
            .submission_depth_4_7
            .saturating_add(scheduler_stats.submission_depth_4_7);
        profile.submission_depth_8_plus = profile
            .submission_depth_8_plus
            .saturating_add(scheduler_stats.submission_depth_8_plus);
        profile.in_flight_bytes = profile
            .in_flight_bytes
            .saturating_add(scheduler_stats.physical_bytes);
        profile.peak_in_flight_bytes = profile
            .peak_in_flight_bytes
            .max(scheduler_stats.peak_in_flight_bytes);
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
            let started = Instant::now();
            let artifact = reader
                .registered_metadata
                .reader(file)
                .map_err(metadata_error_to_io)?;
            self.profile.chunks_file_bytes = self
                .profile
                .chunks_file_bytes
                .saturating_add(artifact.len());
            self.chunk_files[file_index] = Some(artifact);
            self.profile.chunks_open = self.profile.chunks_open.saturating_add(started.elapsed());
            self.stats.chunks_bin_opens = self.stats.chunks_bin_opens.saturating_add(1);
        }
        Ok(self.chunk_files[file_index]
            .as_ref()
            .expect("payload reader was initialized"))
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
            let plan = plan_chunk_payload_batch(&requests, CHUNK_PAYLOAD_COALESCE_MAX_GAP)?;
            plans.push(ChunkPayloadFilePlan {
                file_id,
                file: self.chunk_file(reader, file_id)?.clone(),
                plan,
                logical_requests: requests.len() as u64,
            });
        }
        Ok(plans)
    }

    pub(in crate::storage::segment) fn observe_chunk_payload_requests(
        &mut self,
        requests: &[ChunkPayloadRead],
    ) {
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
}

fn metadata_error_to_io(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
