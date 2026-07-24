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
    instrumentation_mode: QueryInstrumentationMode,
}

impl FacadeSegmentQueryContext {
    pub(in crate::storage::segment) fn open(
        metadata_reader: &SegmentMetadataReader,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
    ) -> io::Result<Self> {
        Self::open_with_instrumentation(
            metadata_reader,
            chunk_reader,
            QueryInstrumentationMode::Off,
        )
    }

    pub(in crate::storage::segment) fn open_with_instrumentation(
        metadata_reader: &SegmentMetadataReader,
        chunk_reader: Arc<crate::storage::io::ChunkReader>,
        instrumentation_mode: QueryInstrumentationMode,
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
            instrumentation_mode,
        })
    }

    pub(in crate::storage::segment) fn instrumentation_mode(&self) -> QueryInstrumentationMode {
        self.instrumentation_mode
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
        payload::execute_chunk_payload_file_plans(
            Arc::clone(&self.chunk_reader),
            &mut self.profile,
            &plans,
        )
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
        payload::observe_chunk_read_scheduler(&mut self.profile, scheduler_stats);
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
        let max_gap = self.chunk_reader.payload_coalesce_max_gap_bytes();
        payload::plan_chunk_payload_file_batches(requests, max_gap, |file_id| {
            Ok(self.chunk_file(reader, file_id)?.clone())
        })
    }

    pub(in crate::storage::segment) fn observe_chunk_payload_requests(
        &mut self,
        requests: &[ChunkPayloadRead],
    ) {
        payload::observe_chunk_payload_requests(&mut self.profile, requests);
    }
}

fn metadata_error_to_io(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}
