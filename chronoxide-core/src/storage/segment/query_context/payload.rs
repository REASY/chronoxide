use std::io;
use std::sync::Arc;

use crate::storage::chunk::{
    ChunkPayloadBatch, ChunkPayloadBatchPlan, ChunkPayloadRead, plan_chunk_payload_batch,
};
use crate::storage::metadata_runtime::GovernedArtifactReader;
use crate::storage::segment::{
    ChunkReadScheduler, ChunkReadSchedulerFile, ChunkReadSchedulerItem, ChunkReadSchedulerStats,
    SegmentStoreQueryProfile,
};

pub(in crate::storage::segment) struct ChunkPayloadFilePlan {
    pub(in crate::storage::segment) file_id: u8,
    pub(in crate::storage::segment) file: GovernedArtifactReader,
    pub(in crate::storage::segment) plan: ChunkPayloadBatchPlan,
    pub(in crate::storage::segment) logical_requests: u64,
}

#[inline]
pub(super) fn observe_chunk_payload_requests(
    profile: &mut SegmentStoreQueryProfile,
    requests: &[ChunkPayloadRead],
) {
    let mut logical_ranges_by_file = [Vec::new(), Vec::new()];
    for request in requests {
        if let Some(logical_ranges) = logical_ranges_by_file.get_mut(usize::from(request.file_id)) {
            logical_ranges.push((request.offset, request.len));
        }
    }
    for logical_ranges in &mut logical_ranges_by_file {
        profile.observe_chunk_payload_file_reads(logical_ranges);
        profile.observe_sorted_chunk_payload_ranges(logical_ranges);
    }
}

#[inline]
pub(super) fn plan_chunk_payload_file_batches<F>(
    requests: &[ChunkPayloadRead],
    payload_coalesce_max_gap_bytes: u64,
    mut chunk_file: F,
) -> io::Result<Vec<ChunkPayloadFilePlan>>
where
    F: FnMut(u8) -> io::Result<GovernedArtifactReader>,
{
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
        let plan = plan_chunk_payload_batch(&requests, payload_coalesce_max_gap_bytes)?;
        plans.push(ChunkPayloadFilePlan {
            file_id,
            file: chunk_file(file_id)?,
            plan,
            logical_requests: requests.len() as u64,
        });
    }
    Ok(plans)
}

#[inline]
pub(super) fn execute_chunk_payload_file_plans(
    chunk_reader: Arc<crate::storage::io::ChunkReader>,
    profile: &mut SegmentStoreQueryProfile,
    plans: &[ChunkPayloadFilePlan],
) -> io::Result<ChunkPayloadBatch> {
    let scheduler = ChunkReadScheduler::new(chunk_reader);
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
    observe_chunk_read_scheduler(profile, scheduler_stats);
    profile.chunk_read = profile
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
        profile.observe_chunk_payload_physical_reads(
            result.payloads.physical_read_count(),
            result.payloads.physical_bytes_read(),
        );
        batch.append(result.payloads);
    }
    Ok(batch)
}

#[inline]
pub(super) fn observe_chunk_read_scheduler(
    profile: &mut SegmentStoreQueryProfile,
    stats: ChunkReadSchedulerStats,
) {
    let scheduler = &mut profile.chunk_read_scheduler;
    scheduler.executions = scheduler.executions.saturating_add(stats.executions);
    scheduler.pread_decisions = scheduler
        .pread_decisions
        .saturating_add(stats.pread_decisions);
    scheduler.io_uring_decisions = scheduler
        .io_uring_decisions
        .saturating_add(stats.io_uring_decisions);
    scheduler.logical_requests = scheduler
        .logical_requests
        .saturating_add(stats.logical_requests);
    scheduler.physical_spans = scheduler
        .physical_spans
        .saturating_add(stats.physical_spans);
    scheduler.backend_submissions = scheduler
        .backend_submissions
        .saturating_add(stats.backend_submissions);
    scheduler.sqes_submitted = scheduler
        .sqes_submitted
        .saturating_add(stats.sqes_submitted);
    scheduler.submission_depth_sum = scheduler
        .submission_depth_sum
        .saturating_add(stats.submission_depth_sum);
    scheduler.submission_depth_max = scheduler
        .submission_depth_max
        .max(stats.submission_depth_max);
    scheduler.submission_depth_1 = scheduler
        .submission_depth_1
        .saturating_add(stats.submission_depth_1);
    scheduler.submission_depth_2_3 = scheduler
        .submission_depth_2_3
        .saturating_add(stats.submission_depth_2_3);
    scheduler.submission_depth_4_7 = scheduler
        .submission_depth_4_7
        .saturating_add(stats.submission_depth_4_7);
    scheduler.submission_depth_8_plus = scheduler
        .submission_depth_8_plus
        .saturating_add(stats.submission_depth_8_plus);
    scheduler.total_physical_bytes_executed = scheduler
        .total_physical_bytes_executed
        .saturating_add(stats.physical_bytes);
    scheduler.peak_in_flight_bytes = scheduler
        .peak_in_flight_bytes
        .max(stats.peak_in_flight_bytes);
}
