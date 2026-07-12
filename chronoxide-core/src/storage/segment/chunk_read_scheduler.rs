use super::*;

pub(super) const CHUNK_READ_AUTO_MIN_SPANS: u64 = 8;
pub(super) const CHUNK_READ_MAX_GROUP_SEGMENTS: usize = 32;
pub(super) const CHUNK_READ_MAX_GROUP_SPANS: u64 = 256;
pub(super) const CHUNK_READ_MAX_GROUP_BYTES: u64 = 256 * 1024 * 1024;

pub(super) fn chunk_read_group_would_exceed_bounds(
    group_len: usize,
    group_spans: u64,
    group_bytes: u64,
    item_spans: u64,
    item_bytes: u64,
) -> bool {
    group_len != 0
        && (group_len >= CHUNK_READ_MAX_GROUP_SEGMENTS
            || group_spans.saturating_add(item_spans) > CHUNK_READ_MAX_GROUP_SPANS
            || group_bytes.saturating_add(item_bytes) > CHUNK_READ_MAX_GROUP_BYTES)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChunkReadSchedulerBackend {
    Pread,
    IoUring,
}

pub(super) struct ChunkReadSchedulerItem {
    pub(super) segment_ordinal: usize,
    pub(super) file: Arc<File>,
    pub(super) plan: ChunkPayloadBatchPlan,
    pub(super) logical_requests: u64,
}

pub(super) struct ChunkReadSchedulerResult {
    pub(super) segment_ordinal: usize,
    pub(super) payloads: ChunkPayloadBatch,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(super) struct ChunkReadSchedulerStats {
    pub(super) backend: Option<ChunkReadSchedulerBackend>,
    pub(super) executions: u64,
    pub(super) logical_requests: u64,
    pub(super) physical_spans: u64,
    pub(super) physical_bytes: u64,
    pub(super) backend_submissions: u64,
    pub(super) sqes_submitted: u64,
    pub(super) submission_depth_sum: u64,
    pub(super) submission_depth_max: u64,
    pub(super) submission_depth_1: u64,
    pub(super) submission_depth_2_3: u64,
    pub(super) submission_depth_4_7: u64,
    pub(super) submission_depth_8_plus: u64,
    pub(super) peak_in_flight_bytes: u64,
    pub(super) read_duration: Duration,
}

pub(super) struct ChunkReadScheduler {
    reader: Arc<crate::storage::io::ChunkReader>,
}

impl ChunkReadScheduler {
    pub(super) fn new(reader: Arc<crate::storage::io::ChunkReader>) -> Self {
        Self { reader }
    }

    pub(super) fn execute(
        &self,
        items: Vec<ChunkReadSchedulerItem>,
    ) -> io::Result<(Vec<ChunkReadSchedulerResult>, ChunkReadSchedulerStats)> {
        if items.is_empty() {
            return Ok((Vec::new(), ChunkReadSchedulerStats::default()));
        }

        let physical_spans = items
            .iter()
            .map(|item| item.plan.physical_read_count())
            .sum::<u64>();
        let logical_requests = items.iter().map(|item| item.logical_requests).sum::<u64>();
        let physical_bytes = items
            .iter()
            .map(|item| item.plan.physical_bytes_read())
            .sum::<u64>();
        let backend = self.choose_backend(physical_spans);

        let request_capacity = usize::try_from(physical_spans).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "chunk scheduler physical span count exceeds usize",
            )
        })?;
        let mut requests = Vec::with_capacity(request_capacity);
        for item in &items {
            requests.extend(item.plan.read_requests(Arc::clone(&item.file))?);
        }

        let start = Instant::now();
        let peak_in_flight_bytes = peak_in_flight_bytes(
            &requests,
            backend,
            usize::try_from(self.reader.queue_depth().max(1)).unwrap_or(usize::MAX),
        );
        let read_results = match backend {
            ChunkReadSchedulerBackend::Pread => self.reader.read_many_pread(&requests),
            ChunkReadSchedulerBackend::IoUring => self.reader.read_many(&requests),
        }
        .map_err(normalize_scheduler_read_error)?;
        let read_duration = start.elapsed();

        let mut stats = ChunkReadSchedulerStats {
            backend: Some(backend),
            executions: 1,
            logical_requests,
            physical_spans,
            physical_bytes,
            peak_in_flight_bytes,
            read_duration,
            ..ChunkReadSchedulerStats::default()
        };
        stats.observe_submissions(self.reader.queue_depth());

        let mut read_results = read_results.into_iter();
        let mut results = Vec::with_capacity(items.len());
        for item in items {
            let result_count = usize::try_from(item.plan.physical_read_count()).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "chunk scheduler item span count exceeds usize",
                )
            })?;
            let item_results = read_results.by_ref().take(result_count).collect::<Vec<_>>();
            results.push(ChunkReadSchedulerResult {
                segment_ordinal: item.segment_ordinal,
                payloads: item.plan.finish(item_results)?,
            });
        }
        if read_results.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "chunk scheduler returned excess results",
            ));
        }
        Ok((results, stats))
    }

    fn choose_backend(&self, physical_spans: u64) -> ChunkReadSchedulerBackend {
        use crate::storage::io::ChunkReadMode;

        match self.reader.configured_mode() {
            ChunkReadMode::Pread => ChunkReadSchedulerBackend::Pread,
            ChunkReadMode::IoUring => ChunkReadSchedulerBackend::IoUring,
            ChunkReadMode::Auto => {
                if physical_spans >= CHUNK_READ_AUTO_MIN_SPANS
                    && u64::from(self.reader.queue_depth()) >= CHUNK_READ_AUTO_MIN_SPANS
                    && self.reader.mode() == ChunkReadMode::IoUring
                {
                    ChunkReadSchedulerBackend::IoUring
                } else {
                    ChunkReadSchedulerBackend::Pread
                }
            }
        }
    }
}

fn peak_in_flight_bytes(
    requests: &[crate::storage::io::ReadRequest],
    backend: ChunkReadSchedulerBackend,
    queue_depth: usize,
) -> u64 {
    let submission_width = match backend {
        ChunkReadSchedulerBackend::Pread => 1,
        ChunkReadSchedulerBackend::IoUring => queue_depth.max(1),
    };
    requests
        .chunks(submission_width)
        .map(|submission| {
            submission.iter().fold(0u64, |bytes, request| {
                bytes.saturating_add(request.len as u64)
            })
        })
        .max()
        .unwrap_or(0)
}

impl ChunkReadSchedulerStats {
    fn observe_submissions(&mut self, queue_depth: u32) {
        let queue_depth = u64::from(queue_depth.max(1));
        match self.backend {
            Some(ChunkReadSchedulerBackend::Pread) => {
                self.backend_submissions = self.physical_spans;
                self.submission_depth_sum = self.physical_spans;
                self.submission_depth_max = u64::from(self.physical_spans != 0);
                self.submission_depth_1 = self.physical_spans;
            }
            Some(ChunkReadSchedulerBackend::IoUring) => {
                self.sqes_submitted = self.physical_spans;
                let mut remaining = self.physical_spans;
                while remaining != 0 {
                    let depth = remaining.min(queue_depth);
                    self.backend_submissions = self.backend_submissions.saturating_add(1);
                    self.submission_depth_sum = self.submission_depth_sum.saturating_add(depth);
                    self.submission_depth_max = self.submission_depth_max.max(depth);
                    match depth {
                        1 => self.submission_depth_1 = self.submission_depth_1.saturating_add(1),
                        2..=3 => {
                            self.submission_depth_2_3 = self.submission_depth_2_3.saturating_add(1)
                        }
                        4..=7 => {
                            self.submission_depth_4_7 = self.submission_depth_4_7.saturating_add(1)
                        }
                        _ => {
                            self.submission_depth_8_plus =
                                self.submission_depth_8_plus.saturating_add(1)
                        }
                    }
                    remaining -= depth;
                }
            }
            None => {}
        }
    }
}

fn normalize_scheduler_read_error(error: io::Error) -> io::Error {
    if error.kind() == io::ErrorKind::UnexpectedEof {
        io::Error::new(io::ErrorKind::UnexpectedEof, "failed to fill whole buffer")
    } else {
        error
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn item(file: Arc<File>, segment_ordinal: usize, spans: u64) -> ChunkReadSchedulerItem {
        let requests = (0..spans)
            .map(|index| ChunkPayloadRead {
                offset: index * 2,
                len: 1,
            })
            .collect::<Vec<_>>();
        ChunkReadSchedulerItem {
            segment_ordinal,
            file,
            plan: plan_chunk_payload_batch(&requests, 0).unwrap(),
            logical_requests: spans,
        }
    }

    #[test]
    fn pread_scheduler_restores_results_to_items_with_identical_offsets() {
        let mut left = tempfile::NamedTempFile::new().unwrap();
        let mut right = tempfile::NamedTempFile::new().unwrap();
        left.write_all(b"left").unwrap();
        right.write_all(b"RIGHT").unwrap();
        let reader = Arc::new(
            crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 8,
            })
            .unwrap(),
        );
        let scheduler = ChunkReadScheduler::new(reader);
        let (results, stats) = scheduler
            .execute(vec![
                item(Arc::new(left.reopen().unwrap()), 7, 1),
                item(Arc::new(right.reopen().unwrap()), 3, 1),
            ])
            .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(results[0].segment_ordinal, 7);
        assert_eq!(results[1].segment_ordinal, 3);
        assert_eq!(stats.backend, Some(ChunkReadSchedulerBackend::Pread));
        assert_eq!(stats.physical_spans, 2);
        assert_eq!(stats.backend_submissions, 2);
        assert_eq!(stats.submission_depth_1, 2);
        assert_eq!(stats.peak_in_flight_bytes, 1);
    }

    #[test]
    fn auto_policy_boundaries_follow_physical_span_depth() {
        let mut data = tempfile::NamedTempFile::new().unwrap();
        data.write_all(&[0; 32]).unwrap();
        let reader = Arc::new(
            crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Auto,
                queue_depth: 8,
            })
            .unwrap(),
        );
        let scheduler = ChunkReadScheduler::new(reader);
        let available_backend =
            if scheduler.reader.mode() == crate::storage::io::ChunkReadMode::IoUring {
                ChunkReadSchedulerBackend::IoUring
            } else {
                ChunkReadSchedulerBackend::Pread
            };
        for (spans, expected) in [
            (
                CHUNK_READ_AUTO_MIN_SPANS - 1,
                ChunkReadSchedulerBackend::Pread,
            ),
            (CHUNK_READ_AUTO_MIN_SPANS, available_backend),
            (CHUNK_READ_AUTO_MIN_SPANS + 1, available_backend),
        ] {
            let (_, stats) = scheduler
                .execute(vec![item(Arc::new(data.reopen().unwrap()), 0, spans)])
                .unwrap();
            assert_eq!(stats.backend, Some(expected), "span count {spans}");
        }
    }

    #[test]
    fn empty_scheduler_plan_performs_no_backend_work() {
        let reader = Arc::new(
            crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 8,
            })
            .unwrap(),
        );
        let (results, stats) = ChunkReadScheduler::new(reader).execute(Vec::new()).unwrap();
        assert!(results.is_empty());
        assert_eq!(stats, ChunkReadSchedulerStats::default());
    }

    #[test]
    fn pread_scheduler_normalizes_short_reads_without_partial_results() {
        let mut data = tempfile::NamedTempFile::new().unwrap();
        data.write_all(b"x").unwrap();
        let reader = Arc::new(
            crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Pread,
                queue_depth: 8,
            })
            .unwrap(),
        );
        let scheduler = ChunkReadScheduler::new(reader);
        let request = ChunkPayloadRead { offset: 0, len: 2 };
        let error = match scheduler.execute(vec![ChunkReadSchedulerItem {
            segment_ordinal: 0,
            file: Arc::new(data.reopen().unwrap()),
            plan: plan_chunk_payload_batch(&[request], 0).unwrap(),
            logical_requests: 1,
        }]) {
            Ok(_) => panic!("short read unexpectedly succeeded"),
            Err(error) => error,
        };

        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert_eq!(error.to_string(), "failed to fill whole buffer");
    }

    #[test]
    fn group_bounds_flush_before_overflow_but_allow_one_oversized_item() {
        assert!(!chunk_read_group_would_exceed_bounds(
            0,
            0,
            0,
            CHUNK_READ_MAX_GROUP_SPANS + 1,
            CHUNK_READ_MAX_GROUP_BYTES + 1,
        ));
        assert!(!chunk_read_group_would_exceed_bounds(
            CHUNK_READ_MAX_GROUP_SEGMENTS - 1,
            CHUNK_READ_MAX_GROUP_SPANS - 1,
            CHUNK_READ_MAX_GROUP_BYTES - 1,
            1,
            1,
        ));
        assert!(chunk_read_group_would_exceed_bounds(
            CHUNK_READ_MAX_GROUP_SEGMENTS,
            1,
            1,
            1,
            1,
        ));
        assert!(chunk_read_group_would_exceed_bounds(
            1,
            CHUNK_READ_MAX_GROUP_SPANS,
            1,
            1,
            1,
        ));
        assert!(chunk_read_group_would_exceed_bounds(
            1,
            1,
            CHUNK_READ_MAX_GROUP_BYTES,
            1,
            1,
        ));
    }

    #[test]
    fn peak_in_flight_bytes_follow_backend_submission_width() {
        let data = tempfile::NamedTempFile::new().unwrap();
        let file = Arc::new(data.reopen().unwrap());
        let requests = [2usize, 7, 3, 5, 11]
            .into_iter()
            .map(|len| crate::storage::io::ReadRequest {
                file: Arc::clone(&file),
                offset: 0,
                len,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            peak_in_flight_bytes(&requests, ChunkReadSchedulerBackend::Pread, 8),
            11
        );
        assert_eq!(
            peak_in_flight_bytes(&requests, ChunkReadSchedulerBackend::IoUring, 3),
            16
        );
    }

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    #[test]
    fn io_uring_plan_larger_than_queue_depth_reports_ordered_submissions() {
        let mut data = tempfile::NamedTempFile::new().unwrap();
        data.write_all(&[0; 32]).unwrap();
        let reader = Arc::new(
            crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::IoUring,
                queue_depth: 8,
            })
            .unwrap(),
        );
        let (_, stats) = ChunkReadScheduler::new(reader)
            .execute(vec![item(Arc::new(data.reopen().unwrap()), 0, 9)])
            .unwrap();

        assert_eq!(stats.backend, Some(ChunkReadSchedulerBackend::IoUring));
        assert_eq!(stats.physical_spans, 9);
        assert_eq!(stats.backend_submissions, 2);
        assert_eq!(stats.sqes_submitted, 9);
        assert_eq!(stats.submission_depth_sum, 9);
        assert_eq!(stats.submission_depth_max, 8);
        assert_eq!(stats.submission_depth_1, 1);
        assert_eq!(stats.submission_depth_8_plus, 1);
        assert_eq!(stats.peak_in_flight_bytes, 8);

        let auto_qd1 = Arc::new(
            crate::storage::io::ChunkReader::new(crate::storage::io::ChunkReadConfig {
                mode: crate::storage::io::ChunkReadMode::Auto,
                queue_depth: 1,
            })
            .unwrap(),
        );
        let (_, stats) = ChunkReadScheduler::new(auto_qd1)
            .execute(vec![item(Arc::new(data.reopen().unwrap()), 0, 9)])
            .unwrap();
        assert_eq!(stats.backend, Some(ChunkReadSchedulerBackend::Pread));
    }
}
