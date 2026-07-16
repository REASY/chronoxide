use std::io;
use std::os::unix::io::AsRawFd;
use std::sync::Mutex;

use io_uring::{IoUring, opcode, types};

use super::{ReadManyError, ReadRequest, ReadResult};

pub struct IoUringReader {
    ring: Mutex<Option<IoUring>>,
    queue_depth: u32,
}

impl IoUringReader {
    pub fn new(queue_depth: u32) -> io::Result<Self> {
        if queue_depth == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queue_depth must be > 0",
            ));
        }
        let ring = IoUring::new(queue_depth)?;
        Ok(Self {
            ring: Mutex::new(Some(ring)),
            queue_depth,
        })
    }

    pub fn read_many(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        self.read_many_indexed(requests)
            .map_err(|error| error.source)
    }

    pub(crate) fn read_many_indexed(
        &self,
        requests: &[ReadRequest],
    ) -> Result<Vec<ReadResult>, ReadManyError> {
        if let Some((request_index, _)) = requests
            .iter()
            .enumerate()
            .find(|(_, request)| request.len > u32::MAX as usize)
        {
            return Err(ReadManyError::request(
                request_index,
                io::Error::new(io::ErrorKind::InvalidInput, "read length exceeds u32::MAX"),
            ));
        }
        let mut out = Vec::with_capacity(requests.len());
        for (chunk_index, chunk) in requests.chunks(self.queue_depth as usize).enumerate() {
            let request_base = chunk_index * self.queue_depth as usize;
            let mut ring_slot = self
                .ring
                .lock()
                .map_err(|_| ReadManyError::batch(io::Error::other("io_uring lock poisoned")))?;
            if ring_slot.is_none() {
                *ring_slot = Some(IoUring::new(self.queue_depth).map_err(ReadManyError::batch)?);
            }

            let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(chunk.len());
            let mut entries = Vec::with_capacity(chunk.len());
            for (local_index, req) in chunk.iter().enumerate() {
                let mut buf = vec![0u8; req.len];
                let entry = opcode::Read::new(
                    types::Fd(req.file.as_raw_fd()),
                    buf.as_mut_ptr(),
                    req.len as u32,
                )
                .offset(req.offset)
                .build()
                .user_data(local_index as u64);
                buffers.push(buf);
                entries.push(entry);
            }
            let completion_state = CompletionBatchState::new(chunk.len());

            let batch_results = {
                let Some(ring) = ring_slot.as_mut() else {
                    return Err(ReadManyError::batch(io::Error::other(
                        "io_uring slot was not initialized",
                    )));
                };
                let mut submission = ring.submission();
                // `push_multiple` first checks capacity for the complete slice,
                // so failure cannot leave a prefix of this batch in the SQ.
                let push_result = unsafe { submission.push_multiple(&entries) };
                drop(submission);
                if push_result.is_err() {
                    None
                } else {
                    let mut operations = RingBatchOperations { ring };
                    Some(drive_batch_to_completion(
                        &mut operations,
                        &mut buffers,
                        completion_state,
                    ))
                }
            };
            let Some(batch_results) = batch_results else {
                // No entry from this batch was pushed. Recreate the ring lazily
                // so an unexpectedly non-empty SQ can never leak into a later
                // call with pointers to this batch's soon-to-drop buffers.
                drop(ring_slot.take());
                return Err(ReadManyError::batch(io::Error::other(
                    "submission queue full",
                )));
            };
            let batch_results = match batch_results {
                Ok(results) => results,
                Err(error) => {
                    // The controller returns only after consuming the complete
                    // raw-CQE cardinality, or after a synchronized default
                    // non-SQPOLL SQ proves the whole batch remains unsubmitted.
                    // Drop that ring before buffers and request leases can leave
                    // this scope, then recreate it lazily for a later call.
                    drop(ring_slot.take());
                    return Err(ReadManyError::batch(error));
                }
            };

            for (local_index, result) in batch_results.into_iter().enumerate() {
                out.push(result.map_err(|source| {
                    ReadManyError::request(request_base + local_index, source)
                })?);
            }
        }
        Ok(out)
    }
}

#[derive(Debug, Clone, Copy)]
struct RawCompletion {
    user_data: u64,
    result: i32,
}

trait CompletionSource {
    fn drain_available(&mut self, state: &mut CompletionBatchState);
    fn submit_and_wait_one(&mut self) -> io::Result<()>;
    fn queued_submissions(&mut self) -> usize;
}

struct RingBatchOperations<'a> {
    ring: &'a mut IoUring,
}

impl CompletionSource for RingBatchOperations<'_> {
    fn drain_available(&mut self, state: &mut CompletionBatchState) {
        let queue = self.ring.completion();
        for completion in queue {
            state.observe(RawCompletion {
                user_data: completion.user_data(),
                result: completion.result(),
            });
        }
    }

    fn submit_and_wait_one(&mut self) -> io::Result<()> {
        self.ring.submit_and_wait(1).map(|_| ())
    }

    fn queued_submissions(&mut self) -> usize {
        let mut queue = self.ring.submission();
        queue.sync();
        queue.len()
    }
}

struct CompletionBatchState {
    slots: Vec<Option<i32>>,
    raw_completions: usize,
    first_protocol_error: Option<io::Error>,
}

impl CompletionBatchState {
    fn new(expected: usize) -> Self {
        Self {
            slots: std::iter::repeat_with(|| None).take(expected).collect(),
            raw_completions: 0,
            first_protocol_error: None,
        }
    }

    fn remaining(&self) -> usize {
        self.slots.len().saturating_sub(self.raw_completions)
    }

    fn record_protocol_error(&mut self, error: io::Error) {
        if self.first_protocol_error.is_none() {
            self.first_protocol_error = Some(error);
        }
    }

    fn observe(&mut self, completion: RawCompletion) {
        self.raw_completions = self.raw_completions.saturating_add(1);
        if self.raw_completions > self.slots.len() {
            self.record_protocol_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "excess io_uring completion",
            ));
            return;
        }

        let Ok(index) = usize::try_from(completion.user_data) else {
            self.record_protocol_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "io_uring completion index out of range",
            ));
            return;
        };
        let Some(slot) = self.slots.get_mut(index) else {
            self.record_protocol_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "io_uring completion index out of range",
            ));
            return;
        };
        if slot.is_some() {
            self.record_protocol_error(io::Error::new(
                io::ErrorKind::InvalidData,
                "duplicate io_uring completion index",
            ));
            return;
        }

        *slot = Some(completion.result);
    }

    fn finish(
        self,
        buffers: &mut [Vec<u8>],
        first_submit_error: Option<io::Error>,
    ) -> io::Result<Vec<io::Result<ReadResult>>> {
        if let Some(error) = first_submit_error {
            return Err(error);
        }
        if let Some(error) = self.first_protocol_error {
            return Err(error);
        }
        let mut results = Vec::with_capacity(self.slots.len());
        for (index, slot) in self.slots.into_iter().enumerate() {
            let Some(result) = slot else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "missing io_uring completion",
                ));
            };
            results.push(if result < 0 {
                let errno = result.checked_neg().unwrap_or(libc::EIO);
                Err(io::Error::from_raw_os_error(errno))
            } else if result as usize != buffers[index].len() {
                Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"))
            } else {
                let bytes = std::mem::take(&mut buffers[index]);
                Ok(ReadResult { bytes })
            });
        }
        Ok(results)
    }
}

/// Drives a pushed batch until every submitted operation has one raw terminal
/// completion. Empty CQ polls and interrupted waits are progress states, not
/// proof that a completion is missing.
///
/// The only pre-completion return is a non-retryable submit failure for which
/// the synchronized SQ still contains the entire batch and no CQE was seen;
/// on this default non-SQPOLL ring that proves that zero SQEs were consumed.
fn drive_batch_to_completion(
    source: &mut impl CompletionSource,
    buffers: &mut [Vec<u8>],
    mut state: CompletionBatchState,
) -> io::Result<Vec<io::Result<ReadResult>>> {
    let mut first_submit_error = None;
    while state.remaining() != 0 {
        source.drain_available(&mut state);
        if state.remaining() == 0 {
            break;
        }

        if let Err(error) = source.submit_and_wait_one() {
            let retryable = matches!(
                error.kind(),
                io::ErrorKind::Interrupted | io::ErrorKind::WouldBlock
            );
            if !retryable && first_submit_error.is_none() {
                first_submit_error = Some(error);
            }

            source.drain_available(&mut state);
            if state.raw_completions == 0
                && source.queued_submissions() == state.slots.len()
                && let Some(error) = first_submit_error.take()
            {
                return Err(error);
            }
        }
    }
    state.finish(buffers, first_submit_error)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::Write;
    use std::sync::Arc;

    enum SubmitStep {
        Ok {
            queued_after: usize,
            completions: Vec<RawCompletion>,
        },
        Error {
            kind: io::ErrorKind,
            message: &'static str,
            queued_after: usize,
            completions: Vec<RawCompletion>,
        },
    }

    struct ScriptedBatchOperations {
        queued: usize,
        available: VecDeque<RawCompletion>,
        steps: VecDeque<SubmitStep>,
        submit_calls: usize,
        drained: usize,
    }

    impl ScriptedBatchOperations {
        fn new(queued: usize, steps: Vec<SubmitStep>) -> Self {
            Self {
                queued,
                available: VecDeque::new(),
                steps: steps.into(),
                submit_calls: 0,
                drained: 0,
            }
        }

        fn apply_step(&mut self, queued_after: usize, completions: Vec<RawCompletion>) {
            self.queued = queued_after;
            self.available.extend(completions);
        }
    }

    impl CompletionSource for ScriptedBatchOperations {
        fn drain_available(&mut self, state: &mut CompletionBatchState) {
            while let Some(completion) = self.available.pop_front() {
                state.observe(completion);
                self.drained += 1;
            }
        }

        fn submit_and_wait_one(&mut self) -> io::Result<()> {
            self.submit_calls += 1;
            match self.steps.pop_front().expect("scripted submit step") {
                SubmitStep::Ok {
                    queued_after,
                    completions,
                } => {
                    self.apply_step(queued_after, completions);
                    Ok(())
                }
                SubmitStep::Error {
                    kind,
                    message,
                    queued_after,
                    completions,
                } => {
                    self.apply_step(queued_after, completions);
                    Err(io::Error::new(kind, message))
                }
            }
        }

        fn queued_submissions(&mut self) -> usize {
            self.queued
        }
    }

    fn completion(index: u64, result: i32) -> RawCompletion {
        RawCompletion {
            user_data: index,
            result,
        }
    }

    #[test]
    fn io_uring_reader_reads_bytes() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"hello io_uring").unwrap();
        let file = Arc::new(tmp.reopen().unwrap());
        let reader = IoUringReader::new(8).unwrap();
        let requests = vec![ReadRequest {
            file: file.into(),
            offset: 6,
            len: 8,
        }];
        let result = reader.read_many(&requests).unwrap();
        assert_eq!(result[0].bytes, b"io_uring");
    }

    #[test]
    fn io_uring_reader_returns_results_in_request_order() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"aaaabbbbccccdddd").unwrap();
        let file = Arc::new(tmp.reopen().unwrap());
        let reader = IoUringReader::new(8).unwrap();
        let requests = [12, 0, 8, 4]
            .into_iter()
            .map(|offset| ReadRequest {
                file: Arc::clone(&file).into(),
                offset,
                len: 4,
            })
            .collect::<Vec<_>>();

        let result = reader.read_many(&requests).unwrap();
        assert_eq!(
            result
                .into_iter()
                .map(|result| result.bytes)
                .collect::<Vec<_>>(),
            [
                b"dddd".to_vec(),
                b"aaaa".to_vec(),
                b"cccc".to_vec(),
                b"bbbb".to_vec()
            ]
        );
    }

    #[test]
    fn io_uring_reader_attributes_the_first_request_error_in_request_order() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"x").unwrap();
        let file = Arc::new(tmp.reopen().unwrap());
        let requests = [1, 2, 3]
            .into_iter()
            .map(|len| ReadRequest {
                file: Arc::clone(&file).into(),
                offset: 0,
                len,
            })
            .collect::<Vec<_>>();
        let error = IoUringReader::new(8)
            .unwrap()
            .read_many_indexed(&requests)
            .unwrap_err();

        assert_eq!(error.request_index, Some(1));
        assert_eq!(error.source.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn completion_controller_restores_synthetic_out_of_order_results() {
        let mut operations = ScriptedBatchOperations::new(
            3,
            vec![SubmitStep::Ok {
                queued_after: 0,
                completions: vec![completion(2, 3), completion(0, 1), completion(1, 2)],
            }],
        );
        let mut buffers = vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()];
        let state = CompletionBatchState::new(buffers.len());
        let results = drive_batch_to_completion(&mut operations, &mut buffers, state).unwrap();
        assert_eq!(
            results
                .into_iter()
                .map(|result| result.unwrap().bytes)
                .collect::<Vec<_>>(),
            [b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]
        );
        assert_eq!(operations.submit_calls, 1);
        assert_eq!(operations.drained, 3);
        assert_eq!(operations.queued, 0);
    }

    #[test]
    fn empty_cq_and_interrupted_wait_retry_until_every_completion_is_drained() {
        let mut operations = ScriptedBatchOperations::new(
            2,
            vec![
                SubmitStep::Error {
                    kind: io::ErrorKind::Interrupted,
                    message: "interrupted after submission",
                    queued_after: 0,
                    completions: Vec::new(),
                },
                SubmitStep::Ok {
                    queued_after: 0,
                    completions: vec![completion(1, 2), completion(0, 1)],
                },
            ],
        );
        let mut buffers = vec![b"a".to_vec(), b"bb".to_vec()];
        let state = CompletionBatchState::new(buffers.len());
        let results = drive_batch_to_completion(&mut operations, &mut buffers, state).unwrap();

        assert_eq!(
            results
                .into_iter()
                .map(|result| result.unwrap().bytes)
                .collect::<Vec<_>>(),
            [b"a".to_vec(), b"bb".to_vec()]
        );
        assert_eq!(operations.submit_calls, 2);
        assert_eq!(operations.drained, 2);
        assert_eq!(operations.queued, 0);
    }

    #[test]
    fn nonretryable_error_after_partial_submission_drains_before_returning() {
        let mut operations = ScriptedBatchOperations::new(
            3,
            vec![
                SubmitStep::Error {
                    kind: io::ErrorKind::Other,
                    message: "submit failed after partial progress",
                    queued_after: 1,
                    completions: vec![completion(0, 1), completion(1, 1)],
                },
                SubmitStep::Ok {
                    queued_after: 0,
                    completions: vec![completion(2, 1)],
                },
            ],
        );
        let mut buffers = vec![vec![0; 1], vec![0; 1], vec![0; 1]];
        let state = CompletionBatchState::new(buffers.len());
        let error = drive_batch_to_completion(&mut operations, &mut buffers, state).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::Other);
        assert_eq!(error.to_string(), "submit failed after partial progress");
        assert_eq!(operations.submit_calls, 2);
        assert_eq!(operations.drained, 3);
        assert_eq!(operations.queued, 0);
        assert!(buffers.iter().all(|buffer| buffer.len() == 1));
    }

    #[test]
    fn nonretryable_error_with_the_whole_batch_queued_returns_unsubmitted() {
        let mut operations = ScriptedBatchOperations::new(
            2,
            vec![SubmitStep::Error {
                kind: io::ErrorKind::InvalidInput,
                message: "submit rejected before progress",
                queued_after: 2,
                completions: Vec::new(),
            }],
        );
        let mut buffers = vec![vec![0; 1], vec![0; 1]];
        let state = CompletionBatchState::new(buffers.len());
        let error = drive_batch_to_completion(&mut operations, &mut buffers, state).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "submit rejected before progress");
        assert_eq!(operations.submit_calls, 1);
        assert_eq!(operations.drained, 0);
        assert_eq!(operations.queued, 2);
    }

    #[test]
    fn invalid_and_duplicate_cqes_are_all_consumed_before_protocol_error() {
        let mut operations = ScriptedBatchOperations::new(
            3,
            vec![SubmitStep::Ok {
                queued_after: 0,
                completions: vec![completion(0, 1), completion(0, 1), completion(99, 1)],
            }],
        );
        let mut buffers = vec![vec![0; 1], vec![0; 1], vec![0; 1]];
        let state = CompletionBatchState::new(buffers.len());
        let error = drive_batch_to_completion(&mut operations, &mut buffers, state).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "duplicate io_uring completion index");
        assert_eq!(operations.submit_calls, 1);
        assert_eq!(operations.drained, 3);
        assert_eq!(operations.queued, 0);
        assert!(buffers.iter().all(|buffer| buffer.len() == 1));
    }

    #[test]
    fn out_of_range_cqe_is_consumed_before_protocol_error() {
        let mut operations = ScriptedBatchOperations::new(
            2,
            vec![SubmitStep::Ok {
                queued_after: 0,
                completions: vec![completion(99, 1), completion(0, 1)],
            }],
        );
        let mut buffers = vec![vec![0; 1], vec![0; 1]];
        let state = CompletionBatchState::new(buffers.len());
        let error = drive_batch_to_completion(&mut operations, &mut buffers, state).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "io_uring completion index out of range");
        assert_eq!(operations.drained, 2);
        assert_eq!(operations.queued, 0);
        assert!(buffers.iter().all(|buffer| buffer.len() == 1));
    }

    #[test]
    fn valid_request_errors_remain_in_request_order() {
        let mut operations = ScriptedBatchOperations::new(
            2,
            vec![SubmitStep::Ok {
                queued_after: 0,
                completions: vec![completion(1, -libc::EIO), completion(0, 1)],
            }],
        );
        let mut buffers = vec![b"a".to_vec(), b"bb".to_vec()];
        let state = CompletionBatchState::new(buffers.len());
        let results = drive_batch_to_completion(&mut operations, &mut buffers, state).unwrap();

        assert_eq!(results[0].as_ref().unwrap().bytes, b"a");
        assert_eq!(
            results[1].as_ref().unwrap_err().raw_os_error(),
            Some(libc::EIO)
        );
        assert_eq!(operations.drained, 2);
    }
}
