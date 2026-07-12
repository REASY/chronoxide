use std::io;
use std::os::unix::io::AsRawFd;
use std::sync::Mutex;

use io_uring::{IoUring, opcode, types};

use super::{ReadRequest, ReadResult};

pub struct IoUringReader {
    ring: Mutex<IoUring>,
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
            ring: Mutex::new(ring),
            queue_depth,
        })
    }

    pub fn read_many(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        let mut out = Vec::with_capacity(requests.len());
        for chunk in requests.chunks(self.queue_depth as usize) {
            let mut ring = self
                .ring
                .lock()
                .map_err(|_| io::Error::new(io::ErrorKind::Other, "io_uring lock poisoned"))?;

            let mut buffers: Vec<Vec<u8>> = Vec::with_capacity(chunk.len());
            for req in chunk {
                if req.len > u32::MAX as usize {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "read length exceeds u32::MAX",
                    ));
                }
                let mut buf = vec![0u8; req.len];
                let entry = opcode::Read::new(
                    types::Fd(req.file.as_raw_fd()),
                    buf.as_mut_ptr(),
                    req.len as u32,
                )
                .offset(req.offset)
                .build()
                .user_data(buffers.len() as u64);
                unsafe {
                    ring.submission().push(&entry).map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "submission queue full")
                    })?;
                }
                buffers.push(buf);
            }

            ring.submit_and_wait(chunk.len())?;

            let mut batch_results: Vec<Option<io::Result<ReadResult>>> =
                std::iter::repeat_with(|| None).take(chunk.len()).collect();
            let mut completed = 0usize;
            while completed < chunk.len() {
                let cqe = ring.completion().next().ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, "missing io_uring completion")
                })?;
                let res = cqe.result();
                let idx = cqe.user_data() as usize;
                if idx >= buffers.len() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "io_uring completion index out of range",
                    ));
                }
                let result = if res < 0 {
                    Err(io::Error::from_raw_os_error(-res))
                } else {
                    let expected = buffers[idx].len() as i32;
                    if res != expected {
                        Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"))
                    } else {
                        let bytes = std::mem::take(&mut buffers[idx]);
                        Ok(ReadResult { bytes })
                    }
                };
                store_completion(&mut batch_results, idx, result)?;
                completed += 1;
            }

            for result in ordered_completions(batch_results)? {
                out.push(result?);
            }
        }
        Ok(out)
    }
}

fn store_completion<T>(slots: &mut [Option<T>], index: usize, value: T) -> io::Result<()> {
    let slot = slots.get_mut(index).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "io_uring completion index out of range",
        )
    })?;
    if slot.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "duplicate io_uring completion index",
        ));
    }
    *slot = Some(value);
    Ok(())
}

fn ordered_completions<T>(slots: Vec<Option<T>>) -> io::Result<Vec<T>> {
    slots
        .into_iter()
        .map(|slot| {
            slot.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "missing io_uring completion")
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Arc;

    #[test]
    fn io_uring_reader_reads_bytes() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"hello io_uring").unwrap();
        let file = Arc::new(tmp.reopen().unwrap());
        let reader = IoUringReader::new(8).unwrap();
        let requests = vec![ReadRequest {
            file,
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
                file: Arc::clone(&file),
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
    fn completion_slots_restore_synthetic_out_of_order_results() {
        let mut slots = vec![None, None, None];
        store_completion(&mut slots, 2, "third").unwrap();
        store_completion(&mut slots, 0, "first").unwrap();
        store_completion(&mut slots, 1, "second").unwrap();

        assert_eq!(
            ordered_completions(slots).unwrap(),
            ["first", "second", "third"]
        );
    }

    #[test]
    fn completion_slots_reject_missing_duplicate_and_out_of_range_results() {
        let missing = ordered_completions(vec![Some(1), None]).unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::InvalidData);
        assert_eq!(missing.to_string(), "missing io_uring completion");

        let mut slots = vec![None];
        store_completion(&mut slots, 0, 1).unwrap();
        let duplicate = store_completion(&mut slots, 0, 2).unwrap_err();
        assert_eq!(duplicate.kind(), io::ErrorKind::InvalidData);
        assert_eq!(duplicate.to_string(), "duplicate io_uring completion index");

        let out_of_range = store_completion(&mut slots, 1, 3).unwrap_err();
        assert_eq!(out_of_range.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            out_of_range.to_string(),
            "io_uring completion index out of range"
        );
    }
}
