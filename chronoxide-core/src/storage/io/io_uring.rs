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
        let ring = IoUring::new(queue_depth as usize)?;
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
                .offset(req.offset as i64)
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

            let mut batch_results: Vec<Option<ReadResult>> = vec![None; chunk.len()];
            let mut completed = 0usize;
            while completed < chunk.len() {
                if let Some(cqe) = ring.completion().next() {
                    let res = cqe.result();
                    let idx = cqe.user_data() as usize;
                    if idx >= buffers.len() {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "io_uring completion index out of range",
                        ));
                    }
                    if res < 0 {
                        return Err(io::Error::from_raw_os_error(-res));
                    }
                    let expected = buffers[idx].len() as i32;
                    if res != expected {
                        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
                    }
                    let bytes = std::mem::take(&mut buffers[idx]);
                    batch_results[idx] = Some(ReadResult { bytes });
                    completed += 1;
                }
            }

            for result in batch_results {
                out.push(result.expect("missing io_uring result"));
            }
        }
        Ok(out)
    }
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
}
