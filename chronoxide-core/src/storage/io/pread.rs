use std::io;

use super::{ReadManyError, ReadRequest, ReadResult};

pub struct PreadReader;

impl Default for PreadReader {
    fn default() -> Self {
        Self::new()
    }
}

impl PreadReader {
    pub fn new() -> Self {
        Self
    }

    pub fn read_many(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        self.read_many_indexed(requests)
            .map_err(|error| error.source)
    }

    pub(crate) fn read_many_indexed(
        &self,
        requests: &[ReadRequest],
    ) -> Result<Vec<ReadResult>, ReadManyError> {
        let mut results = Vec::with_capacity(requests.len());
        for (request_index, req) in requests.iter().enumerate() {
            let mut buf = vec![0u8; req.len];
            req.file
                .read_exact_at(req.offset, &mut buf)
                .map_err(|source| ReadManyError::request(request_index, source))?;
            results.push(ReadResult { bytes: buf });
        }
        Ok(results)
    }
}

#[cfg(unix)]
pub(super) fn read_exact_at(file: &std::fs::File, offset: u64, buf: &mut [u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    let mut read = 0usize;
    while read < buf.len() {
        let bytes = file.read_at(&mut buf[read..], offset + read as u64)?;
        if bytes == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short read"));
        }
        read += bytes;
    }
    Ok(())
}

#[cfg(not(unix))]
pub(super) fn read_exact_at(
    _file: &std::fs::File,
    _offset: u64,
    _buf: &mut [u8],
) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "pread is not supported on this platform",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::Arc;

    #[test]
    fn pread_reader_reads_multiple_ranges() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"abcdefghijkl").unwrap();
        let file = Arc::new(tmp.reopen().unwrap());
        let reader = PreadReader::new();

        let requests = vec![
            ReadRequest {
                file: Arc::clone(&file).into(),
                offset: 0,
                len: 4,
            },
            ReadRequest {
                file: Arc::clone(&file).into(),
                offset: 4,
                len: 4,
            },
            ReadRequest {
                file: Arc::clone(&file).into(),
                offset: 8,
                len: 4,
            },
        ];
        let results = reader.read_many(&requests).unwrap();
        let collected: Vec<u8> = results.into_iter().flat_map(|r| r.bytes).collect();
        assert_eq!(collected, b"abcdefghijkl");
    }
}
