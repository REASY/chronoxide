use std::fs::File;
use std::io;
use std::sync::Arc;

use crate::storage::file_manager::GovernedFileLease;

mod pread;

pub use pread::PreadReader;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
mod io_uring;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use io_uring::IoUringReader;

#[derive(Debug, Clone)]
pub enum ReadFile {
    Unmanaged(Arc<File>),
    /// One governed lease shared by every physical span in the same artifact
    /// plan. Cloning a request must not create another file-manager lease.
    Governed(Arc<GovernedFileLease>),
}

impl From<Arc<File>> for ReadFile {
    fn from(file: Arc<File>) -> Self {
        Self::Unmanaged(file)
    }
}

impl From<GovernedFileLease> for ReadFile {
    fn from(file: GovernedFileLease) -> Self {
        Self::Governed(Arc::new(file))
    }
}

impl ReadFile {
    pub(crate) fn read_exact_at(&self, offset: u64, destination: &mut [u8]) -> io::Result<()> {
        match self {
            Self::Unmanaged(file) => pread::read_exact_at(file, offset, destination),
            Self::Governed(file) => file.read_exact_at(offset, destination),
        }
    }
}

#[cfg(unix)]
impl std::os::fd::AsRawFd for ReadFile {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        match self {
            Self::Unmanaged(file) => std::os::fd::AsRawFd::as_raw_fd(file),
            Self::Governed(file) => std::os::fd::AsRawFd::as_raw_fd(file),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub file: ReadFile,
    pub offset: u64,
    pub len: usize,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub bytes: Vec<u8>,
}

pub(crate) struct ReadManyError {
    pub(crate) request_index: Option<usize>,
    pub(crate) source: io::Error,
}

impl ReadManyError {
    pub(crate) fn request(request_index: usize, source: io::Error) -> Self {
        Self {
            request_index: Some(request_index),
            source,
        }
    }

    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    pub(crate) fn batch(source: io::Error) -> Self {
        Self {
            request_index: None,
            source,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkReadMode {
    Auto,
    IoUring,
    Pread,
}

pub const DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES: u64 = 4096;
pub const MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES: u64 = 4096;

#[derive(Debug, Clone)]
pub struct ChunkReadConfig {
    pub mode: ChunkReadMode,
    pub queue_depth: u32,
    pub payload_coalesce_max_gap_bytes: u64,
}

impl Default for ChunkReadConfig {
    fn default() -> Self {
        Self {
            mode: ChunkReadMode::Auto,
            queue_depth: 256,
            payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        }
    }
}

pub struct ChunkReader {
    inner: ChunkReaderInner,
    configured_mode: ChunkReadMode,
    queue_depth: u32,
    payload_coalesce_max_gap_bytes: u64,
}

impl ChunkReader {
    pub fn new(config: ChunkReadConfig) -> io::Result<Self> {
        if config.queue_depth == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queue_depth must be > 0",
            ));
        }
        if config.payload_coalesce_max_gap_bytes > MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "payload_coalesce_max_gap_bytes must be <= {MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES}"
                ),
            ));
        }

        match config.mode {
            ChunkReadMode::Pread => Ok(Self {
                inner: ChunkReaderInner::Pread(PreadReader::new()),
                configured_mode: config.mode,
                queue_depth: config.queue_depth,
                payload_coalesce_max_gap_bytes: config.payload_coalesce_max_gap_bytes,
            }),
            ChunkReadMode::IoUring => {
                Self::new_io_uring(config.queue_depth, config.payload_coalesce_max_gap_bytes)
            }
            ChunkReadMode::Auto => {
                if let Ok(mut reader) =
                    Self::try_io_uring(config.queue_depth, config.payload_coalesce_max_gap_bytes)
                {
                    reader.configured_mode = ChunkReadMode::Auto;
                    return Ok(reader);
                }
                Ok(Self {
                    inner: ChunkReaderInner::Pread(PreadReader::new()),
                    configured_mode: config.mode,
                    queue_depth: config.queue_depth,
                    payload_coalesce_max_gap_bytes: config.payload_coalesce_max_gap_bytes,
                })
            }
        }
    }

    pub fn mode(&self) -> ChunkReadMode {
        self.inner.mode()
    }

    pub fn configured_mode(&self) -> ChunkReadMode {
        self.configured_mode
    }

    pub fn queue_depth(&self) -> u32 {
        self.queue_depth
    }

    pub fn payload_coalesce_max_gap_bytes(&self) -> u64 {
        self.payload_coalesce_max_gap_bytes
    }

    pub fn read_many(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        self.read_many_indexed(requests)
            .map_err(|error| error.source)
    }

    pub fn read_many_pread(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        self.read_many_pread_indexed(requests)
            .map_err(|error| error.source)
    }

    pub(crate) fn read_many_indexed(
        &self,
        requests: &[ReadRequest],
    ) -> Result<Vec<ReadResult>, ReadManyError> {
        self.inner.read_many_indexed(requests)
    }

    pub(crate) fn read_many_pread_indexed(
        &self,
        requests: &[ReadRequest],
    ) -> Result<Vec<ReadResult>, ReadManyError> {
        PreadReader::new().read_many_indexed(requests)
    }

    fn new_io_uring(queue_depth: u32, payload_coalesce_max_gap_bytes: u64) -> io::Result<Self> {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            Ok(Self {
                inner: ChunkReaderInner::IoUring(Box::new(IoUringReader::new(queue_depth)?)),
                configured_mode: ChunkReadMode::IoUring,
                queue_depth,
                payload_coalesce_max_gap_bytes,
            })
        }
        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            let _ = (queue_depth, payload_coalesce_max_gap_bytes);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is not available on this platform or feature set",
            ))
        }
    }

    fn try_io_uring(queue_depth: u32, payload_coalesce_max_gap_bytes: u64) -> io::Result<Self> {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            Self::new_io_uring(queue_depth, payload_coalesce_max_gap_bytes)
        }
        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            let _ = (queue_depth, payload_coalesce_max_gap_bytes);
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is not available on this platform or feature set",
            ))
        }
    }
}

enum ChunkReaderInner {
    Pread(PreadReader),
    #[cfg(all(target_os = "linux", feature = "io_uring"))]
    IoUring(Box<IoUringReader>),
}

impl ChunkReaderInner {
    fn mode(&self) -> ChunkReadMode {
        match self {
            ChunkReaderInner::Pread(_) => ChunkReadMode::Pread,
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            ChunkReaderInner::IoUring(_) => ChunkReadMode::IoUring,
        }
    }

    fn read_many_indexed(
        &self,
        requests: &[ReadRequest],
    ) -> Result<Vec<ReadResult>, ReadManyError> {
        match self {
            ChunkReaderInner::Pread(reader) => reader.read_many_indexed(requests),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            ChunkReaderInner::IoUring(reader) => reader.read_many_indexed(requests),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn chunk_reader_defaults_to_pread_on_non_linux() {
        let reader = ChunkReader::new(ChunkReadConfig::default()).unwrap();
        assert_eq!(reader.configured_mode(), ChunkReadMode::Auto);
        assert_eq!(
            reader.payload_coalesce_max_gap_bytes(),
            DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES
        );
        if cfg!(all(target_os = "linux", feature = "io_uring")) {
            assert!(
                matches!(reader.mode(), ChunkReadMode::IoUring | ChunkReadMode::Pread),
                "auto mode should select io_uring when available, otherwise pread"
            );
        } else {
            assert_eq!(reader.mode(), ChunkReadMode::Pread);
        }
    }

    #[test]
    fn chunk_reader_validates_and_retains_payload_coalesce_gap() {
        for payload_coalesce_max_gap_bytes in [0, 256, 1024, 4096] {
            let reader = ChunkReader::new(ChunkReadConfig {
                mode: ChunkReadMode::Pread,
                queue_depth: 1,
                payload_coalesce_max_gap_bytes,
            })
            .unwrap();
            assert_eq!(
                reader.payload_coalesce_max_gap_bytes(),
                payload_coalesce_max_gap_bytes
            );
        }

        let gap_error = match ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::Pread,
            queue_depth: 1,
            payload_coalesce_max_gap_bytes: MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES + 1,
        }) {
            Ok(_) => panic!("out-of-range payload coalesce gap unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(gap_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            gap_error.to_string(),
            "payload_coalesce_max_gap_bytes must be <= 4096"
        );

        let queue_depth_error = match ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::Pread,
            queue_depth: 0,
            payload_coalesce_max_gap_bytes: MAX_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES + 1,
        }) {
            Ok(_) => panic!("invalid queue depth unexpectedly succeeded"),
            Err(error) => error,
        };
        assert_eq!(queue_depth_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(queue_depth_error.to_string(), "queue_depth must be > 0");
    }

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    #[test]
    fn forced_io_uring_does_not_silently_fall_back() {
        let error = match ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::IoUring,
            queue_depth: 8,
            payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        }) {
            Ok(_) => panic!("forced io_uring unexpectedly fell back"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn pread_reader_reads_bytes() {
        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        tmp.write_all(b"hello world").unwrap();
        let file = Arc::new(tmp.reopen().unwrap());

        let req = ReadRequest {
            file: file.into(),
            offset: 6,
            len: 5,
        };
        let reader = ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::Pread,
            queue_depth: 1,
            payload_coalesce_max_gap_bytes: DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        })
        .unwrap();
        let results = reader.read_many(&[req]).unwrap();
        assert_eq!(results[0].bytes, b"world");
    }
}
