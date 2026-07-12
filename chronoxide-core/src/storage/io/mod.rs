use std::fs::File;
use std::io;
use std::sync::Arc;

mod pread;

pub use pread::PreadReader;

#[cfg(all(target_os = "linux", feature = "io_uring"))]
mod io_uring;
#[cfg(all(target_os = "linux", feature = "io_uring"))]
pub use io_uring::IoUringReader;

#[derive(Debug, Clone)]
pub struct ReadRequest {
    pub file: Arc<File>,
    pub offset: u64,
    pub len: usize,
}

#[derive(Debug, Clone)]
pub struct ReadResult {
    pub bytes: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkReadMode {
    Auto,
    IoUring,
    Pread,
}

#[derive(Debug, Clone)]
pub struct ChunkReadConfig {
    pub mode: ChunkReadMode,
    pub queue_depth: u32,
}

impl Default for ChunkReadConfig {
    fn default() -> Self {
        Self {
            mode: ChunkReadMode::Auto,
            queue_depth: 256,
        }
    }
}

pub struct ChunkReader {
    inner: ChunkReaderInner,
    configured_mode: ChunkReadMode,
    queue_depth: u32,
}

impl ChunkReader {
    pub fn new(config: ChunkReadConfig) -> io::Result<Self> {
        if config.queue_depth == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "queue_depth must be > 0",
            ));
        }

        match config.mode {
            ChunkReadMode::Pread => Ok(Self {
                inner: ChunkReaderInner::Pread(PreadReader::new()),
                configured_mode: config.mode,
                queue_depth: config.queue_depth,
            }),
            ChunkReadMode::IoUring => Self::new_io_uring(config.queue_depth),
            ChunkReadMode::Auto => {
                if let Ok(mut reader) = Self::try_io_uring(config.queue_depth) {
                    reader.configured_mode = ChunkReadMode::Auto;
                    return Ok(reader);
                }
                Ok(Self {
                    inner: ChunkReaderInner::Pread(PreadReader::new()),
                    configured_mode: config.mode,
                    queue_depth: config.queue_depth,
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

    pub fn read_many(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        self.inner.read_many(requests)
    }

    pub fn read_many_pread(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        PreadReader::new().read_many(requests)
    }

    fn new_io_uring(queue_depth: u32) -> io::Result<Self> {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            Ok(Self {
                inner: ChunkReaderInner::IoUring(IoUringReader::new(queue_depth)?),
                configured_mode: ChunkReadMode::IoUring,
                queue_depth,
            })
        }
        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            let _ = queue_depth;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "io_uring is not available on this platform or feature set",
            ))
        }
    }

    fn try_io_uring(queue_depth: u32) -> io::Result<Self> {
        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        {
            Self::new_io_uring(queue_depth)
        }
        #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
        {
            let _ = queue_depth;
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
    IoUring(IoUringReader),
}

impl ChunkReaderInner {
    fn mode(&self) -> ChunkReadMode {
        match self {
            ChunkReaderInner::Pread(_) => ChunkReadMode::Pread,
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            ChunkReaderInner::IoUring(_) => ChunkReadMode::IoUring,
        }
    }

    fn read_many(&self, requests: &[ReadRequest]) -> io::Result<Vec<ReadResult>> {
        match self {
            ChunkReaderInner::Pread(reader) => reader.read_many(requests),
            #[cfg(all(target_os = "linux", feature = "io_uring"))]
            ChunkReaderInner::IoUring(reader) => reader.read_many(requests),
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
        if cfg!(all(target_os = "linux", feature = "io_uring")) {
            assert!(
                matches!(reader.mode(), ChunkReadMode::IoUring | ChunkReadMode::Pread),
                "auto mode should select io_uring when available, otherwise pread"
            );
        } else {
            assert_eq!(reader.mode(), ChunkReadMode::Pread);
        }
    }

    #[cfg(not(all(target_os = "linux", feature = "io_uring")))]
    #[test]
    fn forced_io_uring_does_not_silently_fall_back() {
        let error = match ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::IoUring,
            queue_depth: 8,
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
            file,
            offset: 6,
            len: 5,
        };
        let reader = ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::Pread,
            queue_depth: 1,
        })
        .unwrap();
        let results = reader.read_many(&[req]).unwrap();
        assert_eq!(results[0].bytes, b"world");
    }
}
