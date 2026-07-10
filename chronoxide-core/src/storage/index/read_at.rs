use std::fs::File;
use std::io::{self, Cursor};

#[doc(hidden)]
pub trait SegmentIndexReadAt: Send + Sync {
    fn len(&self) -> io::Result<u64>;

    fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()>;
}

#[cfg(any(unix, windows))]
impl SegmentIndexReadAt for File {
    fn len(&self) -> io::Result<u64> {
        Ok(self.metadata()?.len())
    }

    fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()> {
        read_exact_at_loop(offset, dst, |read_offset, destination| {
            file_read_at(self, read_offset, destination)
        })
    }
}

impl<T> SegmentIndexReadAt for Cursor<T>
where
    T: AsRef<[u8]> + Send + Sync,
{
    fn len(&self) -> io::Result<u64> {
        u64::try_from(self.get_ref().as_ref().len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "segment index source length exceeds u64",
            )
        })
    }

    fn read_exact_at(&self, offset: u64, dst: &mut [u8]) -> io::Result<()> {
        let end = checked_read_end(offset, dst.len())?;
        let start = usize::try_from(offset).map_err(|_| unexpected_eof())?;
        let end = usize::try_from(end).map_err(|_| unexpected_eof())?;
        let source = self
            .get_ref()
            .as_ref()
            .get(start..end)
            .ok_or_else(unexpected_eof)?;
        dst.copy_from_slice(source);
        Ok(())
    }
}

fn read_exact_at_loop(
    mut offset: u64,
    mut dst: &mut [u8],
    mut read_once: impl FnMut(u64, &mut [u8]) -> io::Result<usize>,
) -> io::Result<()> {
    checked_read_end(offset, dst.len())?;
    while !dst.is_empty() {
        let read_len = match read_once(offset, dst) {
            Ok(0) => return Err(unexpected_eof()),
            Ok(read_len) => read_len,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        if read_len > dst.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "positional read returned more bytes than requested",
            ));
        }
        offset = offset
            .checked_add(u64::try_from(read_len).map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "positional read length exceeds u64",
                )
            })?)
            .ok_or_else(offset_overflow)?;
        dst = &mut dst[read_len..];
    }
    Ok(())
}

fn checked_read_end(offset: u64, len: usize) -> io::Result<u64> {
    let len = u64::try_from(len).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "positional read length exceeds u64",
        )
    })?;
    offset.checked_add(len).ok_or_else(offset_overflow)
}

fn offset_overflow() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "segment index positional read offset overflow",
    )
}

fn unexpected_eof() -> io::Error {
    io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "segment index positional read reached EOF",
    )
}

#[cfg(unix)]
fn file_read_at(file: &File, offset: u64, dst: &mut [u8]) -> io::Result<usize> {
    <File as std::os::unix::fs::FileExt>::read_at(file, dst, offset)
}

#[cfg(windows)]
fn file_read_at(file: &File, offset: u64, dst: &mut [u8]) -> io::Result<usize> {
    <File as std::os::windows::fs::FileExt>::seek_read(file, dst, offset)
}

#[cfg(test)]
mod tests {
    use std::fs::File;
    use std::io::{self, Cursor, Write};
    use std::sync::{Arc, Barrier};
    use std::thread;

    use super::{SegmentIndexReadAt, read_exact_at_loop};

    const THREAD_COUNT: usize = 16;
    const RANGE_LEN: usize = 257;
    const READ_ITERATIONS: usize = 100;

    fn patterned_bytes() -> Vec<u8> {
        (0..THREAD_COUNT * RANGE_LEN)
            .map(|index| ((index * 31 + 7) % 251) as u8)
            .collect()
    }

    fn file_with_bytes(bytes: &[u8]) -> File {
        let mut file = tempfile::tempfile().expect("create temporary file");
        file.write_all(bytes).expect("write temporary file");
        file.flush().expect("flush temporary file");
        file
    }

    fn range_start(thread_index: usize, iteration: usize, source_len: usize) -> usize {
        if iteration % 2 == 0 {
            ((thread_index + iteration / 2) % THREAD_COUNT) * RANGE_LEN
        } else {
            (iteration * 19 + thread_index * 17) % (source_len - RANGE_LEN + 1)
        }
    }

    #[test]
    fn file_supports_sixteen_concurrent_positional_ranges() {
        let expected = Arc::new(patterned_bytes());
        let file = Arc::new(file_with_bytes(&expected));
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));
        assert_eq!(
            SegmentIndexReadAt::len(file.as_ref()).unwrap(),
            expected.len() as u64
        );

        let handles = (0..THREAD_COUNT)
            .map(|thread_index| {
                let expected = Arc::clone(&expected);
                let file = Arc::clone(&file);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut actual = vec![0u8; RANGE_LEN];
                    barrier.wait();
                    for iteration in 0..READ_ITERATIONS {
                        let start = range_start(thread_index, iteration, expected.len());
                        SegmentIndexReadAt::read_exact_at(file.as_ref(), start as u64, &mut actual)
                            .unwrap();
                        assert_eq!(actual, expected[start..start + RANGE_LEN]);
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn cursor_supports_sixteen_concurrent_positional_ranges() {
        let expected = Arc::new(patterned_bytes());
        let cursor = Arc::new(Cursor::new(expected.as_ref().clone()));
        let barrier = Arc::new(Barrier::new(THREAD_COUNT));
        assert_eq!(
            SegmentIndexReadAt::len(cursor.as_ref()).unwrap(),
            expected.len() as u64
        );

        let handles = (0..THREAD_COUNT)
            .map(|thread_index| {
                let expected = Arc::clone(&expected);
                let cursor = Arc::clone(&cursor);
                let barrier = Arc::clone(&barrier);
                thread::spawn(move || {
                    let mut actual = vec![0u8; RANGE_LEN];
                    barrier.wait();
                    for iteration in 0..READ_ITERATIONS {
                        let start = range_start(thread_index, iteration, expected.len());
                        SegmentIndexReadAt::read_exact_at(
                            cursor.as_ref(),
                            start as u64,
                            &mut actual,
                        )
                        .unwrap();
                        assert_eq!(actual, expected[start..start + RANGE_LEN]);
                    }
                })
            })
            .collect::<Vec<_>>();

        for handle in handles {
            handle.join().unwrap();
        }
    }

    #[test]
    fn positional_read_rejects_offset_overflow() {
        let cursor = Cursor::new(vec![1u8]);
        let mut destination = [0u8; 8];
        let cursor_error =
            SegmentIndexReadAt::read_exact_at(&cursor, u64::MAX - 3, &mut destination).unwrap_err();
        assert_eq!(cursor_error.kind(), io::ErrorKind::InvalidInput);

        let file = file_with_bytes(&[1]);
        let file_error =
            SegmentIndexReadAt::read_exact_at(&file, u64::MAX - 3, &mut destination).unwrap_err();
        assert_eq!(file_error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn positional_read_reports_one_byte_past_eof() {
        let cursor = Cursor::new(vec![1u8, 2, 3]);
        let mut byte = [0u8; 1];
        let cursor_error = SegmentIndexReadAt::read_exact_at(&cursor, 3, &mut byte).unwrap_err();
        assert_eq!(cursor_error.kind(), io::ErrorKind::UnexpectedEof);

        let file = file_with_bytes(&[1, 2, 3]);
        let file_error = SegmentIndexReadAt::read_exact_at(&file, 3, &mut byte).unwrap_err();
        assert_eq!(file_error.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn positional_empty_read_at_eof_succeeds() {
        let cursor = Cursor::new(vec![1u8, 2, 3]);
        SegmentIndexReadAt::read_exact_at(&cursor, 3, &mut []).unwrap();

        let file = file_with_bytes(&[1, 2, 3]);
        SegmentIndexReadAt::read_exact_at(&file, 3, &mut []).unwrap();
    }

    #[test]
    fn positional_read_loop_retries_interrupted_and_short_reads() {
        let source = b"positional-read-loop";
        let mut actual = vec![0u8; source.len()];
        let mut calls = 0usize;

        read_exact_at_loop(0, &mut actual, |offset, destination| {
            calls += 1;
            if calls == 1 {
                return Err(io::Error::new(io::ErrorKind::Interrupted, "injected"));
            }
            let start = usize::try_from(offset).unwrap();
            let read_len = destination.len().min(3);
            destination[..read_len].copy_from_slice(&source[start..start + read_len]);
            Ok(read_len)
        })
        .unwrap();

        assert_eq!(actual, source);
        assert!(calls > 2, "expected retry plus multiple short reads");
    }
}
