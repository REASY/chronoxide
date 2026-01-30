use std::env;
use std::fs::File;
use std::io;
use std::path::{Path, PathBuf};

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
#[cfg(all(target_os = "linux", feature = "io_uring"))]
use io_uring::{IoUring, opcode, squeue, types};
use tempfile::NamedTempFile;

const DEFAULT_FILE_MB: u64 = 256;
const DEFAULT_FRAMES: usize = 256;
const DEFAULT_CHUNK_KB: &[usize] = &[32, 128, 256];
const DEFAULT_QUEUE_DEPTHS: &[u32] = &[8, 32, 128];
const DEFAULT_FILES: usize = 1;

const ENV_FILE_MB: &str = "IO_WRITE_BENCH_FILE_MB";
const ENV_FRAMES: &str = "IO_WRITE_BENCH_FRAMES";
const ENV_CHUNK_KB: &str = "IO_WRITE_BENCH_CHUNK_KB";
const ENV_QUEUE_DEPTHS: &str = "IO_WRITE_BENCH_QUEUE_DEPTHS";
const ENV_DIR: &str = "IO_WRITE_BENCH_DIR";
const ENV_FILES: &str = "IO_WRITE_BENCH_FILES";
const ENV_REGISTER_FILES: &str = "IO_WRITE_BENCH_REGISTER_FILES";
const ENV_REGISTER_BUFFERS: &str = "IO_WRITE_BENCH_REGISTER_BUFFERS";
const ENV_URING_FSYNC: &str = "IO_WRITE_BENCH_URING_FSYNC";
const ENV_FSYNC: &str = "IO_WRITE_BENCH_FSYNC";

struct BenchConfig {
    file_bytes: u64,
    frames: usize,
    chunk_sizes: Vec<usize>,
    queue_depths: Vec<u32>,
    dir: Option<PathBuf>,
    files: usize,
    register_files: bool,
    register_buffers: bool,
    uring_fsync: bool,
    fsync: bool,
}

impl BenchConfig {
    fn from_env() -> Self {
        let file_mb = env_u64(ENV_FILE_MB, DEFAULT_FILE_MB);
        let frames = env_usize(ENV_FRAMES, DEFAULT_FRAMES).max(1);
        let chunk_sizes = env_list_kb(ENV_CHUNK_KB, DEFAULT_CHUNK_KB);
        let queue_depths = env_list_u32(ENV_QUEUE_DEPTHS, DEFAULT_QUEUE_DEPTHS);
        let dir = env_path(ENV_DIR);
        let files = env_usize(ENV_FILES, DEFAULT_FILES).max(1);
        let register_files = env_bool(ENV_REGISTER_FILES, false);
        let register_buffers = env_bool(ENV_REGISTER_BUFFERS, false);
        let fsync = env_bool(ENV_FSYNC, false);
        let uring_fsync = fsync && env_bool(ENV_URING_FSYNC, false);

        Self {
            file_bytes: file_mb.saturating_mul(1024 * 1024),
            frames,
            chunk_sizes,
            queue_depths,
            dir,
            files,
            register_files,
            register_buffers,
            uring_fsync,
            fsync,
        }
    }
}

fn io_write_benches(c: &mut Criterion) {
    let config = BenchConfig::from_env();
    let max_chunk = config
        .chunk_sizes
        .iter()
        .copied()
        .max()
        .unwrap_or(32 * 1024);
    let min_bytes = (config.frames as u64).saturating_mul(max_chunk as u64);
    let file_bytes = config.file_bytes.max(min_bytes);
    let bench_files =
        create_files(file_bytes, config.files, config.dir.as_deref()).expect("create bench file");
    let file_count = bench_files.files.len();

    for &chunk_size in &config.chunk_sizes {
        if chunk_size > u32::MAX as usize {
            continue;
        }
        let total_bytes =
            (config.frames as u64).saturating_mul(chunk_size as u64) * file_count as u64;
        let mut group = c.benchmark_group(format!(
            "io_write_{}kb_{}_files{}",
            chunk_size / 1024,
            if config.fsync { "fsync" } else { "buffered" },
            file_count
        ));
        group.throughput(Throughput::Bytes(total_bytes));

        let buf = vec![0u8; chunk_size];

        group.bench_with_input(BenchmarkId::new("write", config.frames), &buf, |b, buf| {
            b.iter(|| {
                write_sequential(&bench_files.files, buf, config.frames, config.fsync)
                    .expect("write benchmark");
            });
        });

        #[cfg(all(target_os = "linux", feature = "io_uring"))]
        for &queue_depth in &config.queue_depths {
            let mut ring = IoUring::new(queue_depth).expect("io_uring init");
            let file_mode = configure_ring(
                &mut ring,
                &bench_files.files,
                &buf,
                config.register_files,
                config.register_buffers,
            )
            .expect("io_uring register");
            let label = io_uring_label(
                queue_depth,
                config.register_files,
                config.register_buffers,
                config.uring_fsync,
            );
            group.bench_with_input(BenchmarkId::new(label, config.frames), &buf, |b, buf| {
                b.iter(|| {
                    write_sequential_uring(
                        &mut ring,
                        &bench_files.files,
                        &file_mode,
                        buf,
                        config.frames,
                        queue_depth,
                        config.register_buffers,
                        config.uring_fsync,
                        config.fsync,
                    )
                    .expect("io_uring write benchmark");
                });
            });
        }

        group.finish();
    }
}

struct BenchFiles {
    _temps: Vec<NamedTempFile>,
    files: Vec<File>,
}

fn create_files(size: u64, count: usize, dir: Option<&Path>) -> io::Result<BenchFiles> {
    let mut temps = Vec::with_capacity(count);
    let mut files = Vec::with_capacity(count);
    for idx in 0..count {
        let mut builder = tempfile::Builder::new();
        builder.prefix("chronoxide-io-writes-");
        let tmp = match dir {
            Some(dir) => builder.tempfile_in(dir)?,
            None => builder.tempfile()?,
        };
        tmp.as_file().set_len(size)?;
        tmp.as_file().sync_all()?;
        if idx == 0 {
            if count == 1 {
                println!("created bench file: {:?}", tmp.path());
            } else {
                println!("created bench file[0] (of {count}): {:?}", tmp.path());
            }
        }
        files.push(tmp.reopen()?);
        temps.push(tmp);
    }
    Ok(BenchFiles {
        _temps: temps,
        files,
    })
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
enum FileMode {
    Fixed,
    Raw(Vec<types::Fd>),
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn configure_ring(
    ring: &mut IoUring,
    files: &[File],
    buf: &[u8],
    register_files: bool,
    register_buffers: bool,
) -> io::Result<FileMode> {
    if register_files {
        use std::os::unix::io::AsRawFd;

        if files.len() > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "file count exceeds u32::MAX",
            ));
        }
        let fds: Vec<_> = files.iter().map(|file| file.as_raw_fd()).collect();
        ring.submitter().register_files(&fds)?;
    }

    if register_buffers {
        if buf.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "registered buffer must be non-empty",
            ));
        }
        let iov = libc::iovec {
            iov_base: buf.as_ptr() as *mut _,
            iov_len: buf.len(),
        };
        unsafe {
            ring.submitter().register_buffers(&[iov])?;
        }
    }

    if register_files {
        Ok(FileMode::Fixed)
    } else {
        use std::os::unix::io::AsRawFd;

        Ok(FileMode::Raw(
            files
                .iter()
                .map(|file| types::Fd(file.as_raw_fd()))
                .collect(),
        ))
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn io_uring_label(
    queue_depth: u32,
    register_files: bool,
    register_buffers: bool,
    uring_fsync: bool,
) -> String {
    let mut label = format!("io_uring_qd{queue_depth}");
    if register_files {
        label.push_str("_fixedfiles");
    }
    if register_buffers {
        label.push_str("_fixedbuf");
    }
    if uring_fsync {
        label.push_str("_uringfsync");
    }
    label
}

fn write_sequential(files: &[File], buf: &[u8], frames: usize, fsync: bool) -> io::Result<()> {
    let mut offsets = vec![0u64; files.len()];
    let len = buf.len() as u64;
    for _ in 0..frames {
        for (idx, file) in files.iter().enumerate() {
            write_exact_at(file, offsets[idx], buf)?;
            offsets[idx] = offsets[idx].saturating_add(len);
        }
    }
    if fsync {
        for file in files {
            file.sync_data()?;
        }
    }
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn write_sequential_uring(
    ring: &mut IoUring,
    files: &[File],
    file_mode: &FileMode,
    buf: &[u8],
    frames: usize,
    queue_depth: u32,
    use_fixed_buf: bool,
    use_uring_fsync: bool,
    fsync: bool,
) -> io::Result<()> {
    let len = buf.len() as u32;
    let file_count = files.len();
    if file_count == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "no files configured for io_uring writes",
        ));
    }
    let mut offsets = vec![0u64; file_count];
    let mut write_counts = vec![0usize; file_count];
    let mut schedule = Vec::with_capacity(frames.saturating_mul(file_count));
    for _ in 0..frames {
        for file_idx in 0..file_count {
            schedule.push(file_idx);
        }
    }

    let mut schedule_idx = 0usize;
    let mut inflight = 0usize;
    let max_inflight = ring.submission().capacity().min(queue_depth as usize);
    let use_uring_fsync = fsync && use_uring_fsync;

    const WRITE_TAG: u64 = 1;
    const FSYNC_TAG: u64 = 2;

    while schedule_idx < schedule.len() || inflight > 0 {
        {
            let mut sq = ring.submission();
            while schedule_idx < schedule.len() {
                let file_idx = schedule[schedule_idx];
                let is_last = write_counts[file_idx] + 1 == frames;
                let need_fsync = use_uring_fsync && is_last;
                let needed = if need_fsync { 2 } else { 1 };
                if inflight + needed > max_inflight {
                    break;
                }
                if sq.capacity().saturating_sub(sq.len()) < needed {
                    break;
                }

                let offset = offsets[file_idx];
                offsets[file_idx] = offsets[file_idx].saturating_add(len as u64);
                write_counts[file_idx] += 1;
                schedule_idx += 1;

                let mut write_entry =
                    build_write_entry(file_mode, file_idx, buf, len, offset, use_fixed_buf)
                        .user_data(WRITE_TAG);
                if need_fsync {
                    write_entry = write_entry.flags(squeue::Flags::IO_LINK);
                }
                unsafe {
                    sq.push(&write_entry).map_err(|_| {
                        io::Error::new(io::ErrorKind::Other, "submission queue full")
                    })?;
                }
                inflight += 1;

                if need_fsync {
                    let fsync_entry = build_fsync_entry(file_mode, file_idx).user_data(FSYNC_TAG);
                    unsafe {
                        sq.push(&fsync_entry).map_err(|_| {
                            io::Error::new(io::ErrorKind::Other, "submission queue full")
                        })?;
                    }
                    inflight += 1;
                }
            }
        }

        if inflight == 0 {
            break;
        }
        if schedule_idx >= schedule.len() || inflight >= max_inflight {
            ring.submit_and_wait(1)?;
        } else {
            ring.submit()?;
        }

        while let Some(cqe) = ring.completion().next() {
            let res = cqe.result();
            if res < 0 {
                return Err(io::Error::from_raw_os_error(-res));
            }
            match cqe.user_data() {
                WRITE_TAG => {
                    if res as u32 != len {
                        return Err(io::Error::new(io::ErrorKind::WriteZero, "short write"));
                    }
                }
                FSYNC_TAG => {
                    if res != 0 {
                        return Err(io::Error::new(io::ErrorKind::Other, "fsync failed"));
                    }
                }
                _ => {}
            }
            inflight = inflight.saturating_sub(1);
        }
    }

    if fsync && !use_uring_fsync {
        for file in files {
            file.sync_data()?;
        }
    }
    Ok(())
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn build_write_entry(
    file_mode: &FileMode,
    file_idx: usize,
    buf: &[u8],
    len: u32,
    offset: u64,
    use_fixed_buf: bool,
) -> squeue::Entry {
    match (file_mode, use_fixed_buf) {
        (FileMode::Fixed, true) => {
            opcode::WriteFixed::new(types::Fixed(file_idx as u32), buf.as_ptr(), len, 0)
                .offset(offset)
                .build()
        }
        (FileMode::Fixed, false) => {
            opcode::Write::new(types::Fixed(file_idx as u32), buf.as_ptr(), len)
                .offset(offset)
                .build()
        }
        (FileMode::Raw(fds), true) => opcode::WriteFixed::new(fds[file_idx], buf.as_ptr(), len, 0)
            .offset(offset)
            .build(),
        (FileMode::Raw(fds), false) => opcode::Write::new(fds[file_idx], buf.as_ptr(), len)
            .offset(offset)
            .build(),
    }
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
fn build_fsync_entry(file_mode: &FileMode, file_idx: usize) -> squeue::Entry {
    match file_mode {
        FileMode::Fixed => opcode::Fsync::new(types::Fixed(file_idx as u32)).build(),
        FileMode::Raw(fds) => opcode::Fsync::new(fds[file_idx]).build(),
    }
}

#[cfg(unix)]
fn write_exact_at(file: &File, offset: u64, buf: &[u8]) -> io::Result<()> {
    use std::os::unix::fs::FileExt;

    let mut written = 0usize;
    while written < buf.len() {
        let bytes = file.write_at(&buf[written..], offset + written as u64)?;
        if bytes == 0 {
            return Err(io::Error::new(io::ErrorKind::WriteZero, "short write"));
        }
        written += bytes;
    }
    Ok(())
}

#[cfg(not(unix))]
fn write_exact_at(_file: &File, _offset: u64, _buf: &[u8]) -> io::Result<()> {
    Err(io::Error::new(
        io::ErrorKind::Unsupported,
        "pwrite is not supported on this platform",
    ))
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(default)
}

fn env_path(name: &str) -> Option<PathBuf> {
    env::var(name).ok().map(PathBuf::from)
}

fn env_list_kb(name: &str, default_kb: &[usize]) -> Vec<usize> {
    let parsed = env::var(name)
        .ok()
        .map(|value| parse_csv_usize(&value))
        .unwrap_or_default();
    let values = if parsed.is_empty() {
        default_kb.to_vec()
    } else {
        parsed
    };
    values
        .into_iter()
        .filter(|value| *value > 0)
        .map(|kb| kb * 1024)
        .collect()
}

fn env_list_u32(name: &str, default: &[u32]) -> Vec<u32> {
    let parsed = env::var(name)
        .ok()
        .map(|value| parse_csv_u32(&value))
        .unwrap_or_default();
    let values = if parsed.is_empty() {
        default.to_vec()
    } else {
        parsed
    };
    values.into_iter().filter(|value| *value > 0).collect()
}

fn env_bool(name: &str, default: bool) -> bool {
    env::var(name)
        .ok()
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(default)
}

fn parse_csv_usize(value: &str) -> Vec<usize> {
    value
        .split(',')
        .filter_map(|item| item.trim().parse::<usize>().ok())
        .collect()
}

fn parse_csv_u32(value: &str) -> Vec<u32> {
    value
        .split(',')
        .filter_map(|item| item.trim().parse::<u32>().ok())
        .collect()
}

criterion_group!(io_write_group, io_write_benches);
criterion_main!(io_write_group);
