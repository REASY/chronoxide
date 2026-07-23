use super::*;

static BENCHMARK_OUTPUT_TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
const BENCHMARK_OUTPUT_TEMP_ATTEMPTS: usize = 128;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in super::super) struct PreparedBenchmarkOutput {
    path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct UnresolvedBenchmarkOutput {
    parent: PathBuf,
    file_name: OsString,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in super::super) enum BenchmarkOutputKind {
    Markdown,
    Raw,
}

#[derive(Debug)]
pub(in super::super) struct StagedBenchmarkOutput {
    destination: PreparedBenchmarkOutput,
    temp_path: PathBuf,
    published: bool,
}

impl StagedBenchmarkOutput {
    pub(in super::super) fn stage(
        destination: PreparedBenchmarkOutput,
        bytes: &[u8],
    ) -> io::Result<Self> {
        let parent = destination.path.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "benchmark output has no parent directory",
            )
        })?;
        for _ in 0..BENCHMARK_OUTPUT_TEMP_ATTEMPTS {
            let sequence = BENCHMARK_OUTPUT_TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
            let temp_path =
                parent.join(format!(".chronoxide-tmp-{}-{sequence}", std::process::id()));
            let mut file = match File::options()
                .write(true)
                .create_new(true)
                .open(&temp_path)
            {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            };
            let staged = Self {
                destination,
                temp_path,
                published: false,
            };
            let write_result = file.write_all(bytes).and_then(|_| file.sync_all());
            drop(file);
            write_result?;
            return Ok(staged);
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not reserve a unique benchmark output temporary file",
        ))
    }

    fn publish(&mut self) -> io::Result<()> {
        fs::rename(&self.temp_path, &self.destination.path)?;
        self.published = true;
        Ok(())
    }
}

impl Drop for StagedBenchmarkOutput {
    fn drop(&mut self) {
        if !self.published {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}

pub(super) fn publish_benchmark_outputs(
    markdown_output: &Path,
    markdown_bytes: &[u8],
    raw: Option<(&Path, &[u8])>,
) -> io::Result<()> {
    publish_benchmark_outputs_with_stager(
        markdown_output,
        markdown_bytes,
        raw,
        |destination, bytes, _| StagedBenchmarkOutput::stage(destination.clone(), bytes),
    )
}

pub(in super::super) fn publish_benchmark_outputs_with_stager<F>(
    markdown_output: &Path,
    markdown_bytes: &[u8],
    raw: Option<(&Path, &[u8])>,
    mut stage: F,
) -> io::Result<()>
where
    F: FnMut(
        &PreparedBenchmarkOutput,
        &[u8],
        BenchmarkOutputKind,
    ) -> io::Result<StagedBenchmarkOutput>,
{
    let raw_output = raw.map(|(path, _)| path);
    let (markdown_destination, raw_destination) =
        preflight_benchmark_outputs(markdown_output, raw_output)?;
    let mut markdown_stage = stage(
        &markdown_destination,
        markdown_bytes,
        BenchmarkOutputKind::Markdown,
    )?;
    let mut raw_stage = match (raw, raw_destination.as_ref()) {
        (Some((_, bytes)), Some(destination)) => {
            Some(stage(destination, bytes, BenchmarkOutputKind::Raw)?)
        }
        (None, None) => None,
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "raw benchmark output preflight was inconsistent",
            ));
        }
    };

    let latest_destinations = preflight_benchmark_outputs(markdown_output, raw_output)?;
    if latest_destinations != (markdown_destination, raw_destination) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "benchmark output destinations changed while staging",
        ));
    }

    if let Some(raw_stage) = &mut raw_stage {
        raw_stage.publish()?;
    }
    markdown_stage.publish()
}

pub(super) fn preflight_benchmark_outputs(
    markdown_output: &Path,
    raw_output: Option<&Path>,
) -> io::Result<(PreparedBenchmarkOutput, Option<PreparedBenchmarkOutput>)> {
    let markdown = identify_benchmark_output(markdown_output)?;
    let raw = raw_output.map(identify_benchmark_output).transpose()?;

    fs::create_dir_all(&markdown.parent)?;
    if let Some(raw) = &raw {
        fs::create_dir_all(&raw.parent)?;
    }

    let markdown = validate_benchmark_output(markdown)?;
    let raw = raw.map(validate_benchmark_output).transpose()?;
    if let Some(raw) = &raw
        && (markdown.path == raw.path
            || existing_outputs_share_identity(&markdown.path, &raw.path)?)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "Markdown and raw benchmark outputs resolve to the same file",
        ));
    }
    Ok((markdown, raw))
}

fn identify_benchmark_output(path: &Path) -> io::Result<UnresolvedBenchmarkOutput> {
    let file_name = path.file_name().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("benchmark output path has no filename: {}", path.display()),
        )
    })?;
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    Ok(UnresolvedBenchmarkOutput {
        parent: parent.to_path_buf(),
        file_name: file_name.to_os_string(),
    })
}

fn validate_benchmark_output(
    unresolved: UnresolvedBenchmarkOutput,
) -> io::Result<PreparedBenchmarkOutput> {
    let canonical_parent = fs::canonicalize(&unresolved.parent)?;
    let normalized = canonical_parent.join(unresolved.file_name);
    match fs::symlink_metadata(&normalized) {
        Ok(metadata) if metadata.file_type().is_symlink() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "benchmark output destination must not be a symlink: {}",
                    normalized.display()
                ),
            ));
        }
        Ok(metadata) if !metadata.is_file() => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "benchmark output destination must be a regular file: {}",
                    normalized.display()
                ),
            ));
        }
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    Ok(PreparedBenchmarkOutput { path: normalized })
}

#[cfg(unix)]
fn existing_outputs_share_identity(left: &Path, right: &Path) -> io::Result<bool> {
    use std::os::unix::fs::MetadataExt;

    let Some(left) = existing_output_metadata(left)? else {
        return Ok(false);
    };
    let Some(right) = existing_output_metadata(right)? else {
        return Ok(false);
    };
    Ok(left.dev() == right.dev() && left.ino() == right.ino())
}

#[cfg(not(unix))]
fn existing_outputs_share_identity(_left: &Path, _right: &Path) -> io::Result<bool> {
    Ok(false)
}

#[cfg(unix)]
fn existing_output_metadata(path: &Path) -> io::Result<Option<fs::Metadata>> {
    match fs::metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error),
    }
}
