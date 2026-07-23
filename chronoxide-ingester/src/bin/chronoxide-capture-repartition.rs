use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use chronoxide_capture::{
    CaptureManifest, CompressionMethod, OtlpCaptureReader, OtlpCaptureWriter, read_manifest,
};
use clap::{Parser, ValueEnum};
use serde::Serialize;
use sha2::{Digest, Sha256};

type DynError = Box<dyn std::error::Error + Send + Sync>;

const REPORT_SCHEMA: &str = "chronoxide-capture-repartition-v2";
const INPUT_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-repartition-input-v2\0";
const OUTPUT_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-repartition-output-v2\0";
const MAPPING_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-repartition-mapping-v2\0";
const CONTENT_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-repartition-content-v2\0";

#[derive(Debug, Parser)]
#[command(
    about = "Deterministically repartition a Chronoxide capture without decoding OTLP payloads"
)]
struct Args {
    /// Source capture directory, including a capture with one partition.
    #[arg(long)]
    input: PathBuf,
    /// New, empty output capture directory.
    #[arg(long)]
    output: PathBuf,
    /// New JSON report path outside the output capture directory.
    #[arg(long)]
    report: PathBuf,
    #[arg(long, value_enum)]
    layout: PartitionLayout,
    #[arg(long, default_value_t = 16)]
    partitions: u32,
    /// Optional deterministic input prefix.
    #[arg(long)]
    max_messages: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, ValueEnum)]
#[serde(rename_all = "kebab-case")]
enum PartitionLayout {
    Uniform,
    Skew80_20,
}

impl PartitionLayout {
    fn partition(self, ordinal: u64, partition_count: u32) -> u32 {
        match self {
            Self::Uniform => (ordinal % u64::from(partition_count)) as u32,
            Self::Skew80_20 => {
                if ordinal % 5 != 4 {
                    0
                } else {
                    1 + ((ordinal / 5) % u64::from(partition_count - 1)) as u32
                }
            }
        }
    }

    const fn mapping_spec(self) -> &'static str {
        match self {
            Self::Uniform => "destination_partition = global_ordinal % partition_count",
            Self::Skew80_20 => {
                "global_ordinal % 5 in 0..=3 -> partition 0; every fifth record -> 1 + ((global_ordinal / 5) % (partition_count - 1))"
            }
        }
    }
}

#[derive(Debug, Default, Clone, Serialize)]
struct PartitionSummary {
    partition: u32,
    message_count: u64,
    payload_bytes: u64,
    first_global_ordinal: Option<u64>,
    last_global_ordinal: Option<u64>,
}

impl PartitionSummary {
    fn observe(&mut self, ordinal: u64, payload_len: usize) {
        self.message_count = self.message_count.saturating_add(1);
        self.payload_bytes = self.payload_bytes.saturating_add(payload_len as u64);
        self.first_global_ordinal.get_or_insert(ordinal);
        self.last_global_ordinal = Some(ordinal);
    }
}

#[derive(Debug, Serialize)]
struct RepartitionReport {
    schema: &'static str,
    input: String,
    output: String,
    layout: PartitionLayout,
    mapping_spec: &'static str,
    partition_count: u32,
    max_messages: Option<u64>,
    topic: String,
    compression: CompressionMethod,
    messages: u64,
    payload_bytes: u64,
    partitions: Vec<PartitionSummary>,
    input_manifest_sha256: String,
    output_manifest_sha256: String,
    input_stream_sha256: String,
    output_stream_sha256: String,
    input_content_stream_sha256: String,
    output_content_stream_sha256: String,
    content_streams_equal: bool,
    mapping_sha256: String,
    output_tree_sha256: String,
    reopened_verification: bool,
}

#[derive(Clone, PartialEq, Eq)]
struct LogicalFingerprints {
    input: String,
    output: String,
    input_content: String,
    output_content: String,
    mapping: String,
}

struct FingerprintBuilder {
    input: Sha256,
    output: Sha256,
    input_content: Sha256,
    output_content: Sha256,
    mapping: Sha256,
}

#[derive(Clone, Copy)]
struct CanonicalContent<'a> {
    topic: &'a str,
    timestamp_ms: i64,
    captured_at_ms: i64,
    payload: &'a [u8],
}

struct FingerprintRecord<'a> {
    ordinal: u64,
    source_sequence: u64,
    output_sequence: u64,
    source_partition: i32,
    source_offset: i64,
    destination_partition: u32,
    destination_offset: i64,
    input: CanonicalContent<'a>,
    output: CanonicalContent<'a>,
}

impl FingerprintBuilder {
    fn new() -> Self {
        let mut input = Sha256::new();
        input.update(INPUT_FINGERPRINT_DOMAIN);
        let mut output = Sha256::new();
        output.update(OUTPUT_FINGERPRINT_DOMAIN);
        let mut input_content = Sha256::new();
        input_content.update(CONTENT_FINGERPRINT_DOMAIN);
        let mut output_content = Sha256::new();
        output_content.update(CONTENT_FINGERPRINT_DOMAIN);
        let mut mapping = Sha256::new();
        mapping.update(MAPPING_FINGERPRINT_DOMAIN);
        Self {
            input,
            output,
            input_content,
            output_content,
            mapping,
        }
    }

    fn observe(&mut self, record: &FingerprintRecord<'_>) {
        update_u64(&mut self.input, record.ordinal);
        update_u64(&mut self.input, record.source_sequence);
        update_bytes(&mut self.input, record.input.topic.as_bytes());
        self.input.update(record.source_partition.to_le_bytes());
        self.input.update(record.source_offset.to_le_bytes());
        self.input.update(record.input.timestamp_ms.to_le_bytes());
        self.input.update(record.input.captured_at_ms.to_le_bytes());
        update_bytes(&mut self.input, record.input.payload);

        update_u64(&mut self.output, record.ordinal);
        update_u64(&mut self.output, record.output_sequence);
        update_bytes(&mut self.output, record.output.topic.as_bytes());
        self.output
            .update(record.destination_partition.to_le_bytes());
        self.output.update(record.destination_offset.to_le_bytes());
        self.output.update(record.output.timestamp_ms.to_le_bytes());
        self.output
            .update(record.output.captured_at_ms.to_le_bytes());
        update_bytes(&mut self.output, record.output.payload);

        observe_content(&mut self.input_content, record.ordinal, record.input);
        observe_content(&mut self.output_content, record.ordinal, record.output);

        update_u64(&mut self.mapping, record.ordinal);
        update_u64(&mut self.mapping, record.source_sequence);
        self.mapping.update(record.source_partition.to_le_bytes());
        self.mapping.update(record.source_offset.to_le_bytes());
        self.mapping
            .update(record.destination_partition.to_le_bytes());
        self.mapping.update(record.destination_offset.to_le_bytes());
        update_u64(&mut self.mapping, record.output_sequence);
    }

    fn finish(self) -> LogicalFingerprints {
        LogicalFingerprints {
            input: hex_digest(self.input.finalize()),
            output: hex_digest(self.output.finalize()),
            input_content: hex_digest(self.input_content.finalize()),
            output_content: hex_digest(self.output_content.finalize()),
            mapping: hex_digest(self.mapping.finalize()),
        }
    }
}

fn observe_content(hasher: &mut Sha256, ordinal: u64, content: CanonicalContent<'_>) {
    update_u64(hasher, ordinal);
    update_bytes(hasher, content.topic.as_bytes());
    hasher.update(content.timestamp_ms.to_le_bytes());
    hasher.update(content.captured_at_ms.to_le_bytes());
    update_bytes(hasher, content.payload);
}

fn update_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_le_bytes());
}

fn update_bytes(hasher: &mut Sha256, value: &[u8]) {
    update_u64(hasher, value.len() as u64);
    hasher.update(value);
}

fn hex_digest(bytes: impl AsRef<[u8]>) -> String {
    let mut out = String::with_capacity(bytes.as_ref().len() * 2);
    for byte in bytes.as_ref() {
        use std::fmt::Write as _;
        write!(&mut out, "{byte:02x}").expect("writing to String cannot fail");
    }
    out
}

fn invalid_input(message: impl Into<String>) -> DynError {
    io::Error::new(io::ErrorKind::InvalidInput, message.into()).into()
}

fn canonicalize_existing_dir(path: &Path, label: &str) -> Result<PathBuf, DynError> {
    let canonical = fs::canonicalize(path)?;
    if !canonical.is_dir() {
        return Err(invalid_input(format!("{label} must be a directory")));
    }
    Ok(canonical)
}

fn absolute_new_path(path: &Path, label: &str) -> Result<PathBuf, DynError> {
    if path.exists() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            format!("{label} already exists: {}", path.display()),
        )
        .into());
    }
    let parent = path
        .parent()
        .ok_or_else(|| invalid_input(format!("{label} has no parent")))?;
    let parent = fs::canonicalize(parent)?;
    let name = path
        .file_name()
        .ok_or_else(|| invalid_input(format!("{label} has no file name")))?;
    Ok(parent.join(name))
}

fn run(args: Args) -> Result<RepartitionReport, DynError> {
    if !(2..=i32::MAX as u32).contains(&args.partitions) {
        return Err(invalid_input("partitions must be in 2..=i32::MAX"));
    }
    if args.max_messages == Some(0) {
        return Err(invalid_input("max_messages must be positive when present"));
    }

    let input = canonicalize_existing_dir(&args.input, "input")?;
    let output = absolute_new_path(&args.output, "output")?;
    let report_path = absolute_new_path(&args.report, "report")?;
    if report_path.starts_with(&output) {
        return Err(invalid_input("report must be outside the output capture"));
    }
    if output.starts_with(&input) || input.starts_with(&output) {
        return Err(invalid_input(
            "input and output capture paths must not nest",
        ));
    }

    let input_manifest = read_manifest(&input)?;
    let input_manifest_sha256 = sha256_file(&input.join("manifest.json"))?;
    let mut reader = OtlpCaptureReader::open(&input)?;
    let mut writer = OtlpCaptureWriter::create(
        &output,
        input_manifest.topic.clone(),
        input_manifest.compression,
    )?;
    let mut offsets = vec![0u64; args.partitions as usize];
    let mut partitions: Vec<_> = (0..args.partitions)
        .map(|partition| PartitionSummary {
            partition,
            ..PartitionSummary::default()
        })
        .collect();
    let mut fingerprints = FingerprintBuilder::new();
    let mut messages = 0u64;
    let mut payload_bytes = 0u64;
    let mut previous_source_sequence = None;

    while args.max_messages.is_none_or(|limit| messages < limit) {
        let Some((source_sequence, message)) = reader.next_with_sequence()? else {
            break;
        };
        if previous_source_sequence.is_some_and(|previous| source_sequence <= previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source persisted sequence is not strictly increasing at global ordinal {messages}: previous={previous_source_sequence:?} actual={source_sequence}"
                ),
            )
            .into());
        }
        previous_source_sequence = Some(source_sequence);
        if message.topic != input_manifest.topic {
            return Err(invalid_input(format!(
                "record topic differs from manifest at global ordinal {messages}"
            )));
        }
        let destination_partition = args.layout.partition(messages, args.partitions);
        let partition_index = destination_partition as usize;
        let destination_offset = i64::try_from(offsets[partition_index]).map_err(|_| {
            invalid_input(format!(
                "destination partition {destination_partition} offset exceeds i64::MAX"
            ))
        })?;
        writer.append(
            destination_partition as i32,
            destination_offset,
            message.timestamp_ms,
            message.captured_at_ms,
            &message.payload,
        )?;
        let content = CanonicalContent {
            topic: &message.topic,
            timestamp_ms: message.timestamp_ms,
            captured_at_ms: message.captured_at_ms,
            payload: &message.payload,
        };
        fingerprints.observe(&FingerprintRecord {
            ordinal: messages,
            source_sequence,
            output_sequence: messages,
            source_partition: message.partition,
            source_offset: message.offset,
            destination_partition,
            destination_offset,
            input: content,
            output: content,
        });
        offsets[partition_index] = offsets[partition_index].saturating_add(1);
        partitions[partition_index].observe(messages, message.payload.len());
        messages = messages.saturating_add(1);
        payload_bytes = payload_bytes.saturating_add(message.payload.len() as u64);
    }
    if messages == 0 {
        return Err(invalid_input("input capture prefix is empty"));
    }
    if let Some(expected) = args.max_messages
        && messages != expected
    {
        return Err(invalid_input(format!(
            "input ended after {messages} records; max_messages requested {expected}"
        )));
    }
    writer.close()?;
    let expected_fingerprints = fingerprints.finish();
    let verified_fingerprints = verify_output(
        &input,
        &output,
        args.layout,
        args.partitions,
        messages,
        &input_manifest,
    )?;
    if verified_fingerprints != expected_fingerprints {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "reopened logical fingerprints differ from the write pass",
        )
        .into());
    }
    if verified_fingerprints.input_content != verified_fingerprints.output_content {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "canonical input/output content stream fingerprints differ",
        )
        .into());
    }

    let output_manifest = read_manifest(&output)?;
    validate_output_manifest(&output_manifest, &input_manifest, &partitions)?;
    let output_manifest_sha256 = sha256_file(&output.join("manifest.json"))?;
    let output_tree_sha256 = capture_tree_sha256(&output)?;
    let report = RepartitionReport {
        schema: REPORT_SCHEMA,
        input: input.display().to_string(),
        output: output.display().to_string(),
        layout: args.layout,
        mapping_spec: args.layout.mapping_spec(),
        partition_count: args.partitions,
        max_messages: args.max_messages,
        topic: input_manifest.topic,
        compression: input_manifest.compression,
        messages,
        payload_bytes,
        partitions,
        input_manifest_sha256,
        output_manifest_sha256,
        input_stream_sha256: expected_fingerprints.input,
        output_stream_sha256: expected_fingerprints.output,
        input_content_stream_sha256: expected_fingerprints.input_content,
        output_content_stream_sha256: expected_fingerprints.output_content,
        content_streams_equal: true,
        mapping_sha256: expected_fingerprints.mapping,
        output_tree_sha256,
        reopened_verification: true,
    };
    write_report(&report_path, &report)?;
    Ok(report)
}

fn verify_output(
    input: &Path,
    output: &Path,
    layout: PartitionLayout,
    partition_count: u32,
    message_count: u64,
    input_manifest: &CaptureManifest,
) -> Result<LogicalFingerprints, DynError> {
    let mut source = OtlpCaptureReader::open(input)?;
    let mut transformed = OtlpCaptureReader::open(output)?;
    let mut offsets = vec![0u64; partition_count as usize];
    let mut fingerprints = FingerprintBuilder::new();
    let mut previous_source_sequence = None;
    for ordinal in 0..message_count {
        let (source_sequence, source_message) = source.next_with_sequence()?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "source prefix ended during verify",
            )
        })?;
        let (output_sequence, output_message) =
            transformed.next_with_sequence()?.ok_or_else(|| {
                io::Error::new(io::ErrorKind::UnexpectedEof, "output ended during verify")
            })?;
        if previous_source_sequence.is_some_and(|previous| source_sequence <= previous) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source persisted sequence is not strictly increasing at ordinal {ordinal}"
                ),
            )
            .into());
        }
        previous_source_sequence = Some(source_sequence);
        let destination_partition = layout.partition(ordinal, partition_count);
        let destination_offset = i64::try_from(offsets[destination_partition as usize])?;
        if output_sequence != ordinal
            || output_message.topic != input_manifest.topic
            || output_message.partition != destination_partition as i32
            || output_message.offset != destination_offset
            || output_message.timestamp_ms != source_message.timestamp_ms
            || output_message.captured_at_ms != source_message.captured_at_ms
            || output_message.payload != source_message.payload
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("transformed record mismatch at global ordinal {ordinal}"),
            )
            .into());
        }
        fingerprints.observe(&FingerprintRecord {
            ordinal,
            source_sequence,
            output_sequence,
            source_partition: source_message.partition,
            source_offset: source_message.offset,
            destination_partition: output_message.partition as u32,
            destination_offset: output_message.offset,
            input: CanonicalContent {
                topic: &source_message.topic,
                timestamp_ms: source_message.timestamp_ms,
                captured_at_ms: source_message.captured_at_ms,
                payload: &source_message.payload,
            },
            output: CanonicalContent {
                topic: &output_message.topic,
                timestamp_ms: output_message.timestamp_ms,
                captured_at_ms: output_message.captured_at_ms,
                payload: &output_message.payload,
            },
        });
        offsets[destination_partition as usize] += 1;
    }
    if transformed.next_with_sequence()?.is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "output contains records after the selected prefix",
        )
        .into());
    }
    Ok(fingerprints.finish())
}

fn validate_output_manifest(
    output: &CaptureManifest,
    input: &CaptureManifest,
    expected: &[PartitionSummary],
) -> Result<(), DynError> {
    if output.topic != input.topic || output.compression != input.compression {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "output manifest changed topic or compression",
        )
        .into());
    }
    let nonempty = expected
        .iter()
        .filter(|partition| partition.message_count != 0)
        .count();
    if output.partitions.len() != nonempty {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "output manifest partition count mismatch",
        )
        .into());
    }
    for partition in expected {
        let actual = output
            .partitions
            .iter()
            .find(|entry| entry.partition == partition.partition as i32);
        if partition.message_count == 0 {
            if actual.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "empty destination partition unexpectedly exists",
                )
                .into());
            }
        } else if actual.is_none_or(|entry| {
            entry.message_count != partition.message_count
                || entry.total_uncompressed_payload_bytes != partition.payload_bytes
        }) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "output manifest metadata mismatch for partition {}",
                    partition.partition
                ),
            )
            .into());
        }
    }
    Ok(())
}

fn sha256_file(path: &Path) -> Result<String, DynError> {
    let mut file = File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0u8; 1024 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hex_digest(hasher.finalize()))
}

fn capture_tree_sha256(path: &Path) -> Result<String, DynError> {
    let mut files = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if entry.file_type()?.is_file() {
            files.push(entry.file_name());
        }
    }
    files.sort();
    let mut hasher = Sha256::new();
    hasher.update(b"chronoxide-capture-tree-v1\0");
    for name in files {
        let name_bytes = name.to_string_lossy();
        update_bytes(&mut hasher, name_bytes.as_bytes());
        let file_path = path.join(&name);
        update_u64(&mut hasher, fs::metadata(&file_path)?.len());
        update_bytes(&mut hasher, sha256_file(&file_path)?.as_bytes());
    }
    Ok(hex_digest(hasher.finalize()))
}

fn write_report(path: &Path, report: &RepartitionReport) -> Result<(), DynError> {
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = BufWriter::new(file);
    serde_json::to_writer_pretty(&mut writer, report)?;
    writer.write_all(b"\n")?;
    writer.flush()?;
    Ok(())
}

fn main() -> Result<(), DynError> {
    let report = run(Args::parse())?;
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn source_capture(root: &Path, messages: u64) -> PathBuf {
        let path = root.join("source");
        let mut writer =
            OtlpCaptureWriter::create(&path, "metrics-topic", CompressionMethod::Uncompressed)
                .unwrap();
        for ordinal in 0..messages {
            let partition = (ordinal % 3) as i32;
            writer
                .append(
                    partition,
                    10_000 + ordinal as i64,
                    -500 + ordinal as i64,
                    20_000 + ordinal as i64,
                    format!(
                        "payload-{ordinal:03}-{}",
                        "x".repeat((ordinal % 7) as usize)
                    )
                    .as_bytes(),
                )
                .unwrap();
        }
        writer.close().unwrap();
        path
    }

    fn zstd_single_partition_source_capture(root: &Path, messages: u64) -> PathBuf {
        let path = root.join("zstd-single-partition-source");
        let mut writer =
            OtlpCaptureWriter::create(&path, "metrics-topic", CompressionMethod::Zstd).unwrap();
        for ordinal in 0..messages {
            writer
                .append(
                    7,
                    40_000 + ordinal as i64,
                    -900 + ordinal as i64,
                    70_000 + ordinal as i64,
                    format!("zstd-payload-{ordinal:03}-{}", "z".repeat(128)).as_bytes(),
                )
                .unwrap();
        }
        writer.close().unwrap();
        path
    }

    fn execute(
        temp: &TempDir,
        input: &Path,
        name: &str,
        layout: PartitionLayout,
        messages: u64,
    ) -> RepartitionReport {
        run(Args {
            input: input.to_path_buf(),
            output: temp.path().join(format!("{name}-capture")),
            report: temp.path().join(format!("{name}-report.json")),
            layout,
            partitions: 16,
            max_messages: Some(messages),
        })
        .unwrap()
    }

    #[test]
    fn mapping_is_exact_for_uniform_and_eighty_twenty_layouts() {
        let uniform: Vec<_> = (0..20)
            .map(|ordinal| PartitionLayout::Uniform.partition(ordinal, 16))
            .collect();
        assert_eq!(
            uniform,
            vec![
                0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 0, 1, 2, 3
            ]
        );
        let skewed: Vec<_> = (0..25)
            .map(|ordinal| PartitionLayout::Skew80_20.partition(ordinal, 16))
            .collect();
        assert_eq!(
            skewed,
            vec![
                0, 0, 0, 0, 1, 0, 0, 0, 0, 2, 0, 0, 0, 0, 3, 0, 0, 0, 0, 4, 0, 0, 0, 0, 5
            ]
        );
    }

    #[test]
    fn repartition_preserves_logical_records_and_has_deterministic_file_bytes() {
        let temp = tempfile::tempdir().unwrap();
        let source = source_capture(temp.path(), 80);
        let source_tree_before = capture_tree_sha256(&source).unwrap();
        for layout in [PartitionLayout::Uniform, PartitionLayout::Skew80_20] {
            let first = execute(&temp, &source, &format!("{layout:?}-a"), layout, 80);
            let second = execute(&temp, &source, &format!("{layout:?}-b"), layout, 80);
            assert!(first.reopened_verification);
            assert_eq!(first.messages, 80);
            assert_eq!(first.input_stream_sha256, second.input_stream_sha256);
            assert_eq!(first.output_stream_sha256, second.output_stream_sha256);
            assert_eq!(
                first.input_content_stream_sha256,
                first.output_content_stream_sha256
            );
            assert!(first.content_streams_equal);
            assert_eq!(
                first.input_content_stream_sha256,
                second.input_content_stream_sha256
            );
            assert_eq!(
                first.output_content_stream_sha256,
                second.output_content_stream_sha256
            );
            assert_eq!(first.mapping_sha256, second.mapping_sha256);
            assert_eq!(first.output_tree_sha256, second.output_tree_sha256);
            assert_eq!(first.output_manifest_sha256, second.output_manifest_sha256);
            match layout {
                PartitionLayout::Uniform => {
                    assert!(
                        first
                            .partitions
                            .iter()
                            .all(|entry| entry.message_count == 5)
                    );
                }
                PartitionLayout::Skew80_20 => {
                    assert_eq!(first.partitions[0].message_count, 64);
                    assert_eq!(first.partitions[1].message_count, 2);
                    assert!(
                        first.partitions[2..]
                            .iter()
                            .all(|entry| entry.message_count == 1)
                    );
                }
            }
        }
        assert_eq!(capture_tree_sha256(&source).unwrap(), source_tree_before);
    }

    #[test]
    fn zstd_single_partition_input_has_deterministic_canonical_content_proof() {
        let temp = tempfile::tempdir().unwrap();
        let source = zstd_single_partition_source_capture(temp.path(), 32);
        let source_tree_before = capture_tree_sha256(&source).unwrap();
        let source_manifest = read_manifest(&source).unwrap();
        assert_eq!(source_manifest.compression, CompressionMethod::Zstd);
        assert_eq!(source_manifest.partitions.len(), 1);

        let first = execute(&temp, &source, "zstd-a", PartitionLayout::Uniform, 32);
        let second = execute(&temp, &source, "zstd-b", PartitionLayout::Uniform, 32);
        assert!(first.reopened_verification);
        assert!(first.content_streams_equal);
        assert_eq!(
            first.input_content_stream_sha256,
            first.output_content_stream_sha256
        );
        assert_eq!(
            first.input_content_stream_sha256,
            second.input_content_stream_sha256
        );
        assert_eq!(
            first.output_content_stream_sha256,
            second.output_content_stream_sha256
        );
        assert_ne!(first.input_stream_sha256, first.output_stream_sha256);
        assert_eq!(first.output_tree_sha256, second.output_tree_sha256);
        assert_eq!(capture_tree_sha256(&source).unwrap(), source_tree_before);
    }

    #[test]
    fn refuses_zero_prefix_and_any_existing_output_or_report() {
        let temp = tempfile::tempdir().unwrap();
        let source = source_capture(temp.path(), 1);
        let output = temp.path().join("output");
        let error = run(Args {
            input: source.clone(),
            output: output.clone(),
            report: temp.path().join("report.json"),
            layout: PartitionLayout::Uniform,
            partitions: 16,
            max_messages: Some(0),
        })
        .err()
        .unwrap();
        assert!(error.to_string().contains("max_messages"));

        fs::create_dir(&output).unwrap();
        let error = run(Args {
            input: source,
            output: output.clone(),
            report: output.join("report.json"),
            layout: PartitionLayout::Uniform,
            partitions: 16,
            max_messages: Some(1),
        })
        .err()
        .unwrap();
        assert!(error.to_string().contains("output already exists"));

        let second_output = temp.path().join("second-output");
        let existing_report = temp.path().join("existing-report.json");
        fs::write(&existing_report, b"owned").unwrap();
        let error = run(Args {
            input: temp.path().join("source"),
            output: second_output.clone(),
            report: existing_report,
            layout: PartitionLayout::Uniform,
            partitions: 16,
            max_messages: Some(1),
        })
        .err()
        .unwrap();
        assert!(error.to_string().contains("report already exists"));
        assert!(!second_output.exists());
    }
}
