use std::fmt;
use std::fs::File;
use std::io;

use sha2::{Digest, Sha256};

use super::{
    SEGMENT_FOOTER_TRACKED_FILES, SEGMENT_SCHEMA_VERSION_V6, SEGMENT_SCHEMA_VERSION_V7,
    SEGMENT_SCHEMA_VERSION_V8, SegmentChunkKindStats, SegmentChunkSummary, SegmentFooter,
    SegmentId, SegmentMeta, SegmentStoreReader, SegmentStoreSchemaPolicy,
    read_segment_footer_for_exact_schema,
};

const SEGMENT_CORPUS_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide/segment-corpus-fingerprint";

pub const SEGMENT_CORPUS_FINGERPRINT_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash)]
pub struct SegmentCorpusFingerprint([u8; 32]);

impl SegmentCorpusFingerprint {
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    pub fn to_hex(self) -> String {
        let mut output = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            write!(&mut output, "{byte:02x}").expect("writing to String cannot fail");
        }
        output
    }
}

impl fmt::Display for SegmentCorpusFingerprint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(formatter, "{byte:02x}")?;
        }
        Ok(())
    }
}

impl SegmentStoreReader {
    pub fn corpus_fingerprint_sha256(&self) -> io::Result<SegmentCorpusFingerprint> {
        let selected_segments = self
            .segments
            .iter()
            .map(|segment| {
                Ok(CorpusFingerprintSegment {
                    directory_id: segment_directory_id(&segment.dir)?,
                    dir: &segment.dir,
                    meta: segment.meta(),
                    storage_schema_policy: segment.storage_schema_policy,
                })
            })
            .collect::<io::Result<Vec<_>>>()?;
        corpus_fingerprint_sha256(selected_segments)
    }
}

#[derive(Clone, Copy)]
struct CorpusFingerprintSegment<'a> {
    directory_id: SegmentId,
    dir: &'a std::path::Path,
    meta: &'a SegmentMeta,
    storage_schema_policy: SegmentStoreSchemaPolicy,
}

fn corpus_fingerprint_sha256(
    mut selected_segments: Vec<CorpusFingerprintSegment<'_>>,
) -> io::Result<SegmentCorpusFingerprint> {
    selected_segments.sort_by(|left, right| {
        left.directory_id
            .start_ms()
            .cmp(&right.directory_id.start_ms())
            .then_with(|| left.directory_id.end_ms().cmp(&right.directory_id.end_ms()))
            .then_with(|| left.directory_id.ulid().cmp(&right.directory_id.ulid()))
    });

    let mut digest = Sha256::new();
    digest.update(SEGMENT_CORPUS_FINGERPRINT_DOMAIN);
    digest.update(SEGMENT_CORPUS_FINGERPRINT_VERSION.to_le_bytes());
    update_count(
        &mut digest,
        selected_segments.len(),
        "selected segment count",
    )?;

    for segment in selected_segments {
        update_segment_directory_id(&mut digest, segment.directory_id);
        update_segment_meta(&mut digest, segment.meta);
        update_segment_footer(&mut digest, segment)?;
    }

    Ok(SegmentCorpusFingerprint(digest.finalize().into()))
}

fn segment_directory_id(dir: &std::path::Path) -> io::Result<SegmentId> {
    let directory_name = dir
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| invalid_data("segment directory name is not valid UTF-8"))?;
    SegmentId::parse_dir_name(directory_name)
        .map_err(|error| invalid_data(format!("invalid segment directory id: {error}")))
}

fn update_segment_directory_id(digest: &mut Sha256, directory_id: SegmentId) {
    update_u64(digest, directory_id.start_ms());
    update_u64(digest, directory_id.end_ms());
    digest.update(directory_id.ulid().to_bytes());
}

fn update_segment_meta(digest: &mut Sha256, meta: &SegmentMeta) {
    update_bytes(digest, meta.segment_id.as_bytes());
    update_u64(digest, meta.start_ms);
    update_u64(digest, meta.end_ms);
    update_u64(digest, meta.datapoints);
    update_u64(digest, meta.series);
    match meta.chunk_summary {
        None => digest.update([0]),
        Some(summary) => {
            digest.update([1]);
            update_chunk_summary(digest, summary);
        }
    }
}

fn update_chunk_summary(digest: &mut Sha256, summary: SegmentChunkSummary) {
    update_u64(digest, summary.chunks);
    update_u64(digest, summary.chunk_bytes);
    update_chunk_kind_stats(digest, summary.by_kind.float);
    update_chunk_kind_stats(digest, summary.by_kind.int64);
    update_chunk_kind_stats(digest, summary.by_kind.histogram);
    update_chunk_kind_stats(digest, summary.by_kind.exponential_histogram);
    update_chunk_kind_stats(digest, summary.by_kind.summary);
}

fn update_chunk_kind_stats(digest: &mut Sha256, stats: SegmentChunkKindStats) {
    update_u64(digest, stats.chunks);
    update_u64(digest, stats.chunk_bytes);
}

fn update_segment_footer(
    digest: &mut Sha256,
    segment: CorpusFingerprintSegment<'_>,
) -> io::Result<()> {
    let mut footer = read_segment_footer_bounded(segment)?;
    validate_footer_inventory(&footer)?;
    footer
        .files
        .sort_by(|left, right| left.file.filename().cmp(right.file.filename()));

    digest.update(footer.schema_version.to_le_bytes());
    update_count(
        digest,
        footer.files.len(),
        "segment footer tracked-file count",
    )?;
    for entry in footer.files {
        let path = segment.dir.join(entry.file.filename());
        let file = File::open(&path)?;
        let actual = file.metadata()?;
        if !actual.is_file() {
            return Err(invalid_data(format!(
                "segment footer tracked path is not a file: {}",
                path.display()
            )));
        }
        if actual.len() != entry.size {
            return Err(invalid_data(format!(
                "segment footer file length mismatch for {}: stored={} actual={}",
                entry.file.filename(),
                entry.size,
                actual.len()
            )));
        }

        update_bytes(digest, entry.file.filename().as_bytes());
        update_u64(digest, entry.size);
        update_u64(digest, entry.checksum_xxh64);
    }
    Ok(())
}

fn read_segment_footer_bounded(segment: CorpusFingerprintSegment<'_>) -> io::Result<SegmentFooter> {
    let expected_schema = match segment.storage_schema_policy {
        SegmentStoreSchemaPolicy::StrictSchema7 => SEGMENT_SCHEMA_VERSION_V7,
        SegmentStoreSchemaPolicy::StrictSchema8 => SEGMENT_SCHEMA_VERSION_V8,
        SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb => SEGMENT_SCHEMA_VERSION_V6,
    };
    read_segment_footer_for_exact_schema(segment.dir, expected_schema)
}

fn validate_footer_inventory(footer: &SegmentFooter) -> io::Result<()> {
    let mut seen = Vec::with_capacity(footer.files.len());
    for entry in &footer.files {
        if seen.contains(&entry.file) {
            return Err(invalid_data(format!(
                "duplicate segment footer file entry: {}",
                entry.file.filename()
            )));
        }
        seen.push(entry.file);
    }

    for required in SEGMENT_FOOTER_TRACKED_FILES {
        if !seen.contains(&required) {
            return Err(invalid_data(format!(
                "segment footer missing tracked file: {}",
                required.filename()
            )));
        }
    }
    if seen.len() != SEGMENT_FOOTER_TRACKED_FILES.len() {
        return Err(invalid_data(
            "segment footer contains an unknown tracked-file inventory",
        ));
    }
    Ok(())
}

fn update_count(digest: &mut Sha256, count: usize, field: &'static str) -> io::Result<()> {
    let count = u64::try_from(count)
        .map_err(|_| invalid_data(format!("{field} exceeds the u64 fingerprint encoding")))?;
    update_u64(digest, count);
    Ok(())
}

fn update_bytes(digest: &mut Sha256, bytes: &[u8]) {
    update_u64(digest, bytes.len() as u64);
    digest.update(bytes);
}

fn update_u64(digest: &mut Sha256, value: u64) {
    digest.update(value.to_le_bytes());
}

fn invalid_data(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

#[cfg(test)]
mod tests {
    use std::fs::{self, OpenOptions};
    use std::io::{self, Write};
    use std::path::{Path, PathBuf};
    use std::time::Duration;

    use tempfile::TempDir;
    use ulid::Ulid;

    use super::super::*;

    fn write_fixture_corpus(root: &Path, starts_in_creation_order: &[u64]) {
        for &start_ms in starts_in_creation_order {
            let config = SegmentWriterConfig::new(root, Duration::from_secs(1))
                .with_deterministic_segment_ids(start_ms);
            let mut writer = SegmentWriter::new(config).unwrap();
            writer
                .record_samples_ordered_with_label_visitor(
                    SeriesRef::new(1),
                    &[(start_ms, start_ms as f64)],
                    |visit| {
                        visit(METRIC_NAME_LABEL, "corpus_fixture");
                        visit("site", "sg");
                    },
                )
                .unwrap();
            writer.flush().unwrap();
        }
    }

    fn fixture_store(starts_in_creation_order: &[u64]) -> (TempDir, SegmentStoreReader) {
        let root = tempfile::tempdir().unwrap();
        write_fixture_corpus(root.path(), starts_in_creation_order);
        let store = SegmentStoreReader::open(root.path()).unwrap();
        (root, store)
    }

    fn segment_dirs(root: &Path) -> Vec<PathBuf> {
        let mut dirs = fs::read_dir(root)
            .unwrap()
            .map(|entry| entry.unwrap())
            .filter(|entry| entry.file_type().unwrap().is_dir())
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .map(|entry| entry.path())
            .collect::<Vec<_>>();
        dirs.sort();
        dirs
    }

    fn only_segment_dir(root: &Path) -> PathBuf {
        let dirs = segment_dirs(root);
        assert_eq!(dirs.len(), 1);
        dirs.into_iter().next().unwrap()
    }

    fn rewrite_footer(segment_dir: &Path, mutate: impl FnOnce(&mut SegmentFooter)) {
        let mut footer = read_segment_footer_for_schema8(segment_dir).unwrap();
        mutate(&mut footer);
        fs::write(
            segment_dir.join(SegmentFile::Footer.filename()),
            encode_segment_footer(&footer).unwrap(),
        )
        .unwrap();
    }

    fn footer_file_mut(footer: &mut SegmentFooter, file: SegmentFile) -> &mut SegmentFooterFile {
        footer
            .files
            .iter_mut()
            .find(|entry| entry.file == file)
            .unwrap()
    }

    fn assert_fingerprint_error(root: &Path) -> io::Error {
        let segments = segment_dirs(root)
            .into_iter()
            .map(|dir| {
                let meta = serde_json::from_slice(
                    &fs::read(dir.join(SegmentFile::MetaJson.filename())).unwrap(),
                )
                .unwrap();
                (dir, meta)
            })
            .collect::<Vec<_>>();
        let selected = segments
            .iter()
            .map(|(dir, meta)| {
                Ok(super::CorpusFingerprintSegment {
                    directory_id: super::segment_directory_id(dir)?,
                    dir,
                    meta,
                    storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                })
            })
            .collect::<io::Result<Vec<_>>>();
        match selected.and_then(super::corpus_fingerprint_sha256) {
            Ok(_) => panic!("corpus fingerprint unexpectedly succeeded"),
            Err(error) => error,
        }
    }

    #[test]
    fn segment_corpus_fingerprint_has_stable_versioned_digest() {
        let (_root, store) = fixture_store(&[1_000, 2_000]);

        let fingerprint = store.corpus_fingerprint_sha256().unwrap();

        assert_eq!(SEGMENT_CORPUS_FINGERPRINT_VERSION, 1);
        assert_eq!(
            fingerprint.to_hex(),
            "ea48f980250bc069d46466dd000fe3a3feb3c5a0624e7d1822eb59660343ecac"
        );
        assert_eq!(format!("{fingerprint}"), fingerprint.to_hex());
        assert_eq!(fingerprint.as_bytes().len(), 32);
    }

    #[test]
    fn segment_corpus_fingerprint_is_independent_of_directory_enumeration() {
        let (_left_root, left) = fixture_store(&[2_000, 1_000]);
        let (_right_root, right) = fixture_store(&[1_000, 2_000]);

        assert_eq!(
            left.corpus_fingerprint_sha256().unwrap(),
            right.corpus_fingerprint_sha256().unwrap()
        );
    }

    #[test]
    fn segment_corpus_fingerprint_sorts_directory_ids_after_reader_order_changes() {
        let (left_root, left_store) = fixture_store(&[1_000, 2_000]);
        drop(left_store);
        let left = SegmentStoreReader::open(left_root.path()).unwrap();

        let (right_root, right_store) = fixture_store(&[2_000, 1_000]);
        drop(right_store);
        let mut right = SegmentStoreReader::open(right_root.path()).unwrap();
        right.segments.reverse();

        assert_eq!(
            left.corpus_fingerprint_sha256().unwrap(),
            right.corpus_fingerprint_sha256().unwrap()
        );
    }

    #[test]
    fn segment_corpus_fingerprint_hashes_canonical_segment_metadata() {
        let (root, baseline_store) = fixture_store(&[1_000]);
        let baseline = baseline_store.corpus_fingerprint_sha256().unwrap();
        drop(baseline_store);
        let segment_dir = only_segment_dir(root.path());
        let meta_path = segment_dir.join(SegmentFile::MetaJson.filename());
        let mut meta: SegmentMeta = serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
        meta.datapoints = 2;
        let changed = serde_json::to_vec_pretty(&meta).unwrap();
        assert_eq!(
            changed.len() as u64,
            fs::metadata(&meta_path).unwrap().len()
        );
        fs::write(meta_path, changed).unwrap();

        let changed = SegmentStoreReader::open(root.path())
            .unwrap()
            .corpus_fingerprint_sha256()
            .unwrap();

        assert_ne!(baseline, changed);
    }

    #[test]
    fn segment_store_rejects_directory_meta_identity_mismatch_before_fingerprinting() {
        let (renamed_root, renamed_store) = fixture_store(&[1_000]);
        drop(renamed_store);
        let original_dir = only_segment_dir(renamed_root.path());
        let original_name = original_dir.file_name().unwrap().to_str().unwrap();
        let original_id = SegmentId::parse_dir_name(original_name).unwrap();
        let renamed_id = SegmentId::with_ulid(
            original_id.start_ms(),
            original_id.end_ms(),
            Ulid::from(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef_u128),
        )
        .unwrap();
        assert_ne!(original_id, renamed_id);
        fs::rename(
            &original_dir,
            renamed_root.path().join(renamed_id.dir_name()),
        )
        .unwrap();
        let renamed_error = SegmentStoreReader::open(renamed_root.path())
            .err()
            .expect("renamed directory must not disagree with meta.json");
        assert_eq!(renamed_error.kind(), io::ErrorKind::InvalidData);
        assert!(renamed_error.to_string().contains("directory identity"));

        let (meta_root, meta_store) = fixture_store(&[1_000]);
        drop(meta_store);
        let segment_dir = only_segment_dir(meta_root.path());
        let meta_path = segment_dir.join(SegmentFile::MetaJson.filename());
        let mut meta: SegmentMeta = serde_json::from_slice(&fs::read(&meta_path).unwrap()).unwrap();
        meta.segment_id = renamed_id.dir_name();
        let changed = serde_json::to_vec_pretty(&meta).unwrap();
        assert_eq!(
            changed.len() as u64,
            fs::metadata(&meta_path).unwrap().len()
        );
        fs::write(meta_path, changed).unwrap();
        let mismatched_meta_error = SegmentStoreReader::open(meta_root.path())
            .err()
            .expect("meta.json must not disagree with its directory");
        assert_eq!(mismatched_meta_error.kind(), io::ErrorKind::InvalidData);
        assert!(
            mismatched_meta_error
                .to_string()
                .contains("directory identity")
        );
    }

    #[test]
    fn segment_corpus_fingerprint_hashes_footer_checksum_metadata() {
        let (root, baseline_store) = fixture_store(&[1_000]);
        let baseline = baseline_store.corpus_fingerprint_sha256().unwrap();
        drop(baseline_store);
        rewrite_footer(&only_segment_dir(root.path()), |footer| {
            footer_file_mut(footer, SegmentFile::Chunks).checksum_xxh64 ^= 1;
        });

        let changed = SegmentStoreReader::open(root.path())
            .unwrap()
            .corpus_fingerprint_sha256()
            .unwrap();

        assert_ne!(baseline, changed);
    }

    #[test]
    fn segment_corpus_fingerprint_rejects_noncanonical_footer_entry_order() {
        let (root, store) = fixture_store(&[1_000]);
        drop(store);
        rewrite_footer(&only_segment_dir(root.path()), |footer| {
            footer.files.reverse();
        });

        assert_eq!(
            assert_fingerprint_error(root.path()).kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn segment_corpus_fingerprint_rejects_stored_footer_length_mismatch() {
        let (root, store) = fixture_store(&[1_000]);
        drop(store);
        rewrite_footer(&only_segment_dir(root.path()), |footer| {
            footer_file_mut(footer, SegmentFile::Chunks).size += 1;
        });

        assert_eq!(
            assert_fingerprint_error(root.path()).kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn segment_corpus_fingerprint_rejects_noncanonical_footer_file_length_before_decode() {
        let (root, store) = fixture_store(&[1_000]);
        drop(store);
        let footer_path = only_segment_dir(root.path()).join(SegmentFile::Footer.filename());
        OpenOptions::new()
            .append(true)
            .open(footer_path)
            .unwrap()
            .write_all(&[0])
            .unwrap();

        let error = assert_fingerprint_error(root.path());

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "segment footer length is not canonical");
    }

    #[test]
    fn segment_corpus_fingerprint_rejects_non_regular_footer_before_open() {
        let (root, store) = fixture_store(&[1_000]);
        drop(store);
        let footer_path = only_segment_dir(root.path()).join(SegmentFile::Footer.filename());
        fs::remove_file(&footer_path).unwrap();
        fs::create_dir(&footer_path).unwrap();

        let error = assert_fingerprint_error(root.path());

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(error.to_string(), "segment footer is not a regular file");
    }

    #[test]
    fn segment_corpus_fingerprint_rejects_actual_truncation_extension_and_missing_files() {
        let (truncated_root, store) = fixture_store(&[1_000]);
        drop(store);
        let chunks = only_segment_dir(truncated_root.path()).join(SegmentFile::Chunks.filename());
        let original_len = fs::metadata(&chunks).unwrap().len();
        assert!(original_len > 0);
        OpenOptions::new()
            .write(true)
            .open(chunks)
            .unwrap()
            .set_len(original_len - 1)
            .unwrap();
        assert_eq!(
            assert_fingerprint_error(truncated_root.path()).kind(),
            io::ErrorKind::InvalidData
        );

        let (extended_root, store) = fixture_store(&[1_000]);
        drop(store);
        let ooo = only_segment_dir(extended_root.path()).join(SegmentFile::OooChunks.filename());
        OpenOptions::new()
            .append(true)
            .open(ooo)
            .unwrap()
            .write_all(&[1])
            .unwrap();
        assert_eq!(
            assert_fingerprint_error(extended_root.path()).kind(),
            io::ErrorKind::InvalidData
        );

        let (missing_root, store) = fixture_store(&[1_000]);
        drop(store);
        fs::remove_file(
            only_segment_dir(missing_root.path()).join(SegmentFile::Symbols.filename()),
        )
        .unwrap();
        assert_eq!(
            assert_fingerprint_error(missing_root.path()).kind(),
            io::ErrorKind::NotFound
        );
    }

    #[test]
    fn segment_corpus_fingerprint_rejects_footer_filename_inventory_changes() {
        let (missing_root, store) = fixture_store(&[1_000]);
        drop(store);
        rewrite_footer(&only_segment_dir(missing_root.path()), |footer| {
            footer
                .files
                .retain(|entry| entry.file != SegmentFile::Symbols);
        });
        assert_eq!(
            assert_fingerprint_error(missing_root.path()).kind(),
            io::ErrorKind::InvalidData
        );

        let (duplicate_root, store) = fixture_store(&[1_000]);
        drop(store);
        rewrite_footer(&only_segment_dir(duplicate_root.path()), |footer| {
            let duplicate = footer
                .files
                .iter()
                .find(|entry| entry.file == SegmentFile::Chunks)
                .unwrap()
                .clone();
            footer.files.push(duplicate);
        });
        assert_eq!(
            assert_fingerprint_error(duplicate_root.path()).kind(),
            io::ErrorKind::InvalidData
        );

        let (same_count_root, store) = fixture_store(&[1_000]);
        drop(store);
        rewrite_footer(&only_segment_dir(same_count_root.path()), |footer| {
            let duplicate = footer
                .files
                .iter()
                .find(|entry| entry.file == SegmentFile::Chunks)
                .unwrap()
                .clone();
            let replaced = footer
                .files
                .iter_mut()
                .find(|entry| entry.file == SegmentFile::Symbols)
                .unwrap();
            *replaced = duplicate;
        });
        let error = assert_fingerprint_error(same_count_root.path());
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            error.to_string(),
            "segment footer tracked-file inventory is not canonical"
        );
    }

    #[test]
    fn segment_corpus_fingerprint_hashes_selected_segment_inventory() {
        let (_one_root, one) = fixture_store(&[1_000]);
        let (_two_root, two) = fixture_store(&[1_000, 2_000]);

        assert_ne!(
            one.corpus_fingerprint_sha256().unwrap(),
            two.corpus_fingerprint_sha256().unwrap()
        );
    }
}
