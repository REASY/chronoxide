//! Short, layout-neutral replay/readback gate for storage-schema experiments.

use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::time::Instant;

use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::storage::chunk::{
    ChunkKind, ChunkSamples, Schema7ChunkPrefixExpectation, decode_chunk_record,
    verify_schema7_indexed_prefix,
};
use crate::storage::index::SegmentIndexReadAt;

use super::full_validation::{
    RegisteredSegmentValidationPolicy, preflight_registered_segment,
    registered_validation_error_to_io,
};
use super::metadata_facade::{
    Schema7MetadataOpenContext, SegmentChunkAuthentication, SegmentMetadataLayout,
    SegmentMetadataReader, SegmentMetadataVisitControl, SegmentMetadataVisitError,
};
use super::*;

const VERIFIED_SELECTION_FINGERPRINT_DOMAIN: &[u8] = b"chronoxide-verified-storage-selection-v1\0";
const VERIFIED_EXACT_POSTINGS_FINGERPRINT_DOMAIN: &[u8] =
    b"chronoxide-verified-exact-postings-v1\0";

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalExactPostingsVerification {
    pub logical_fingerprint: String,
    pub lists: u64,
    pub decoded_refs: u64,
    pub encoded_bytes: u64,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ExperimentalStorageVerification {
    pub schema_version: u16,
    pub footer_validation_enabled: bool,
    pub series_sample_per_segment: Option<u32>,
    pub verified_selection_fingerprint: String,
    pub segments: u64,
    pub corpus_series: u64,
    pub series: u64,
    pub chunks: u64,
    pub chunks_by_kind: [u64; 5],
    pub samples: u64,
    pub logical_chunk_bytes: u64,
    pub exact_postings: Option<ExperimentalExactPostingsVerification>,
    /// Total wall time. When footer validation is enabled this includes its
    /// registered full-file reads, also attributed to the metadata-runtime
    /// counters below.
    pub elapsed_ns: u64,
    pub metadata_read_calls: u64,
    pub metadata_read_bytes: u64,
    pub metadata_peak_retained_bytes: u64,
    pub metadata_peak_in_flight_bytes: u64,
    pub metadata_peak_open_files: u32,
    pub metadata_cache_hits: u64,
    pub metadata_cache_misses: u64,
}

struct ExactPostingsAccumulator {
    hasher: Sha256,
    lists: u64,
    decoded_refs: u64,
    encoded_bytes: u64,
    scratch: Vec<u8>,
}

impl ExactPostingsAccumulator {
    fn new(segment_count: u32) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(VERIFIED_EXACT_POSTINGS_FINGERPRINT_DOMAIN);
        hash_u32(&mut hasher, segment_count);
        Self {
            hasher,
            lists: 0,
            decoded_refs: 0,
            encoded_bytes: 0,
            scratch: Vec::with_capacity(64 * 1024),
        }
    }

    fn start_segment(&mut self, segment_id: &str) -> io::Result<()> {
        hash_bytes(&mut self.hasher, segment_id.as_bytes())
    }

    fn observe(
        &mut self,
        name_sym: u32,
        value_sym: u32,
        ref_count: u32,
        encoded_bytes: u64,
        refs: &[u32],
    ) -> io::Result<()> {
        if refs.len() != ref_count as usize {
            return Err(invalid_segment_data(
                "decoded exact-postings count disagrees with its protected record",
            ));
        }
        self.lists = self
            .lists
            .checked_add(1)
            .ok_or_else(|| invalid_segment_data("exact-postings list count overflows"))?;
        self.decoded_refs = self
            .decoded_refs
            .checked_add(u64::from(ref_count))
            .ok_or_else(|| invalid_segment_data("exact-postings ref count overflows"))?;
        self.encoded_bytes = self
            .encoded_bytes
            .checked_add(encoded_bytes)
            .ok_or_else(|| invalid_segment_data("exact-postings encoded bytes overflow"))?;

        hash_u32(&mut self.hasher, name_sym);
        hash_u32(&mut self.hasher, value_sym);
        hash_u32(&mut self.hasher, ref_count);
        for chunk in refs.chunks(16 * 1024) {
            self.scratch.clear();
            for series_ref in chunk {
                self.scratch.extend_from_slice(&series_ref.to_le_bytes());
            }
            self.hasher.update(&self.scratch);
        }
        Ok(())
    }

    fn finish(self) -> ExperimentalExactPostingsVerification {
        ExperimentalExactPostingsVerification {
            logical_fingerprint: hex_digest(self.hasher.finalize().into()),
            lists: self.lists,
            decoded_refs: self.decoded_refs,
            encoded_bytes: self.encoded_bytes,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CanonicalChunk {
    file_id: u8,
    kind: u8,
    flags: u16,
    min_time_ms: u64,
    max_time_ms: u64,
    file_offset: u64,
    length: u32,
    scalar_lane_offset: u32,
    scalar_lane_len: u32,
    digest: [u8; 32],
}

/// Walks one homogeneous schema-6, schema-7, or schema-8 corpus and verifies each selected
/// series identity/label route and decodes each of its indexed chunks. A
/// missing sample limit selects the complete corpus; a limit selects stable,
/// evenly spaced refs in every segment for a short real-corpus A/B gate.
pub fn verify_experimental_storage_corpus(
    segments_dir: impl AsRef<Path>,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
    series_sample_per_segment: Option<u32>,
) -> io::Result<ExperimentalStorageVerification> {
    verify_experimental_storage_corpus_impl(
        segments_dir.as_ref(),
        schema,
        validate_segment_footers,
        series_sample_per_segment,
        false,
    )
}

pub fn verify_experimental_storage_corpus_with_exact_postings(
    segments_dir: impl AsRef<Path>,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
    series_sample_per_segment: Option<u32>,
) -> io::Result<ExperimentalStorageVerification> {
    verify_experimental_storage_corpus_impl(
        segments_dir.as_ref(),
        schema,
        validate_segment_footers,
        series_sample_per_segment,
        true,
    )
}

fn verify_experimental_storage_corpus_impl(
    segments_dir: &Path,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
    series_sample_per_segment: Option<u32>,
    verify_exact_postings: bool,
) -> io::Result<ExperimentalStorageVerification> {
    let started = Instant::now();
    let inventory = read_manifest_inventory(segments_dir.join("manifest"))?
        .ok_or_else(|| invalid_segment_data("segment manifest is missing"))?;
    let segment_count = u32::try_from(inventory.segments.len())
        .map_err(|_| invalid_segment_data("segment count exceeds u32"))?;
    let runtime = open_metadata_runtime(MetadataGovernorConfig::default())?;
    let mut hasher = Sha256::new();
    hasher.update(VERIFIED_SELECTION_FINGERPRINT_DOMAIN);
    match series_sample_per_segment {
        Some(limit) => {
            hasher.update([1]);
            hash_u32(&mut hasher, limit);
        }
        None => hasher.update([0]),
    }
    hash_u32(&mut hasher, segment_count);
    let mut exact_postings = if verify_exact_postings {
        if schema == SegmentStorageSchema::Schema6 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exhaustive integrity-checked exact-postings verification requires schema 7 or 8",
            ));
        }
        Some(ExactPostingsAccumulator::new(segment_count))
    } else {
        None
    };

    let mut total_series = 0u64;
    let mut corpus_series = 0u64;
    let mut total_chunks = 0u64;
    let mut chunks_by_kind = [0u64; 5];
    let mut total_samples = 0u64;
    let mut total_chunk_bytes = 0u64;

    for manifest_segment in &inventory.segments {
        let segment_dir = segments_dir.join(&manifest_segment.segment_id);
        let (registered, footer, meta) = if validate_segment_footers {
            let policy = match schema {
                SegmentStorageSchema::Schema6 => {
                    RegisteredSegmentValidationPolicy::ValidatedSchema6
                }
                SegmentStorageSchema::Schema7 => RegisteredSegmentValidationPolicy::Schema7,
                SegmentStorageSchema::Schema8 => RegisteredSegmentValidationPolicy::Schema8,
            };
            let preflight = preflight_registered_segment(&runtime, &segment_dir, policy)
                .map_err(registered_validation_error_to_io)?;
            preflight
                .validate_footer_checksums()
                .map_err(registered_validation_error_to_io)?
                .into_open_parts()
        } else {
            let footer = match schema {
                SegmentStorageSchema::Schema6 => read_segment_footer_for_schema6(&segment_dir)?,
                SegmentStorageSchema::Schema7 => read_segment_footer_for_schema7(&segment_dir)?,
                SegmentStorageSchema::Schema8 => read_segment_footer_for_schema8(&segment_dir)?,
            };
            let meta: SegmentMeta = serde_json::from_slice(&fs::read(
                segment_dir.join(SegmentFile::MetaJson.filename()),
            )?)
            .map_err(io::Error::other)?;
            let registered =
                super::query_reader::register_segment_metadata(&runtime, &segment_dir, &footer)?;
            (registered, footer, meta)
        };
        validate_manifest_segment_meta(manifest_segment, &meta)?;
        let series_count = u32::try_from(meta.series)
            .map_err(|_| invalid_segment_data("segment series count exceeds u32"))?;
        corpus_series = corpus_series.saturating_add(u64::from(series_count));
        let sampled_refs = series_sample_per_segment
            .filter(|limit| *limit < series_count)
            .map(|limit| evenly_spaced_series_refs(series_count, limit));
        let selected_series = sampled_refs
            .as_ref()
            .map_or(series_count, |refs| refs.len() as u32);
        let layout = match schema {
            SegmentStorageSchema::Schema6 => SegmentMetadataLayout::Schema6 { series_count },
            SegmentStorageSchema::Schema7 => {
                SegmentMetadataLayout::Schema7(Schema7MetadataOpenContext {
                    series_file_len: footer_file_len(&footer, SegmentFile::Series)?,
                    chunk_index_file_len: footer_file_len(&footer, SegmentFile::ChunkIndex)?,
                    segment_start_ms: meta.start_ms,
                    segment_end_ms: meta.end_ms,
                    series_count,
                })
            }
            SegmentStorageSchema::Schema8 => {
                SegmentMetadataLayout::Schema8(Schema7MetadataOpenContext {
                    series_file_len: footer_file_len(&footer, SegmentFile::Series)?,
                    chunk_index_file_len: footer_file_len(&footer, SegmentFile::ChunkIndex)?,
                    segment_start_ms: meta.start_ms,
                    segment_end_ms: meta.end_ms,
                    series_count,
                })
            }
        };
        let metadata = SegmentMetadataReader::open(&registered, layout).map_err(facade_io)?;
        let session = metadata.query_session().map_err(facade_io)?;
        let root = session.bind_roots().map_err(facade_io)?;
        if root.series_count() != series_count {
            return Err(invalid_segment_data(
                "metadata root series count disagrees with meta.json",
            ));
        }

        if let Some(exact_postings) = exact_postings.as_mut() {
            exact_postings.start_segment(&manifest_segment.segment_id)?;
            let mut visitor_error = None;
            let exhausted = session
                .visit_authenticated_exact_postings(
                    &root,
                    |name_sym, value_sym, ref_count, encoded_bytes, refs| match exact_postings
                        .observe(name_sym, value_sym, ref_count, encoded_bytes, refs)
                    {
                        Ok(()) => true,
                        Err(error) => {
                            visitor_error = Some(error);
                            false
                        }
                    },
                )
                .map_err(facade_io)?;
            if let Some(error) = visitor_error {
                return Err(error);
            }
            if !exhausted {
                return Err(invalid_segment_data(
                    "integrity-checked exact-postings verification stopped early",
                ));
            }
        }

        hash_bytes(&mut hasher, manifest_segment.segment_id.as_bytes())?;
        hash_u64(&mut hasher, manifest_segment.start_ms);
        hash_u64(&mut hasher, manifest_segment.end_ms);
        hash_u32(&mut hasher, series_count);
        hash_u32(&mut hasher, selected_series);

        let mut chunk_files = [
            File::open(segment_dir.join(SegmentFile::Chunks.filename()))?,
            File::open(segment_dir.join(SegmentFile::OooChunks.filename()))?,
        ];
        let mut chunk_buffer = Vec::new();
        const SERIES_BATCH: u32 = 409 * 16;
        let mut selected_offset = 0u32;
        while selected_offset < selected_series {
            let batch_end = selected_offset
                .saturating_add(SERIES_BATCH)
                .min(selected_series);
            let refs = if let Some(sampled_refs) = sampled_refs.as_ref() {
                sampled_refs[selected_offset as usize..batch_end as usize].to_vec()
            } else {
                (selected_offset..batch_end).collect::<Vec<_>>()
            };
            let candidates = session.series_ref_set(&root, &refs).map_err(facade_io)?;
            let mut batch_offset = 0usize;
            let visit = session.visit_verified_series(&root, &candidates, |series| {
                if refs.get(batch_offset).copied() != Some(series.series_ref()) {
                    return Err(invalid_segment_data(
                        "verified series refs do not match the ordered selection",
                    ));
                }
                batch_offset += 1;

                hash_u32(&mut hasher, series.series_ref());
                hash_u64(&mut hasher, series.series_id());
                hasher.update([series.kind_mask()]);
                hash_u32(
                    &mut hasher,
                    u32::try_from(series.labels().len())
                        .map_err(|_| invalid_segment_data("label count exceeds u32"))?,
                );
                let mut previous_label: Option<&[u8]> = None;
                for (name, value) in series.labels() {
                    if previous_label.is_some_and(|previous| previous >= name.as_bytes()) {
                        return Err(invalid_segment_data(
                            "verified series labels are not strictly ordered",
                        ));
                    }
                    hash_bytes(&mut hasher, name.as_bytes())?;
                    hash_bytes(&mut hasher, value.as_bytes())?;
                    previous_label = Some(name.as_bytes());
                }

                let mut canonical = Vec::new();
                canonical
                    .try_reserve_exact(series.chunks().len())
                    .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
                let mut observed_kind_mask = 0u8;
                series.chunks().visit(|locator| {
                    let file = chunk_files
                        .get_mut(usize::from(locator.file_id()))
                        .ok_or_else(|| invalid_segment_data("chunk locator file id is invalid"))?;
                    let length = usize::try_from(locator.chunk_len())
                        .map_err(|_| invalid_segment_data("chunk length exceeds usize"))?;
                    chunk_buffer.clear();
                    chunk_buffer
                        .try_reserve(length)
                        .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
                    chunk_buffer.resize(length, 0);
                    SegmentIndexReadAt::read_exact_at(
                        file,
                        locator.file_offset(),
                        &mut chunk_buffer,
                    )?;

                    let authenticated_flags = match locator.authentication() {
                        SegmentChunkAuthentication::Schema6Legacy => locator.flags(),
                        SegmentChunkAuthentication::Schema7IndexedPrefix { crc32c } => {
                            let prefix_len = locator.indexed_prefix_len();
                            let prefix = chunk_buffer.get(..prefix_len).ok_or_else(|| {
                                invalid_segment_data("schema-7/8 chunk prefix is short")
                            })?;
                            verify_schema7_indexed_prefix(
                                &Schema7ChunkPrefixExpectation {
                                    series_ref: locator.series_ref(),
                                    kind: locator.kind(),
                                    min_time_ms: locator.min_time_ms(),
                                    max_time_ms: locator.max_time_ms(),
                                    length: locator.chunk_len(),
                                    scalar_lane_offset: locator.scalar_lane_offset(),
                                    scalar_lane_len: locator.scalar_lane_len(),
                                    indexed_prefix_crc32c: crc32c,
                                },
                                prefix,
                            )?
                            .flags
                        }
                    };

                    let decoded = decode_chunk_record(&chunk_buffer)?;
                    if decoded.series_ref != locator.series_ref()
                        || decoded.kind != locator.kind()
                        || decoded.min_time_ms != locator.min_time_ms()
                        || decoded.max_time_ms != locator.max_time_ms()
                    {
                        return Err(invalid_segment_data(
                            "decoded chunk header disagrees with its metadata locator",
                        ));
                    }
                    if locator.min_time_ms() < manifest_segment.start_ms
                        || locator.max_time_ms() >= manifest_segment.end_ms
                    {
                        return Err(invalid_segment_data(
                            "chunk time range lies outside its segment",
                        ));
                    }
                    let kind = chunk_kind_id(locator.kind());
                    observed_kind_mask |= 1u8 << kind;
                    chunks_by_kind[usize::from(kind)] =
                        chunks_by_kind[usize::from(kind)].saturating_add(1);
                    total_samples =
                        total_samples.saturating_add(chunk_sample_count(&decoded.samples));
                    total_chunk_bytes =
                        total_chunk_bytes.saturating_add(u64::from(locator.chunk_len()));
                    canonical.push(CanonicalChunk {
                        file_id: locator.file_id(),
                        kind,
                        flags: authenticated_flags,
                        min_time_ms: locator.min_time_ms(),
                        max_time_ms: locator.max_time_ms(),
                        file_offset: locator.file_offset(),
                        length: locator.chunk_len(),
                        scalar_lane_offset: locator.scalar_lane_offset(),
                        scalar_lane_len: locator.scalar_lane_len(),
                        digest: Sha256::digest(&chunk_buffer).into(),
                    });
                    Ok(SegmentMetadataVisitControl::Continue)
                })?;
                if canonical.is_empty() {
                    return Err(invalid_segment_data("verified series has no chunks"));
                }
                if observed_kind_mask != series.kind_mask() {
                    return Err(invalid_segment_data(
                        "verified series kind mask disagrees with its chunks",
                    ));
                }
                canonical.sort_unstable_by_key(|chunk| {
                    (
                        chunk.file_id,
                        chunk.file_offset,
                        chunk.min_time_ms,
                        chunk.max_time_ms,
                        chunk.kind,
                        chunk.digest,
                    )
                });
                let chunk_count = u64::try_from(canonical.len())
                    .map_err(|_| invalid_segment_data("chunk count exceeds u64"))?;
                hash_u32(
                    &mut hasher,
                    u32::try_from(canonical.len())
                        .map_err(|_| invalid_segment_data("chunk count exceeds u32"))?,
                );
                for chunk in canonical {
                    hasher.update([chunk.file_id, chunk.kind]);
                    hasher.update(chunk.flags.to_le_bytes());
                    hash_u64(&mut hasher, chunk.min_time_ms);
                    hash_u64(&mut hasher, chunk.max_time_ms);
                    hash_u64(&mut hasher, chunk.file_offset);
                    hash_u32(&mut hasher, chunk.length);
                    hash_u32(&mut hasher, chunk.scalar_lane_offset);
                    hash_u32(&mut hasher, chunk.scalar_lane_len);
                    hasher.update(chunk.digest);
                }
                total_chunks = total_chunks.saturating_add(chunk_count);
                total_series = total_series.saturating_add(1);
                Ok(SegmentMetadataVisitControl::Continue)
            });
            match visit {
                Ok(_) => {}
                Err(SegmentMetadataVisitError::Metadata(error)) => return Err(facade_io(error)),
                Err(SegmentMetadataVisitError::Visitor(error)) => return Err(error),
            }
            if batch_offset != refs.len() {
                return Err(invalid_segment_data(
                    "verified series batch did not cover every requested ref",
                ));
            }
            selected_offset = batch_end;
        }
        if selected_offset != selected_series {
            return Err(invalid_segment_data(
                "verified series visit did not cover the complete selection",
            ));
        }
    }

    let snapshot = runtime.snapshot();
    Ok(ExperimentalStorageVerification {
        schema_version: schema.footer_version(),
        footer_validation_enabled: validate_segment_footers,
        series_sample_per_segment,
        verified_selection_fingerprint: hex_digest(hasher.finalize().into()),
        segments: u64::from(segment_count),
        corpus_series,
        series: total_series,
        chunks: total_chunks,
        chunks_by_kind,
        samples: total_samples,
        logical_chunk_bytes: total_chunk_bytes,
        exact_postings: exact_postings.map(ExactPostingsAccumulator::finish),
        elapsed_ns: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
        metadata_read_calls: snapshot.reads.issued.calls,
        metadata_read_bytes: snapshot.reads.issued.bytes,
        metadata_peak_retained_bytes: snapshot.governor.peak_retained_bytes,
        metadata_peak_in_flight_bytes: snapshot.governor.peak_in_flight_bytes,
        metadata_peak_open_files: snapshot.files.peak_open_files,
        metadata_cache_hits: snapshot.cache.hits,
        metadata_cache_misses: snapshot.cache.misses,
    })
}

fn footer_file_len(footer: &SegmentFooter, file: SegmentFile) -> io::Result<u64> {
    footer
        .files
        .iter()
        .find_map(|entry| (entry.file == file).then_some(entry.size))
        .ok_or_else(|| invalid_segment_data("segment footer omits a tracked file"))
}

fn evenly_spaced_series_refs(series_count: u32, limit: u32) -> Vec<u32> {
    let selected = limit.min(series_count);
    match selected {
        0 => Vec::new(),
        1 => vec![0],
        selected => {
            let last = u64::from(series_count - 1);
            let denominator = u64::from(selected - 1);
            (0..selected)
                .map(|index| ((u64::from(index) * last) / denominator) as u32)
                .collect()
        }
    }
}

fn facade_io(error: impl std::error::Error + Send + Sync + 'static) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, error)
}

fn hash_bytes(hasher: &mut Sha256, bytes: &[u8]) -> io::Result<()> {
    hash_u32(
        hasher,
        u32::try_from(bytes.len())
            .map_err(|_| invalid_segment_data("fingerprint byte string exceeds u32"))?,
    );
    hasher.update(bytes);
    Ok(())
}

fn hash_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_le_bytes());
}

fn hash_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_le_bytes());
}

fn chunk_kind_id(kind: ChunkKind) -> u8 {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}

fn chunk_sample_count(samples: &ChunkSamples) -> u64 {
    match samples {
        ChunkSamples::Float(values) => values.len() as u64,
        ChunkSamples::Int64(values) => values.len() as u64,
        ChunkSamples::Histogram(values) => values.len() as u64,
        ChunkSamples::ExponentialHistogram(values) => values.len() as u64,
        ChunkSamples::Summary(values) => values.len() as u64,
    }
}

fn hex_digest(digest: [u8; 32]) -> String {
    let mut value = String::with_capacity(64);
    for byte in digest {
        use std::fmt::Write as _;
        write!(&mut value, "{byte:02x}").expect("writing to a String cannot fail");
    }
    value
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::labels::SeriesRef;
    use crate::promql::METRIC_NAME_LABEL;
    use crate::storage::metadata_cache::{MetadataCacheError, StructuralMetadataErrorKind};
    use crate::storage::segment::metadata_facade::SegmentMetadataFacadeError;
    use crate::storage::series::v3::Schema7MetadataReaderError;

    use super::*;

    #[test]
    fn schema6_and_schema7_verified_selection_matches_after_full_decode() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema6.path(), false);
        write_fixture(schema7.path(), true);

        let schema6_report = verify_experimental_storage_corpus(
            schema6.path(),
            SegmentStorageSchema::Schema6,
            true,
            None,
        )
        .unwrap();
        let schema7_report = verify_experimental_storage_corpus(
            schema7.path(),
            SegmentStorageSchema::Schema7,
            true,
            None,
        )
        .unwrap();

        assert_eq!(
            schema6_report.verified_selection_fingerprint,
            schema7_report.verified_selection_fingerprint
        );
        assert_eq!(schema6_report.segments, 1);
        assert_eq!(schema6_report.series, 2);
        assert_eq!(schema6_report.chunks, 2);
        assert_eq!(schema6_report.samples, 3);
        assert_eq!(
            schema6_report.logical_chunk_bytes,
            schema7_report.logical_chunk_bytes
        );

        let sampled = verify_experimental_storage_corpus(
            schema7.path(),
            SegmentStorageSchema::Schema7,
            false,
            Some(1),
        )
        .unwrap();
        assert_eq!(sampled.series, 1);
        assert_ne!(
            sampled.verified_selection_fingerprint, schema7_report.verified_selection_fingerprint,
            "sampled and exhaustive selections have distinct fingerprint streams"
        );
    }

    #[test]
    fn independently_written_schema7_and_schema8_corpora_match_after_full_decode() {
        let schema7 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_fixture(schema7.path(), true);
        write_schema8_fixture(schema8.path());

        let schema7_report = verify_experimental_storage_corpus_with_exact_postings(
            schema7.path(),
            SegmentStorageSchema::Schema7,
            true,
            None,
        )
        .unwrap();
        let schema8_report = verify_experimental_storage_corpus_with_exact_postings(
            schema8.path(),
            SegmentStorageSchema::Schema8,
            true,
            None,
        )
        .unwrap();

        assert_eq!(
            schema7_report.verified_selection_fingerprint,
            schema8_report.verified_selection_fingerprint
        );
        assert_eq!(schema7_report.segments, schema8_report.segments);
        assert_eq!(schema7_report.corpus_series, schema8_report.corpus_series);
        assert_eq!(schema7_report.series, schema8_report.series);
        assert_eq!(schema7_report.chunks, schema8_report.chunks);
        assert_eq!(schema7_report.chunks_by_kind, schema8_report.chunks_by_kind);
        assert_eq!(schema7_report.samples, schema8_report.samples);
        assert_eq!(
            schema7_report.logical_chunk_bytes,
            schema8_report.logical_chunk_bytes
        );
        let schema7_postings = schema7_report.exact_postings.unwrap();
        let schema8_postings = schema8_report.exact_postings.unwrap();
        assert_eq!(
            schema7_postings.logical_fingerprint,
            schema8_postings.logical_fingerprint
        );
        assert_eq!(schema7_postings.lists, schema8_postings.lists);
        assert_eq!(schema7_postings.decoded_refs, schema8_postings.decoded_refs);
        assert!(schema8_postings.encoded_bytes < schema7_postings.encoded_bytes);
    }

    #[test]
    fn schema6_and_schema7_promql_query_facades_match() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema6.path(), false);
        write_fixture(schema7.path(), true);

        let schema6_store = SegmentStoreReader::open_with_options(
            schema6.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let query = "replay_float";
        let mut schema6_session = schema6_store.query_session().unwrap();
        let schema6_execution = schema6_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let mut schema7_session = schema7_store.query_session().unwrap();
        let schema7_execution = schema7_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();

        assert_eq!(schema6_execution.results.len(), 1);
        assert_eq!(schema6_execution.results[0].samples.len(), 2);
        assert_eq!(schema6_execution.stats, schema7_execution.stats);
        assert_eq!(
            schema6_execution.semantic_fingerprint_sha256(),
            schema7_execution.semantic_fingerprint_sha256()
        );
        assert_eq!(
            schema6_execution.portable_semantic_fingerprint_sha256(),
            schema7_execution.portable_semantic_fingerprint_sha256()
        );
    }

    #[test]
    fn schema7_and_schema8_default_demand_driven_labels_match_forced_full_labels() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_fixture(schema6.path(), false);
        write_selective_fixture(schema7.path(), true);
        write_selective_schema8_fixture(schema8.path());

        let schema6_store = SegmentStoreReader::open_with_options(
            schema6.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema8_store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut schema6_session = schema6_store.query_session().unwrap();
        let mut schema7_session = schema7_store.query_session().unwrap();
        let mut schema7_full_session = schema7_store.query_session().unwrap();
        let mut schema8_session = schema8_store.query_session().unwrap();
        let mut schema8_full_session = schema8_store.query_session().unwrap();
        let mut schema8_owned_session = schema8_store.query_session().unwrap();
        let mut schema8_profiled_owned_session = schema8_store.query_session().unwrap();
        schema8_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();
        schema8_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap();
        schema7_full_session
            .set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
        schema8_full_session
            .set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
        schema8_owned_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::OwnedStrings)
            .unwrap();
        schema8_profiled_owned_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();

        for query in [
            "sum by (service) (replay_float)",
            "sum by (service) (rate(replay_float[3s]))",
            "sum by (service) (increase(replay_float[3s]))",
            "sum(replay_float{instance=~\"api-[12]\"})",
            "sum by (__name__) (rate(replay_float[3s]))",
            "count by (service) (replay_hist{instance=~\"api-1\"})",
            "group(replay_exp)",
            "count by (service) (rate(replay_hist[3s]))",
            "group by (service) (increase(replay_exp[3s]))",
            "count by (__name__) (replay_hist)",
            "count by (__name__) (rate(replay_hist[3s]))",
        ] {
            let schema7_profile_before = schema7_session.profile();
            let schema7_full_profile_before = schema7_full_session.profile();
            let schema8_profile_before = schema8_session.profile();
            let schema8_full_profile_before = schema8_full_session.profile();
            let schema8_owned_profile_before = schema8_owned_session.profile();
            let schema8_profiled_owned_profile_before = schema8_profiled_owned_session.profile();
            let schema6_execution = schema6_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema7_execution = schema7_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema7_full_execution = schema7_full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_started = Instant::now();
            let schema8_execution = schema8_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_elapsed = schema8_started.elapsed();
            let schema8_owned_execution = schema8_owned_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_profiled_owned_execution = schema8_profiled_owned_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema8_full_execution = schema8_full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let schema7_profile = schema7_session
                .profile()
                .delta_since(schema7_profile_before);
            let schema7_full_profile = schema7_full_session
                .profile()
                .delta_since(schema7_full_profile_before);
            let schema8_profile = schema8_session
                .profile()
                .delta_since(schema8_profile_before);
            let schema8_full_profile = schema8_full_session
                .profile()
                .delta_since(schema8_full_profile_before);
            let schema8_owned_profile = schema8_owned_session
                .profile()
                .delta_since(schema8_owned_profile_before);
            let schema8_profiled_owned_profile = schema8_profiled_owned_session
                .profile()
                .delta_since(schema8_profiled_owned_profile_before);

            assert_eq!(schema6_execution.stats, schema7_execution.stats, "{query}");
            assert_eq!(
                schema7_full_execution.stats, schema7_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema8_full_execution.stats, schema8_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema8_owned_execution.stats, schema8_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema8_profiled_owned_execution.stats, schema8_owned_execution.stats,
                "{query}"
            );
            assert_eq!(
                schema6_execution.semantic_fingerprint_sha256(),
                schema7_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema7_full_execution.semantic_fingerprint_sha256(),
                schema7_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema6_execution.semantic_fingerprint_sha256(),
                schema8_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_full_execution.semantic_fingerprint_sha256(),
                schema8_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_owned_execution.semantic_fingerprint_sha256(),
                schema8_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_profiled_owned_execution.semantic_fingerprint_sha256(),
                schema8_owned_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema7_full_execution.portable_semantic_fingerprint_sha256(),
                schema7_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_full_execution.portable_semantic_fingerprint_sha256(),
                schema8_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_owned_execution.portable_semantic_fingerprint_sha256(),
                schema8_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                schema8_profiled_owned_execution.portable_semantic_fingerprint_sha256(),
                schema8_owned_execution.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert!(
                schema8_execution.results.iter().all(|result| {
                    result.labels.uses_shared_atoms()
                        && !result.labels.owned_compatibility_materialized()
                }),
                "shared execution materialized an owned compatibility view for {query}"
            );
            assert!(
                schema8_owned_execution
                    .results
                    .iter()
                    .all(|result| !result.labels.uses_shared_atoms()),
                "owned comparator returned shared labels for {query}"
            );
            assert!(
                schema7_execution
                    .results
                    .iter()
                    .all(SegmentQueryResult::labels_are_complete)
            );
            assert!(
                schema8_execution
                    .results
                    .iter()
                    .all(SegmentQueryResult::labels_are_complete)
            );
            assert_eq!(
                schema7_profile.label_pairs_integrity_checked,
                schema7_full_profile.label_pairs_integrity_checked,
                "{query}"
            );
            assert!(schema7_profile.label_rows_selectively_materialized > 0);
            assert!(schema7_profile.label_pairs_omitted > 0);
            assert_eq!(schema7_full_profile.label_rows_selectively_materialized, 0);
            assert_eq!(schema7_full_profile.label_pairs_omitted, 0);
            assert!(
                schema7_profile.label_pairs_materialized
                    < schema7_full_profile.label_pairs_materialized
            );
            assert_eq!(
                schema8_profile.label_pairs_integrity_checked,
                schema8_full_profile.label_pairs_integrity_checked,
                "{query}"
            );
            assert!(schema8_profile.label_rows_selectively_materialized > 0);
            assert!(schema8_profile.label_pairs_omitted > 0);
            assert_eq!(schema8_full_profile.label_rows_selectively_materialized, 0);
            assert_eq!(schema8_full_profile.label_pairs_omitted, 0);
            assert!(
                schema8_profile.label_pairs_materialized
                    < schema8_full_profile.label_pairs_materialized
            );
            if query == "sum by (service) (replay_float)" {
                assert_eq!(schema8_owned_profile.stages, QueryStageProfile::default());
                assert!(schema8_profiled_owned_profile.stages.total_exclusive() > Duration::ZERO);
                let stages = schema8_profile.stages;
                assert!(
                    stages
                        .canonical_row_decode
                        .saturating_add(stages.symbol_resolution)
                        .saturating_add(stages.canonical_identity)
                        .saturating_add(stages.metadata_visit_overhead)
                        > Duration::ZERO
                );
                assert!(
                    stages
                        .symbol_lookup
                        .saturating_add(stages.candidate_selection)
                        .saturating_add(stages.matcher_evaluation)
                        .saturating_add(stages.locator_planning)
                        > Duration::ZERO
                );
                assert!(
                    stages
                        .payload_io
                        .saturating_add(stages.payload_decode)
                        .saturating_add(stages.source_merge)
                        > Duration::ZERO
                );
                assert!(
                    stages
                        .promql_grouping_evaluation
                        .saturating_add(stages.result_construction)
                        > Duration::ZERO
                );
                assert!(stages.total_exclusive() <= schema8_elapsed);
            }
        }

        for query in [
            "sum by (service) (last_over_time(replay_float[3s]))",
            "sum without (instance) (replay_float)",
            "topk(1, replay_float)",
            "bottomk(1, replay_float)",
            "count_values(\"sample\", replay_float)",
            "sum(sum by (service) (replay_float))",
            "label_replace(replay_float, \"copy\", \"$1\", \"instance\", \"(.*)\")",
            "replay_float + 1",
            "replay_float or replay_float",
            "sort(replay_float)",
            "absent(replay_missing)",
            "sum by (service) (replay_hist)",
            "sum by (service) (replay_exp)",
            "sum by (service) (replay_hist_count)",
            "sum by (service) (replay_exp_count)",
            "count without (instance) (replay_hist)",
            "count without (instance) (rate(replay_exp[3s]))",
            "histogram_count(replay_hist)",
            "sum(count by (service) (replay_hist))",
            "sum(group by (service) (increase(replay_exp[3s])))",
        ] {
            let profile_before = schema8_session.profile();
            let full_profile_before = schema8_full_session.profile();
            let execution = schema8_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let full_execution = schema8_full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let owned_execution = schema8_owned_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let profile = schema8_session.profile().delta_since(profile_before);
            let full_profile = schema8_full_session
                .profile()
                .delta_since(full_profile_before);

            assert_eq!(execution.stats, full_execution.stats, "{query}");
            assert_eq!(execution.stats, owned_execution.stats, "{query}");
            assert_eq!(
                execution.semantic_fingerprint_sha256(),
                full_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                execution.semantic_fingerprint_sha256(),
                owned_execution.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert!(
                execution.results.iter().all(|result| {
                    result.labels.uses_shared_atoms()
                        && !result.labels.owned_compatibility_materialized()
                }),
                "shared execution materialized an owned compatibility view for {query}"
            );
            assert_eq!(profile.label_rows_selectively_materialized, 0, "{query}");
            assert_eq!(profile.label_pairs_omitted, 0, "{query}");
            assert_eq!(
                profile.label_pairs_materialized, full_profile.label_pairs_materialized,
                "{query}"
            );
        }

        for range_query in [
            "sum by (service) (rate(replay_float[3s]))",
            "count by (service) (rate(replay_hist[3s]))",
            "group by (service) (increase(replay_exp[3s]))",
        ] {
            let selective_profile_before = schema8_session.profile();
            let full_profile_before = schema8_full_session.profile();
            let range_execution = schema8_session
                .query_promql_range_with_limits(
                    range_query,
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            let full_range_execution = schema8_full_session
                .query_promql_range_with_limits(
                    range_query,
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            let owned_range_execution = schema8_owned_session
                .query_promql_range_with_limits(
                    range_query,
                    2_000,
                    3_000,
                    1_000,
                    QueryLimits::unlimited(),
                )
                .unwrap();
            let selective_profile = schema8_session
                .profile()
                .delta_since(selective_profile_before);
            let full_profile = schema8_full_session
                .profile()
                .delta_since(full_profile_before);
            assert_eq!(
                range_execution.stats, full_range_execution.stats,
                "{range_query}"
            );
            assert_eq!(
                range_execution.stats, owned_range_execution.stats,
                "{range_query}"
            );
            assert_eq!(
                range_execution.semantic_fingerprint_sha256(),
                full_range_execution.semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert_eq!(
                range_execution.portable_semantic_fingerprint_sha256(),
                full_range_execution.portable_semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert_eq!(
                range_execution.semantic_fingerprint_sha256(),
                owned_range_execution.semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert_eq!(
                range_execution.portable_semantic_fingerprint_sha256(),
                owned_range_execution.portable_semantic_fingerprint_sha256(),
                "{range_query}"
            );
            assert!(
                range_execution.results.iter().all(|result| {
                    result.labels.uses_shared_atoms()
                        && !result.labels.owned_compatibility_materialized()
                }),
                "shared range execution materialized an owned compatibility view for {range_query}"
            );
            assert!(
                range_execution
                    .results
                    .iter()
                    .all(SegmentQueryResult::labels_are_complete),
                "{range_query}"
            );
            assert!(
                selective_profile.label_rows_selectively_materialized > 0,
                "{range_query}"
            );
            assert!(selective_profile.label_pairs_omitted > 0, "{range_query}");
            assert_eq!(
                full_profile.label_rows_selectively_materialized, 0,
                "{range_query}"
            );
            assert_eq!(full_profile.label_pairs_omitted, 0, "{range_query}");
        }

        let direct_name = schema8_session
            .query_promql_with_limits(
                "count by (__name__) (replay_hist)",
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        assert_eq!(
            direct_name.results[0].labels.pairs().collect::<Vec<_>>(),
            vec![(METRIC_NAME_LABEL, "replay_hist")]
        );
        let range_name = schema8_session
            .query_promql_with_limits(
                "count by (__name__) (rate(replay_hist[3s]))",
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        assert!(range_name.results[0].labels.is_empty());

        // A selective execution must not populate the session-wide full-label
        // cache with its reduced label set.
        let raw = schema7_session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        assert_eq!(raw.results.len(), 2);
        assert!(raw.results.iter().all(|result| {
            result
                .labels
                .pairs()
                .any(|(name, value)| name == "instance" && (value == "api-1" || value == "api-2"))
        }));

        let mixed_query = r#"count by (service) ({__name__=~"replay_(float|hist|exp)"})"#;
        let mixed_shared = schema8_session
            .query_promql_with_limits(mixed_query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let mixed_owned = schema8_owned_session
            .query_promql_with_limits(mixed_query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        assert_eq!(mixed_shared.stats, mixed_owned.stats);
        assert_eq!(
            mixed_shared.semantic_fingerprint_sha256(),
            mixed_owned.semantic_fingerprint_sha256()
        );
        assert_eq!(
            mixed_shared.portable_semantic_fingerprint_sha256(),
            mixed_owned.portable_semantic_fingerprint_sha256()
        );
        assert!(mixed_shared.results.iter().all(|result| {
            result.labels.uses_shared_atoms() && !result.labels.owned_compatibility_materialized()
        }));

        let detached = {
            let mut session = schema8_store.query_session().unwrap();
            session
                .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
                .unwrap();
            session
                .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
                .unwrap()
        };
        assert!(detached.results.iter().all(|result| {
            result.labels.uses_shared_atoms()
                && result
                    .labels
                    .pairs()
                    .any(|(name, _)| name == METRIC_NAME_LABEL)
        }));
        assert!(schema8_session.query_label_storage_stats().atom_hits > 0);
        assert_eq!(
            schema8_owned_session
                .query_label_storage_stats()
                .atom_lookups,
            0
        );
        let policy_change_error = schema8_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::OwnedStrings)
            .unwrap_err();
        assert_eq!(policy_change_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            schema8_session.query_label_storage_policy(),
            QueryLabelStoragePolicy::SharedAtoms
        );
    }

    #[test]
    fn schema8_demand_driven_native_mixed_kind_row_falls_back_to_full_labels() {
        let schema8 = tempfile::tempdir().unwrap();
        write_mixed_kind_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        for query in [
            "count by (service) (mixed_kind)",
            "group by (service) (mixed_kind)",
        ] {
            let mut demand_session = store.query_session().unwrap();
            let mut full_session = store.query_session().unwrap();
            demand_session
                .set_label_materialization_policy(QueryLabelMaterializationPolicy::DemandDriven);
            full_session.set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);

            let demand_before = demand_session.profile();
            let full_before = full_session.profile();
            let demand = demand_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let full = full_session
                .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
                .unwrap();
            let demand_profile = demand_session.profile().delta_since(demand_before);
            let full_profile = full_session.profile().delta_since(full_before);

            assert_eq!(demand.stats, full.stats, "{query}");
            assert_eq!(
                demand.semantic_fingerprint_sha256(),
                full.semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                demand.portable_semantic_fingerprint_sha256(),
                full.portable_semantic_fingerprint_sha256(),
                "{query}"
            );
            assert_eq!(
                demand_profile.label_pairs_integrity_checked,
                full_profile.label_pairs_integrity_checked,
                "{query}"
            );
            assert_eq!(
                demand_profile.label_rows_selectively_materialized, 0,
                "mixed-kind rows must not use reduced labels for {query}"
            );
            assert_eq!(demand_profile.label_pairs_omitted, 0, "{query}");
            assert_eq!(
                demand_profile.label_pairs_materialized, full_profile.label_pairs_materialized,
                "{query}"
            );
            assert!(demand_profile.label_pairs_materialized >= 4, "{query}");
        }
    }

    #[test]
    fn query_label_storage_policy_freezes_on_empty_prefetch_and_parse_error_attempts() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut empty_session = store.query_session().unwrap();
        let empty = empty_session
            .query_promql_with_limits("replay_missing", 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        assert!(empty.results.is_empty());
        let empty_error = empty_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(empty_error.kind(), io::ErrorKind::InvalidInput);

        let mut prefetch_session = store.query_session().unwrap();
        prefetch_session
            .prefetch_promql_data("replay_float", 0, 3_000)
            .unwrap();
        let prefetch_error = prefetch_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(prefetch_error.kind(), io::ErrorKind::InvalidInput);

        let mut malformed_session = store.query_session().unwrap();
        malformed_session
            .query_promql_with_limits("sum(", 0, 3_000, QueryLimits::unlimited())
            .unwrap_err();
        let malformed_error = malformed_session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(malformed_error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn query_instrumentation_off_is_semantically_identical_to_detailed() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut off_session = store.query_session().unwrap();
        let mut detailed_session = store.query_session().unwrap();
        assert_eq!(
            off_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );
        detailed_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();

        let off_before = off_session.profile();
        let detailed_before = detailed_session.profile();
        let query = "sum by (service) (rate(replay_float[3s]))";
        let off = off_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let detailed = detailed_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let off_profile = off_session.profile().delta_since(off_before);
        let detailed_profile = detailed_session.profile().delta_since(detailed_before);

        assert_eq!(off.stats, detailed.stats);
        assert_eq!(
            off.semantic_fingerprint_sha256(),
            detailed.semantic_fingerprint_sha256()
        );
        assert_eq!(
            off.portable_semantic_fingerprint_sha256(),
            detailed.portable_semantic_fingerprint_sha256()
        );
        assert_eq!(off_profile.stages, QueryStageProfile::default());
        assert!(detailed_profile.stages.total_exclusive() > Duration::ZERO);
    }

    #[test]
    fn query_instrumentation_mode_freezes_on_first_query_prewarm_or_prefetch() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut query_session = store.query_session().unwrap();
        query_session
            .query_promql_with_limits("1", 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let query_error = query_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap_err();
        assert_eq!(query_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            query_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );

        let mut prewarm_session = store.query_session().unwrap();
        prewarm_session.prewarm_promql("1", 0, 3_000).unwrap();
        let prewarm_error = prewarm_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap_err();
        assert_eq!(prewarm_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            prewarm_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );

        let mut prefetch_session = store.query_session().unwrap();
        prefetch_session
            .prefetch_promql_data("1", 0, 3_000)
            .unwrap();
        let prefetch_error = prefetch_session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap_err();
        assert_eq!(prefetch_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            prefetch_session.query_instrumentation_mode(),
            QueryInstrumentationMode::Off
        );
    }

    #[test]
    fn query_instrumentation_detailed_missing_equality_records_no_payload_or_result_work() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let mut session = store.query_session().unwrap();
        session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();
        let before = session.profile();
        let execution = session
            .query_promql_with_limits(
                "replay_float{service=\"does-not-exist\"}",
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        let stages = session.profile().delta_since(before).stages;

        assert!(execution.results.is_empty());
        assert_eq!(stages.payload_io, Duration::ZERO);
        assert_eq!(stages.payload_decode, Duration::ZERO);
        assert_eq!(stages.source_merge, Duration::ZERO);
        assert_eq!(stages.result_construction, Duration::ZERO);
        assert!(
            stages.symbol_lookup > Duration::ZERO || stages.matcher_evaluation > Duration::ZERO
        );
    }

    #[test]
    fn query_label_storage_policy_freezes_before_touched_series_page_corruption() {
        use crate::storage::series::v3::{
            SERIES_HEADER_LEN_V3, SERIES_HOT_PAGE_HEADER_LEN_V1, SeriesHeaderV3,
        };

        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let inventory = read_manifest_inventory(schema8.path().join("manifest"))
            .unwrap()
            .unwrap();
        let series_path = schema8
            .path()
            .join(&inventory.segments[0].segment_id)
            .join(SegmentFile::Series.filename());
        let mut series = fs::read(&series_path).unwrap();
        let header = SeriesHeaderV3::decode(&series[..SERIES_HEADER_LEN_V3]).unwrap();
        let corrupt_offset =
            usize::try_from(header.hot_pages_offset).unwrap() + SERIES_HOT_PAGE_HEADER_LEN_V1;
        series[corrupt_offset] ^= 0x80;
        fs::write(&series_path, series).unwrap();

        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();
        let profile_before = session.profile();
        let query_started = Instant::now();
        let query_error = session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .unwrap_err();
        let query_elapsed = query_started.elapsed();
        let stages = session.profile().delta_since(profile_before).stages;

        assert!(
            query_error
                .to_string()
                .contains("series v3 hot page CRC mismatch")
        );
        assert!(stages.total_exclusive() > Duration::ZERO);
        assert!(stages.total_exclusive() <= query_elapsed);
        assert!(stages.metadata_visit_overhead > Duration::ZERO);
        assert_eq!(stages.payload_io, Duration::ZERO);
        assert_eq!(stages.payload_decode, Duration::ZERO);
        assert_eq!(stages.result_construction, Duration::ZERO);
        assert_eq!(
            session.query_label_storage_stats(),
            QueryLabelStorageStats::default(),
            "the touched page must fail before label interning"
        );
        let policy_error = session
            .set_query_label_storage_policy(QueryLabelStoragePolicy::SharedAtoms)
            .unwrap_err();
        assert_eq!(policy_error.kind(), io::ErrorKind::InvalidInput);
    }

    #[test]
    fn query_instrumentation_detailed_records_transient_metadata_budget_refusal() {
        let schema8 = tempfile::tempdir().unwrap();
        write_selective_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_query_instrumentation_mode(QueryInstrumentationMode::Detailed)
            .unwrap();

        let governor_before = store.metadata_runtime.snapshot().governor;
        let blocker = store
            .metadata_runtime
            .governor()
            .reserve_in_flight_for_usage(
                governor_before
                    .in_flight_max_bytes
                    .checked_sub(governor_before.in_flight_bytes)
                    .and_then(|remaining| remaining.checked_sub(1))
                    .expect("fixture leaves one reservable metadata byte"),
                crate::storage::metadata_governor::MetadataUsageClass::Scratch,
            )
            .expect("reserve all but one in-flight metadata byte");
        let runtime_before = store.metadata_runtime.snapshot();
        let profile_before = session.profile();
        let query_started = Instant::now();
        let query_error = session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .unwrap_err();
        let query_elapsed = query_started.elapsed();
        let stages = session.profile().delta_since(profile_before).stages;
        let runtime_refused = store.metadata_runtime.snapshot();

        assert!(query_error.to_string().contains("metadata"));
        assert!(stages.total_exclusive() > Duration::ZERO);
        assert!(stages.total_exclusive() <= query_elapsed);
        assert!(stages.metadata_visit_overhead > Duration::ZERO);
        assert_eq!(stages.payload_io, Duration::ZERO);
        assert_eq!(stages.payload_decode, Duration::ZERO);
        assert_eq!(stages.result_construction, Duration::ZERO);
        assert_eq!(runtime_refused.reads, runtime_before.reads);
        assert_eq!(
            runtime_refused.cache.sticky_artifacts,
            runtime_before.cache.sticky_artifacts
        );
        assert_eq!(
            runtime_refused.governor.in_flight_refusals,
            runtime_before.governor.in_flight_refusals + 1
        );

        drop(blocker);
        let retry = session
            .query_promql_with_limits("replay_float", 0, 3_000, QueryLimits::unlimited())
            .expect("transient metadata-budget refusal must allow a clean retry");
        assert!(!retry.results.is_empty());
    }

    #[test]
    fn schema7_and_schema8_promql_query_facades_match() {
        let schema7 = tempfile::tempdir().unwrap();
        let schema8 = tempfile::tempdir().unwrap();
        write_fixture(schema7.path(), true);
        write_schema8_fixture(schema8.path());

        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema8_store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let query = "replay_float";
        let mut schema7_session = schema7_store.query_session().unwrap();
        let schema7_execution = schema7_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();
        let mut schema8_session = schema8_store.query_session().unwrap();
        let schema8_execution = schema8_session
            .query_promql_with_limits(query, 0, 3_000, QueryLimits::unlimited())
            .unwrap();

        assert_eq!(schema7_execution.results, schema8_execution.results);
        assert_eq!(
            schema7_execution.semantic_fingerprint_sha256(),
            schema8_execution.semantic_fingerprint_sha256()
        );
        assert_eq!(
            schema7_execution.portable_semantic_fingerprint_sha256(),
            schema8_execution.portable_semantic_fingerprint_sha256()
        );

        let mut schema8_normalized_stats = schema8_execution.stats;
        schema8_normalized_stats.index_postings_bytes_read =
            schema7_execution.stats.index_postings_bytes_read;
        assert_eq!(schema7_execution.stats, schema8_normalized_stats);
        assert!(
            schema8_execution.stats.index_postings_bytes_read
                < schema7_execution.stats.index_postings_bytes_read,
            "adaptive postings should issue fewer exact-postings payload bytes"
        );
    }

    #[test]
    fn schema8_public_store_reader_surfaces_use_the_v9_facade() {
        let schema8 = tempfile::tempdir().unwrap();
        write_schema8_fixture(schema8.path());
        let store = SegmentStoreReader::open_with_options(
            schema8.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let exact = store
            .query_exact(&[(METRIC_NAME_LABEL, "replay_float")], 0, 3_000)
            .unwrap();
        let selector = store
            .query_selector(
                &SegmentSelector::with_metric(
                    "replay_float",
                    vec![LabelMatcher::eq("service", "api")],
                ),
                0,
                3_000,
            )
            .unwrap();
        let promql = store.query_promql("replay_float", 0, 3_000).unwrap();

        assert_eq!(exact, selector);
        assert_eq!(exact, promql);
        assert_eq!(exact.len(), 1);
        assert_eq!(exact[0].samples, [(1_000, 1.0), (2_000, 2.0)]);
        assert_eq!(
            store.metric_names(0, 3_000).unwrap(),
            ["replay_float", "replay_int"]
        );
        assert_eq!(
            store.label_names(0, 3_000).unwrap(),
            [METRIC_NAME_LABEL, "service"]
        );
        assert_eq!(
            store.label_values("service", 0, 3_000).unwrap(),
            ["api", "worker"]
        );
    }

    #[test]
    fn schema6_and_schema7_smoke_reports_match_through_metadata_facade() {
        let schema6 = tempfile::tempdir().unwrap();
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema6.path(), false);
        write_fixture(schema7.path(), true);

        let schema6_store = SegmentStoreReader::open_with_options(
            schema6.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let schema7_store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();

        let schema6_report = schema6_store.smoke_verify(0, 3_000, 1).unwrap();
        let schema7_report = schema7_store.smoke_verify(0, 3_000, 1).unwrap();

        assert_eq!(schema6_report, schema7_report);
        assert_eq!(schema7_report.totals.series, 2);
        assert_eq!(schema7_report.totals.chunks, 2);
        assert_eq!(schema7_report.sample_series.len(), 2);
        assert!(
            schema7_report
                .queries
                .iter()
                .all(|query| query.result_samples > 0)
        );
    }

    #[test]
    fn schema7_smoke_rejects_corrupt_indexed_chunk_prefix() {
        let schema7 = tempfile::tempdir().unwrap();
        write_fixture(schema7.path(), true);
        let inventory = read_manifest_inventory(schema7.path().join("manifest"))
            .unwrap()
            .unwrap();
        let chunks_path = schema7
            .path()
            .join(&inventory.segments[0].segment_id)
            .join(SegmentFile::Chunks.filename());
        let mut chunks = fs::read(&chunks_path).unwrap();
        chunks[CHUNK_FRAME_HEADER_LEN] ^= 0x80;
        fs::write(chunks_path, chunks).unwrap();

        let store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let error = store.smoke_verify(0, 3_000, 1).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("indexed prefix crc mismatch"));
    }

    #[test]
    fn schema7_smoke_resumes_a_series_across_bounded_payload_batches() {
        const CHUNKS: usize = 65;

        let schema7 = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(schema7.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(43)
            .with_storage_schema(SegmentStorageSchema::Schema7);
        let mut writer = SegmentWriter::new(config).unwrap();
        for chunk_index in 0..CHUNKS {
            let timestamp = 1_000 + chunk_index as u64;
            writer
                .record_samples_ordered_with_label_visitor(
                    SeriesRef::new(1),
                    &[(timestamp, chunk_index as f64)],
                    |visit| {
                        visit(METRIC_NAME_LABEL, "many_chunks");
                        visit("service", "api");
                    },
                )
                .unwrap();
        }
        writer.flush().unwrap();

        let store = SegmentStoreReader::open_with_options(
            schema7.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        assert_eq!(store.segments.len(), 1);
        let mut report = SegmentStoreSmokeReport::default();
        store.segments[0]
            .collect_smoke_report(0, 3_000, CHUNKS, true, &mut report)
            .unwrap();

        assert_eq!(report.totals.chunks, CHUNKS as u64);
        assert_eq!(report.sample_series.len(), CHUNKS);
        assert_eq!(report.sample_series.first().unwrap().min_time_ms, 1_000);
        assert_eq!(report.sample_series.last().unwrap().min_time_ms, 1_064);
        assert!(
            report
                .sample_series
                .iter()
                .all(|sample| sample.samples == 1 && sample.kind == ChunkKind::Float)
        );
    }

    #[test]
    fn smoke_facade_error_mapping_preserves_transient_and_structural_kinds() {
        for (cache_error, expected_kind) in [
            (
                MetadataCacheError::transient(io::ErrorKind::TimedOut, "metadata read timed out"),
                io::ErrorKind::TimedOut,
            ),
            (
                MetadataCacheError::structural(
                    StructuralMetadataErrorKind::UnexpectedEof,
                    "metadata page is truncated",
                ),
                io::ErrorKind::UnexpectedEof,
            ),
        ] {
            let error = super::query_reader::metadata_facade_io_error(
                SegmentMetadataFacadeError::Schema7Metadata(Schema7MetadataReaderError::Cache(
                    cache_error,
                )),
            );
            assert_eq!(error.kind(), expected_kind);
        }

        let error = super::query_reader::metadata_facade_io_error(
            SegmentMetadataFacadeError::RefSetAllocation(io::Error::new(
                io::ErrorKind::OutOfMemory,
                "series-ref allocation failed",
            )),
        );
        assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    }

    fn write_fixture(path: &Path, schema7: bool) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42)
            .with_storage_schema(if schema7 {
                SegmentStorageSchema::Schema7
            } else {
                SegmentStorageSchema::Schema6
            });
        write_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn write_schema8_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        write_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn write_selective_fixture(path: &Path, schema7: bool) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(43)
            .with_storage_schema(if schema7 {
                SegmentStorageSchema::Schema7
            } else {
                SegmentStorageSchema::Schema6
            });
        write_selective_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn write_selective_schema8_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(43)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        write_selective_fixture_samples(SegmentWriter::new(config).unwrap());
    }

    fn write_mixed_kind_schema8_fixture(path: &Path) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(44)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_kind");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(
                    2_000,
                    HistogramValue {
                        count: 1,
                        sum: Some(0.5),
                        min: Some(0.5),
                        max: Some(0.5),
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 0],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_kind");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_selective_fixture_samples(mut writer: SegmentWriter) {
        for (series_ref, instance, samples) in [
            (SeriesRef::new(1), "api-1", [(1_000, 1.0), (2_000, 2.0)]),
            (SeriesRef::new(2), "api-2", [(1_000, 2.0), (2_000, 4.0)]),
        ] {
            writer
                .record_samples_ordered_with_label_visitor(series_ref, &samples, |visit| {
                    visit(METRIC_NAME_LABEL, "replay_float");
                    visit("service", "api");
                    visit("instance", instance);
                    visit("region", "sg");
                })
                .unwrap();
        }
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(3),
                &[
                    (
                        1_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(0.5),
                            min: Some(0.5),
                            max: Some(0.5),
                            metadata: TypedSampleMetadata::default(),
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![1, 0],
                        },
                    ),
                    (
                        2_000,
                        HistogramValue {
                            count: 3,
                            sum: Some(4.0),
                            min: Some(0.5),
                            max: Some(2.0),
                            metadata: TypedSampleMetadata::default(),
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![1, 2],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_hist");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer
            .record_exponential_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(4),
                &[
                    (
                        1_000,
                        ExponentialHistogramValue {
                            count: 1,
                            sum: Some(0.5),
                            min: Some(0.5),
                            max: Some(0.5),
                            scale: 1,
                            zero_threshold: 0.0,
                            zero_count: 0,
                            metadata: TypedSampleMetadata::default(),
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![1, 0],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![0],
                            },
                        },
                    ),
                    (
                        2_000,
                        ExponentialHistogramValue {
                            count: 3,
                            sum: Some(4.0),
                            min: Some(0.5),
                            max: Some(2.0),
                            scale: 1,
                            zero_threshold: 0.0,
                            zero_count: 0,
                            metadata: TypedSampleMetadata::default(),
                            positive: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![1, 2],
                            },
                            negative: ExponentialHistogramBuckets {
                                offset: 0,
                                counts: vec![0],
                            },
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_exp");
                    visit("service", "api");
                    visit("instance", "api-1");
                    visit("region", "sg");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }

    fn write_fixture_samples(mut writer: SegmentWriter) {
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.0), (2_000, 2.0)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_float");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer
            .record_i64_samples_ordered_with_label_visitor(
                SeriesRef::new(2),
                &[(1_500, 7)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "replay_int");
                    visit("service", "worker");
                },
            )
            .unwrap();
        writer.flush().unwrap();
    }
}
