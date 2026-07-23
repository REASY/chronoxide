use std::fs::{self, File};
use std::io;
use std::path::Path;
use std::time::Instant;

use sha2::{Digest, Sha256};

use crate::storage::chunk::{
    Schema7ChunkPrefixExpectation, decode_chunk_record_with_layout, verify_schema7_indexed_prefix,
};
use crate::storage::index::SegmentIndexReadAt;

use super::super::footer::{
    read_segment_footer_for_schema6, read_segment_footer_for_schema7,
    read_segment_footer_for_schema8, validate_manifest_segment_meta,
};
use super::super::full_validation::{
    RegisteredSegmentValidationPolicy, preflight_registered_segment,
    registered_validation_error_to_io,
};
use super::super::metadata_facade::{
    Schema7MetadataOpenContext, SegmentChunkAuthentication, SegmentMetadataLayout,
    SegmentMetadataReader, SegmentMetadataVisitControl, SegmentMetadataVisitError,
};
use super::super::query_reader::{open_metadata_runtime, register_segment_metadata};
use super::super::{
    MetadataGovernorConfig, SegmentFile, SegmentMeta, SegmentStorageSchema, invalid_segment_data,
    read_manifest_inventory,
};
use super::VERIFIED_SELECTION_FINGERPRINT_DOMAIN;
use super::fingerprint::{
    DecodedSemanticAccumulator, ExactPostingsAccumulator,
    TopologyIndependentDecodedSemanticAccumulator,
};
use super::helpers::{
    checked_add, chunk_kind_id, chunk_sample_count, evenly_spaced_series_refs, facade_io,
    footer_file_len, hash_bytes, hash_u32, hash_u64, hex_digest,
};
use super::inventory::ChunkInventoryAccumulator;
use super::report::ExperimentalStorageVerification;

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
        false,
    )
}

/// Performs the exhaustive exact-postings gate and additionally fingerprints
/// every decoded logical sample independently of physical topology.
pub fn verify_experimental_storage_corpus_with_decoded_semantics(
    segments_dir: impl AsRef<Path>,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
) -> io::Result<ExperimentalStorageVerification> {
    verify_experimental_storage_corpus_impl(
        segments_dir.as_ref(),
        schema,
        validate_segment_footers,
        None,
        true,
        true,
    )
}

fn verify_experimental_storage_corpus_impl(
    segments_dir: &Path,
    schema: SegmentStorageSchema,
    validate_segment_footers: bool,
    series_sample_per_segment: Option<u32>,
    verify_exact_postings: bool,
    fingerprint_topology_independent_semantics: bool,
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
    let mut decoded_semantics =
        DecodedSemanticAccumulator::new(segment_count, series_sample_per_segment);
    let mut chunk_inventory = ChunkInventoryAccumulator::default();
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
    let mut topology_independent_semantics = fingerprint_topology_independent_semantics
        .then(TopologyIndependentDecodedSemanticAccumulator::new);
    let mut topology_independent_value_buffer = Vec::new();

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
            let registered = register_segment_metadata(&runtime, &segment_dir, &footer)?;
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
        decoded_semantics.start_segment(
            &manifest_segment.segment_id,
            manifest_segment.start_ms,
            manifest_segment.end_ms,
            selected_series,
        )?;
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
                decoded_semantics.start_series(
                    series.series_id(),
                    series.kind_mask(),
                    series.labels(),
                )?;
                let topology_independent_series_digest = topology_independent_semantics
                    .as_ref()
                    .map(|_| {
                        TopologyIndependentDecodedSemanticAccumulator::series_digest(
                            series.labels(),
                        )
                    })
                    .transpose()?;

                let mut canonical = Vec::new();
                canonical
                    .try_reserve_exact(series.chunks().len())
                    .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
                let mut observed_kind_mask = 0u8;
                let mut semantic_sample_count = 0u64;
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

                    let (decoded, layout) = decode_chunk_record_with_layout(&chunk_buffer)?;
                    if decoded.series_ref != locator.series_ref()
                        || decoded.kind != locator.kind()
                        || decoded.min_time_ms != locator.min_time_ms()
                        || decoded.max_time_ms != locator.max_time_ms()
                    {
                        return Err(invalid_segment_data(
                            "decoded chunk header disagrees with its metadata locator",
                        ));
                    }
                    if layout.flags != authenticated_flags
                        || layout.scalar_lane_bytes != locator.scalar_lane_len()
                    {
                        return Err(invalid_segment_data(
                            "decoded chunk layout disagrees with its authenticated locator",
                        ));
                    }
                    if locator.min_time_ms() < manifest_segment.start_ms
                        || locator.max_time_ms() >= manifest_segment.end_ms
                    {
                        return Err(invalid_segment_data(
                            "chunk time range lies outside its segment",
                        ));
                    }
                    if let (Some(accumulator), Some(series_digest)) = (
                        topology_independent_semantics.as_mut(),
                        topology_independent_series_digest.as_ref(),
                    ) {
                        accumulator.observe_samples(
                            series_digest,
                            &decoded.samples,
                            &mut topology_independent_value_buffer,
                        )?;
                    }
                    let kind = chunk_kind_id(locator.kind());
                    observed_kind_mask |= 1u8 << kind;
                    chunks_by_kind[usize::from(kind)] =
                        chunks_by_kind[usize::from(kind)].saturating_add(1);
                    total_samples =
                        total_samples.saturating_add(chunk_sample_count(&decoded.samples));
                    total_chunk_bytes =
                        total_chunk_bytes.saturating_add(u64::from(locator.chunk_len()));
                    chunk_inventory.observe(
                        &layout,
                        locator.chunk_len(),
                        decoded.min_time_ms,
                        decoded.max_time_ms,
                        &decoded.samples,
                    )?;
                    checked_add(
                        &mut semantic_sample_count,
                        decoded_semantics.observe_chunk(locator.file_id(), &decoded.samples)?,
                        "semantic series sample count",
                    )?;
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
                decoded_semantics.finish_series(semantic_sample_count)?;
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
        decoded_semantic_fingerprint: decoded_semantics.finish(),
        topology_independent_decoded_semantic_fingerprint: topology_independent_semantics
            .map(TopologyIndependentDecodedSemanticAccumulator::finish),
        segments: u64::from(segment_count),
        corpus_series,
        series: total_series,
        chunks: total_chunks,
        chunks_by_kind,
        samples: total_samples,
        logical_chunk_bytes: total_chunk_bytes,
        chunk_inventory: chunk_inventory.finish(),
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
