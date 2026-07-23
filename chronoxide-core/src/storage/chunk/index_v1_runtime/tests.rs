use std::fs;

use tempfile::TempDir;

use crate::storage::metadata_governor::{MetadataGovernorConfig, MetadataUsageClass};
use crate::storage::metadata_runtime::{
    MetadataIssuedReadCount, SegmentArtifactRegistration, StoreMetadataRuntime,
};
use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;

use super::*;
use crate::storage::chunk::{CHUNK_HEADER_LEN, ChunkKind, chunk_index_ranges, write_chunk_index};

const CHUNKS_LEN: usize = 4096;
const OOO_CHUNKS_LEN: usize = 2048;

struct Fixture {
    _directory: TempDir,
    runtime: StoreMetadataRuntime,
    registered: Option<RegisteredSegment>,
    ranges: Vec<ChunkIndexRange>,
    chunk_index_path: std::path::PathBuf,
}

fn runtime(retained_max_bytes: u64, in_flight_max_bytes: u64) -> StoreMetadataRuntime {
    StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes,
        in_flight_max_bytes,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .expect("valid schema-6 chunk-index test runtime")
}

fn entry(file_id: u8, time_ms: u64, offset: u64) -> ChunkIndexEntry {
    ChunkIndexEntry {
        file_id,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms: time_ms,
        max_time_ms: time_ms + 1,
        offset,
        length: CHUNK_HEADER_LEN as u32,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    }
}

fn default_entries() -> Vec<Vec<ChunkIndexEntry>> {
    let mut scalar = entry(0, 100, 64);
    scalar.flags = 0xa5a5;
    let mut typed = entry(0, 200, 128);
    typed.kind = ChunkKind::Histogram;
    typed.flags = 0x5a5a;
    vec![vec![scalar, typed], vec![entry(1, 300, 256)]]
}

fn encoded_index(entries: &[Vec<ChunkIndexEntry>]) -> (Vec<u8>, Vec<ChunkIndexRange>) {
    let ranges = chunk_index_ranges(entries).expect("compute schema-6 chunk-index ranges");
    let mut bytes = Vec::new();
    write_chunk_index(&mut bytes, entries).expect("encode schema-6 chunk index");
    (bytes, ranges)
}

fn fixture(
    identity: &str,
    runtime: StoreMetadataRuntime,
    chunk_index: Vec<u8>,
    ranges: Vec<ChunkIndexRange>,
) -> Fixture {
    let directory = TempDir::new().expect("create schema-6 chunk-index fixture directory");
    let mut chunk_index_path = None;
    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let path = directory.path().join(file.filename());
        match file {
            SegmentFile::MetaJson => fs::write(&path, b"{}").expect("write meta fixture"),
            SegmentFile::Symbols => fs::write(&path, b"symbols").expect("write symbols fixture"),
            SegmentFile::Series => fs::write(&path, b"series").expect("write series fixture"),
            SegmentFile::Chunks => {
                fs::write(&path, vec![0; CHUNKS_LEN]).expect("write chunks fixture")
            }
            SegmentFile::OooChunks => {
                fs::write(&path, vec![0; OOO_CHUNKS_LEN]).expect("write OOO fixture")
            }
            SegmentFile::ChunkIndex => {
                fs::write(&path, &chunk_index).expect("write chunk-index fixture");
                chunk_index_path = Some(path.clone());
            }
            SegmentFile::Indexes => fs::write(&path, b"indexes").expect("write indexes fixture"),
            SegmentFile::Footer => unreachable!("footer is not runtime-inventoried"),
        }
        let len = fs::metadata(&path).expect("stat fixture artifact").len();
        SegmentArtifactRegistration::new(file, path, len)
    });
    let registered = runtime
        .register_segment(identity, &artifacts)
        .expect("register schema-6 chunk-index fixture");
    Fixture {
        _directory: directory,
        runtime,
        registered: Some(registered),
        ranges,
        chunk_index_path: chunk_index_path.expect("chunk-index path captured"),
    }
}

fn standard_fixture(identity: &str, retained_max_bytes: u64, in_flight_max_bytes: u64) -> Fixture {
    let entries = default_entries();
    let (bytes, ranges) = encoded_index(&entries);
    fixture(
        identity,
        runtime(retained_max_bytes, in_flight_max_bytes),
        bytes,
        ranges,
    )
}

fn open_reader(fixture: &Fixture) -> GovernedSchema6ChunkIndexReader {
    GovernedSchema6ChunkIndexReader::open(
        fixture.registered.as_ref().expect("fixture owner exists"),
        fixture.ranges.len() as u32,
    )
    .expect("open governed schema-6 chunk-index reader")
}

fn class_reads(
    runtime: &StoreMetadataRuntime,
    class: MetadataCacheClass,
) -> MetadataIssuedReadCount {
    runtime.snapshot().reads.classes[class.stable_index()].issued
}

fn delta(
    after: MetadataIssuedReadCount,
    before: MetadataIssuedReadCount,
) -> MetadataIssuedReadCount {
    MetadataIssuedReadCount {
        calls: after.calls - before.calls,
        bytes: after.bytes - before.bytes,
    }
}

#[test]
fn root_decoder_is_exact_and_strict() {
    let entries = default_entries();
    let (bytes, ranges) = encoded_index(&entries);
    let root_bytes = &bytes[..CHUNK_INDEX_ROOT_V1_LEN];
    let root = decode_schema6_chunk_index_root_v1(root_bytes, bytes.len() as u64)
        .expect("decode valid schema-6 root");
    assert_eq!(root.num_series, 2);
    assert_eq!(root.data_start, 36);
    assert_eq!(root.file_len, bytes.len() as u64);

    for (offset, replacement) in [
        (0, 0_u32.to_le_bytes().to_vec()),
        (4, 2_u16.to_le_bytes().to_vec()),
        (6, 1_u16.to_le_bytes().to_vec()),
        (12, 35_u64.to_le_bytes().to_vec()),
    ] {
        let mut malformed = root_bytes.to_vec();
        malformed[offset..offset + replacement.len()].copy_from_slice(&replacement);
        assert!(
            decode_schema6_chunk_index_root_v1(&malformed, bytes.len() as u64).is_err(),
            "root mutation at byte {offset} must fail"
        );
    }
    assert!(decode_schema6_chunk_index_root_v1(&root_bytes[..19], bytes.len() as u64).is_err());
    assert!(decode_schema6_chunk_index_root_v1(root_bytes, bytes.len() as u64 - 1).is_err());
    assert!(decode_schema6_chunk_index_root_v1(root_bytes, 19).is_err());

    let (empty, empty_ranges) = encoded_index(&[]);
    assert!(empty_ranges.is_empty());
    assert_eq!(empty.len(), CHUNK_INDEX_ROOT_V1_LEN);
    let empty_root = decode_schema6_chunk_index_root_v1(&empty, empty.len() as u64)
        .expect("decode canonical empty schema-6 chunk index");
    assert_eq!(empty_root.num_series, 0);
    assert_eq!(empty_root.data_start, CHUNK_INDEX_ROOT_V1_LEN as u64);
    let mut empty_with_body = empty.clone();
    empty_with_body.extend_from_slice(&[0; CHUNK_ENTRY_LEN]);
    assert!(
        decode_schema6_chunk_index_root_v1(
            &empty_with_body[..CHUNK_INDEX_ROOT_V1_LEN],
            empty_with_body.len() as u64,
        )
        .is_err()
    );

    let pair_offset = usize::try_from(
        schema6_directory_pair_offset(&root, 1).expect("compute second directory pair"),
    )
    .expect("directory pair offset fits usize");
    let pair_bytes = &bytes[pair_offset..pair_offset + 16];
    let pair = decode_schema6_chunk_index_directory_pair(pair_bytes, root, 1)
        .expect("decode exact second directory pair");
    assert_eq!(pair.series_ref, 1);
    assert_eq!(pair.range, ranges[1]);
    assert_eq!(pair.entry_count, 1);

    let truncated = decode_schema6_chunk_index_directory_pair(&pair_bytes[..15], root, 1)
        .expect_err("truncated directory pair must fail");
    assert_eq!(truncated.kind(), io::ErrorKind::UnexpectedEof);
    let mut trailing = pair_bytes.to_vec();
    trailing.push(0);
    let trailing = decode_schema6_chunk_index_directory_pair(&trailing, root, 1)
        .expect_err("directory pair with trailing bytes must fail");
    assert_eq!(trailing.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn exact_root_and_series_spans_are_cached_and_reused() {
    let fixture = standard_fixture("schema6-cached-spans", 1024 * 1024, 1024 * 1024);
    let before_open = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
    let reader = open_reader(&fixture);
    assert_eq!(reader.segment_identity(), "schema6-cached-spans");
    assert_eq!(
        delta(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
            before_open
        ),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: CHUNK_INDEX_ROOT_V1_LEN as u64,
        }
    );

    let session = reader.query_session().expect("open schema-6 query session");
    let root = session.load_root().expect("reuse governed root");
    assert_eq!(root.data_start, 36);
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    session
        .validate_series_range(&root, 0, fixture.ranges[0])
        .expect("validate authoritative range without reading its body");
    let after_validation_directory =
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(
        delta(after_validation_directory, before_directory),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
        }
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_span
    );

    let first = session
        .read_series_entries(&root, 0, fixture.ranges[0])
        .expect("read exact first-series span");
    assert_eq!(first.series_ref(), 0);
    let first_locators = session
        .locators(&first)
        .expect("consume first locators through their owning session");
    assert_eq!(first_locators.len(), 2);
    assert_eq!(first_locators[0].payload_identity(), (0, 64, 40));
    assert_eq!(first_locators[1].payload_identity(), (0, 128, 40));
    assert_eq!(first_locators[0].entry().kind, ChunkKind::Float);
    assert_eq!(first_locators[0].entry().flags, 0xa5a5);
    assert_eq!(first_locators[1].entry().kind, ChunkKind::Histogram);
    assert_eq!(first_locators[1].entry().flags, 0x5a5a);
    assert!(first.charged_bytes() > 0);
    let after_first_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(after_first_directory, after_validation_directory);
    let after_first = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(
        delta(after_first, before_span),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: u64::from(fixture.ranges[0].len),
        }
    );

    let second = session
        .read_series_entries(&root, 0, fixture.ranges[0])
        .expect("reuse exact first-series span");
    assert_eq!(
        session
            .locators(&second)
            .expect("consume reused locators through their owning session"),
        first_locators
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        after_first_directory
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after_first
    );
}

#[test]
fn zero_retention_releases_directory_and_span_pins_and_reissues_exact_reads() {
    let fixture = standard_fixture("schema6-zero-retention", 0, 1024 * 1024);
    let reader = open_reader(&fixture);
    let session = reader.query_session().expect("open schema-6 query session");
    let root = session.load_root().expect("load query-local root");
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    {
        let first = session
            .read_series_entries(&root, 1, fixture.ranges[1])
            .expect("read transient span");
        assert_eq!(
            session
                .locators(&first)
                .expect("consume transient locators through their owning session")[0]
                .payload_identity(),
            (1, 256, 40)
        );
    }
    let middle_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(
        delta(middle_directory, before_directory),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
        }
    );
    let middle_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(
        delta(middle_span, before_span),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: u64::from(fixture.ranges[1].len),
        }
    );
    let second = session
        .read_series_entries(&root, 1, fixture.ranges[1])
        .expect("reload released transient span");
    assert_eq!(
        session
            .locators(&second)
            .expect("consume reloaded locators through their owning session")
            .len(),
        1
    );
    assert_eq!(
        delta(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
            middle_directory,
        ),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
        }
    );
    assert_eq!(
        delta(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            middle_span
        ),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: u64::from(fixture.ranges[1].len),
        }
    );
}

#[test]
fn tiny_budget_refusals_before_directory_and_body_io_are_retryable() {
    let fixture = standard_fixture("schema6-budget", 1024 * 1024, 4096);
    let reader = open_reader(&fixture);
    let session = reader.query_session().expect("open schema-6 query session");
    let root = session.load_root().expect("load cached root");
    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(4096, MetadataUsageClass::Scratch)
        .expect("reserve competing in-flight bytes");
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let error = session
        .read_series_entries(&root, 0, fixture.ranges[0])
        .expect_err("tiny budget must refuse directory pair before I/O");
    assert!(matches!(
        error,
        Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        before_directory
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_span
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    drop(blocker);
    session
        .validate_series_range(&root, 0, fixture.ranges[0])
        .expect("load authoritative directory pair after budget is released");
    let cached_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(delta(cached_directory, before_directory).calls, 1);

    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(3000, MetadataUsageClass::Scratch)
        .expect("reserve competing in-flight bytes after caching the directory pair");
    let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let error = session
        .read_series_entries(&root, 0, fixture.ranges[0])
        .expect_err("tiny budget must refuse locator/body work before body I/O");
    assert!(matches!(
        error,
        Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        cached_directory
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_span
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    drop(blocker);
    let retried = session
        .read_series_entries(&root, 0, fixture.ranges[0])
        .expect("budget refusal must be retryable");
    assert_eq!(
        session
            .locators(&retried)
            .expect("consume retried locators through their owning session")
            .len(),
        2
    );
}

#[test]
fn authoritative_directory_rejects_aligned_swapped_and_shifted_ranges_before_body_io() {
    {
        let fixture = standard_fixture("schema6-swapped-range", 1024 * 1024, 1024 * 1024);
        let reader = open_reader(&fixture);
        let session = reader.query_session().expect("open swapped-range session");
        let root = session.load_root().expect("load swapped-range root");
        let swapped = fixture.ranges[1];
        assert!(validate_schema6_series_range(&root, 0, swapped).is_ok());
        let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

        let error = session
            .read_series_entries(&root, 0, swapped)
            .expect_err("locally valid swapped range must disagree with the directory");
        assert!(matches!(
            error,
            Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        let after_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        assert_eq!(
            delta(after_directory, before_directory),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
            }
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            before_span
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

        fixture.runtime.evict_all_resident_metadata();
        assert!(session.read_series_entries(&root, 0, swapped).is_err());
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
            after_directory
        );
    }

    {
        let fixture = standard_fixture("schema6-shifted-range", 1024 * 1024, 1024 * 1024);
        let reader = open_reader(&fixture);
        let session = reader.query_session().expect("open shifted-range session");
        let root = session.load_root().expect("load shifted-range root");
        let shifted = ChunkIndexRange {
            offset: fixture.ranges[0].offset + CHUNK_ENTRY_LEN as u64,
            len: CHUNK_ENTRY_LEN as u32,
        };
        assert!(validate_schema6_series_range(&root, 0, shifted).is_ok());
        let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
        let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

        let error = session
            .read_series_entries(&root, 0, shifted)
            .expect_err("locally valid shifted range must disagree with the directory");
        assert!(matches!(
            error,
            Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
        ));
        assert_eq!(
            delta(
                class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
                before_directory,
            ),
            MetadataIssuedReadCount {
                calls: 1,
                bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
            }
        );
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
            before_span
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    }
}

#[test]
fn malformed_directory_ordering_is_sticky_and_never_reads_the_body() {
    let entries = default_entries();
    let (mut bytes, ranges) = encoded_index(&entries);
    bytes[20..28].copy_from_slice(&(ranges[0].offset - 1).to_le_bytes());
    let fixture = fixture(
        "schema6-sticky-directory-ordering",
        runtime(0, 1024 * 1024),
        bytes,
        ranges,
    );
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open malformed-directory session");
    let root = session.load_root().expect("load malformed-directory root");
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_span = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

    let error = session
        .read_series_entries(&root, 0, fixture.ranges[0])
        .expect_err("out-of-order directory pair must fail");
    assert!(matches!(
        error,
        Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(
        delta(after_directory, before_directory),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: CHUNK_INDEX_DIRECTORY_PAIR_LEN,
        }
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_span
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    fixture.runtime.evict_all_resident_metadata();
    assert!(
        session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .is_err()
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        after_directory
    );
}

#[test]
fn root_count_range_and_entry_validation_are_strict() {
    let fixture = standard_fixture("schema6-strict", 1024 * 1024, 1024 * 1024);
    let count_error = GovernedSchema6ChunkIndexReader::open(
        fixture.registered.as_ref().expect("fixture owner exists"),
        3,
    )
    .err()
    .expect("cross-root series count mismatch must fail");
    assert!(matches!(
        count_error,
        Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));

    let entries = default_entries();
    let (bytes, ranges) = encoded_index(&entries);
    let root =
        decode_schema6_chunk_index_root_v1(&bytes[..CHUNK_INDEX_ROOT_V1_LEN], bytes.len() as u64)
            .expect("decode valid root");
    assert!(
        validate_schema6_series_range(
            &root,
            0,
            ChunkIndexRange {
                offset: ranges[0].offset + 1,
                len: ranges[0].len,
            },
        )
        .is_err()
    );
    assert!(
        validate_schema6_series_range(
            &root,
            0,
            ChunkIndexRange {
                offset: ranges[0].offset,
                len: ranges[0].len - 1,
            },
        )
        .is_err()
    );

    let mut invalid_file = bytes
        [ranges[0].offset as usize..(ranges[0].offset + u64::from(ranges[0].len)) as usize]
        .to_vec();
    assert!(
        decode_schema6_chunk_index_span(
            invalid_file.clone(),
            1,
            [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],
        )
        .is_err()
    );
    invalid_file[0] = 2;
    assert!(
        decode_schema6_chunk_index_span(
            invalid_file,
            2,
            [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],
        )
        .is_err()
    );

    let mut out_of_bounds = bytes
        [ranges[0].offset as usize..(ranges[0].offset + u64::from(ranges[0].len)) as usize]
        .to_vec();
    out_of_bounds[20..28].copy_from_slice(&(CHUNKS_LEN as u64).to_le_bytes());
    assert!(
        decode_schema6_chunk_index_span(
            out_of_bounds,
            2,
            [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],
        )
        .is_err()
    );

    let mut reversed = bytes
        [ranges[0].offset as usize..(ranges[0].offset + u64::from(ranges[0].len)) as usize]
        .to_vec();
    let (first, second) = reversed.split_at_mut(CHUNK_ENTRY_LEN);
    first.swap_with_slice(second);
    assert!(
        decode_schema6_chunk_index_span(reversed, 2, [CHUNKS_LEN as u64, OOO_CHUNKS_LEN as u64],)
            .is_err()
    );
}

#[test]
fn touched_corruption_and_truncation_are_sticky() {
    let entries = default_entries();
    let (mut bytes, ranges) = encoded_index(&entries);
    bytes[ranges[0].offset as usize] = 2;
    let fixture = fixture(
        "schema6-sticky-corruption",
        runtime(0, 1024 * 1024),
        bytes,
        ranges,
    );
    let reader = open_reader(&fixture);
    let session = reader.query_session().expect("open query session");
    let root = session.load_root().expect("load root");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert!(
        session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .is_err()
    );
    let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(delta(after, before).calls, 1);
    fixture.runtime.evict_all_resident_metadata();
    assert!(
        session
            .read_series_entries(&root, 0, fixture.ranges[0])
            .is_err()
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    let truncation = standard_fixture("schema6-sticky-truncation", 0, 1024 * 1024);
    let truncation_reader = open_reader(&truncation);
    let truncation_session = truncation_reader
        .query_session()
        .expect("open truncation session");
    let truncation_root = truncation_session
        .load_root()
        .expect("load truncation root");
    let file_len = fs::metadata(&truncation.chunk_index_path)
        .expect("stat chunk index")
        .len();
    fs::OpenOptions::new()
        .write(true)
        .open(&truncation.chunk_index_path)
        .expect("open chunk index for truncation")
        .set_len(file_len - 1)
        .expect("truncate chunk-index fixture");
    assert!(
        truncation_session
            .read_series_entries(&truncation_root, 0, truncation.ranges[0])
            .is_err()
    );
    assert_eq!(truncation.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn reader_owner_and_query_guard_have_explicit_lifetimes() {
    let mut fixture = standard_fixture("schema6-owner", 0, 1024 * 1024);
    let reader = open_reader(&fixture);
    drop(fixture.registered.take());
    assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 7);

    let session = reader.query_session().expect("open guarded session");
    let root = session.load_root().expect("load guarded root");
    let locators = session
        .read_series_entries(&root, 0, fixture.ranges[0])
        .expect("load guarded locators");
    assert_eq!(
        session
            .locators(&locators)
            .expect("locators retain matching provenance")
            .len(),
        2
    );
    drop(reader);
    assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 7);

    drop(locators);
    drop(root);
    drop(session);
    assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 0);
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn chunk_index_session_rejects_a_foreign_generation_without_io_or_poisoning() {
    let shared_runtime = runtime(0, 1024 * 1024);
    let entries = default_entries();
    let (first_bytes, first_ranges) = encoded_index(&entries);
    let first = fixture(
        "schema6-session-generation-first",
        shared_runtime.clone(),
        first_bytes,
        first_ranges,
    );
    let (second_bytes, second_ranges) = encoded_index(&entries);
    let second = fixture(
        "schema6-session-generation-second",
        shared_runtime,
        second_bytes,
        second_ranges,
    );
    let reader = open_reader(&first);
    let session = reader.query_session().expect("open first query session");
    let own_guard = first
        .registered
        .as_ref()
        .expect("first fixture owner exists")
        .read_guard()
        .expect("open own read guard");
    let foreign_guard = second
        .registered
        .as_ref()
        .expect("second fixture owner exists")
        .read_guard()
        .expect("open foreign read guard");
    let before_root = class_reads(&first.runtime, MetadataCacheClass::IndexRoot);
    let before_directory = class_reads(&first.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&first.runtime, MetadataCacheClass::IndexPage);

    session
        .ensure_same_generation(&own_guard)
        .expect("own generation must match");
    assert!(matches!(
        session.ensure_same_generation(&foreign_guard),
        Err(Schema6ChunkIndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(
        class_reads(&first.runtime, MetadataCacheClass::IndexRoot),
        before_root
    );
    assert_eq!(
        class_reads(&first.runtime, MetadataCacheClass::IndexDirectory),
        before_directory
    );
    assert_eq!(
        class_reads(&first.runtime, MetadataCacheClass::IndexPage),
        before_page
    );
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn locator_provenance_rejects_another_segment_generation() {
    let shared_runtime = runtime(0, 1024 * 1024);
    let entries = default_entries();
    let (first_bytes, first_ranges) = encoded_index(&entries);
    let first = fixture(
        "schema6-provenance-first",
        shared_runtime.clone(),
        first_bytes,
        first_ranges,
    );
    let (second_bytes, second_ranges) = encoded_index(&entries);
    let second = fixture(
        "schema6-provenance-second",
        shared_runtime,
        second_bytes,
        second_ranges,
    );

    let first_reader = open_reader(&first);
    let first_session = first_reader.query_session().expect("open first session");
    let first_root = first_session.load_root().expect("load first root");
    let locators = first_session
        .read_series_entries(&first_root, 0, first.ranges[0])
        .expect("read first locators");

    let second_reader = open_reader(&second);
    let second_session = second_reader.query_session().expect("open second session");
    assert!(matches!(
        second_session.locators(&locators),
        Err(Schema6ChunkIndexReaderError::ForeignSegmentGeneration)
    ));
}
