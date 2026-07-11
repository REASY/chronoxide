#[allow(dead_code)]
#[path = "support/promql_range_scalar_cache.rs"]
mod support;

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::Mutex;
use std::time::Duration;

use chronoxide_core::labels::{METRIC_NAME_LABEL, SeriesRef};
use chronoxide_core::promql::PromqlQueryError;
use chronoxide_core::storage::chunk::{ChunkIndexEntry, read_chunk_index, write_chunk_index};
use chronoxide_core::storage::head::{HistogramValue, TypedSampleMetadata};
use chronoxide_core::storage::segment::{
    RangeScalarCacheSummary, SegmentFile, SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
    range_scalar_cache_governor_stats,
};
use support::{
    TypedRangeFixture, build_error_oracle_document_with_session_budget, deterministic_segment_dirs,
    write_stale_reset_delta_fixture,
};

const MIB: u64 = 1024 * 1024;
const CHUNK_HEADER_LEN: u32 = 40;
const TYPED_SCALAR_LANE_HEADER_LEN: usize = 16;
const ERROR_ARTIFACT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../docs/superpowers/benchmarks/2026-07-10-promql-range-scalar-cache-errors-v1.json"
);

static CACHE_ERROR_TEST_LOCK: Mutex<()> = Mutex::new(());

#[derive(Debug, Clone, Copy)]
enum ScalarLaneCorruption {
    MissingOffset,
    MissingLength,
    OffsetIntoChunkHeader,
    OffsetOverflow,
    RangeExceedsChunkLength,
    DeclaredHeaderTruncation,
    PhysicalTruncation,
    Magic,
    Version,
    BodyLength,
    BodyCrc,
    TrailingBytes,
    FallbackFullRecordCrc,
}

#[derive(Debug)]
struct LocatedScalarLane {
    segment_dir: PathBuf,
    entries: Vec<Vec<ChunkIndexEntry>>,
    series_index: usize,
    entry_index: usize,
    entry: ChunkIndexEntry,
}

fn locate_scalar_lane(fixture: &TypedRangeFixture) -> LocatedScalarLane {
    for segment_dir in deterministic_segment_dirs(fixture.path()) {
        let mut index_file =
            File::open(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
        let entries = read_chunk_index(&mut index_file).unwrap();
        for (series_index, series_entries) in entries.iter().enumerate() {
            if let Some(entry_index) = series_entries.iter().position(|entry| {
                entry.scalar_lane_offset > 0
                    && entry.scalar_lane_len > TYPED_SCALAR_LANE_HEADER_LEN as u32
            }) {
                return LocatedScalarLane {
                    segment_dir,
                    entry: series_entries[entry_index].clone(),
                    entries,
                    series_index,
                    entry_index,
                };
            }
        }
    }
    panic!("fixture must contain a dedicated scalar lane");
}

fn overwrite_bytes(path: &std::path::Path, offset: u64, bytes: &[u8]) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)
        .unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
}

fn read_bytes(path: &std::path::Path, offset: u64, len: usize) -> Vec<u8> {
    let mut file = File::open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    let mut bytes = vec![0; len];
    file.read_exact(&mut bytes).unwrap();
    bytes
}

fn rewrite_chunk_index(located: &LocatedScalarLane, entries: &[Vec<ChunkIndexEntry>]) {
    let index_path = located.segment_dir.join(SegmentFile::ChunkIndex.filename());
    write_chunk_index(File::create(index_path).unwrap(), entries).unwrap();
}

fn apply_scalar_lane_corruption(
    fixture: &TypedRangeFixture,
    corruption: ScalarLaneCorruption,
) -> ChunkIndexEntry {
    let located = locate_scalar_lane(fixture);
    let chunks_path = located.segment_dir.join(SegmentFile::Chunks.filename());
    let lane_offset = located
        .entry
        .offset
        .checked_add(u64::from(located.entry.scalar_lane_offset))
        .unwrap();
    let mut entries = located.entries.clone();
    let entry = &mut entries[located.series_index][located.entry_index];
    let mut rewrite_index = false;

    match corruption {
        ScalarLaneCorruption::MissingOffset => {
            entry.scalar_lane_offset = 0;
            rewrite_index = true;
        }
        ScalarLaneCorruption::MissingLength => {
            entry.scalar_lane_len = 0;
            rewrite_index = true;
        }
        ScalarLaneCorruption::OffsetIntoChunkHeader => {
            entry.scalar_lane_offset = CHUNK_HEADER_LEN - 1;
            rewrite_index = true;
        }
        ScalarLaneCorruption::OffsetOverflow => {
            entry.scalar_lane_offset = u32::MAX;
            entry.scalar_lane_len = 2;
            rewrite_index = true;
        }
        ScalarLaneCorruption::RangeExceedsChunkLength => {
            entry.scalar_lane_offset = entry.length;
            entry.scalar_lane_len = 1;
            rewrite_index = true;
        }
        ScalarLaneCorruption::DeclaredHeaderTruncation => {
            entry.scalar_lane_len = (TYPED_SCALAR_LANE_HEADER_LEN - 1) as u32;
            rewrite_index = true;
        }
        ScalarLaneCorruption::PhysicalTruncation => {
            let truncated_len = located
                .entry
                .offset
                .checked_add(u64::from(located.entry.scalar_projection_read_len()))
                .and_then(|end| end.checked_sub(1))
                .unwrap();
            OpenOptions::new()
                .write(true)
                .open(&chunks_path)
                .unwrap()
                .set_len(truncated_len)
                .unwrap();
        }
        ScalarLaneCorruption::Magic => {
            overwrite_bytes(&chunks_path, lane_offset, &[0, 0, 0, 0]);
        }
        ScalarLaneCorruption::Version => {
            overwrite_bytes(&chunks_path, lane_offset + 4, &2_u16.to_le_bytes());
        }
        ScalarLaneCorruption::BodyLength => {
            let body_len = u32::from_le_bytes(
                read_bytes(&chunks_path, lane_offset + 8, 4)
                    .try_into()
                    .unwrap(),
            );
            overwrite_bytes(
                &chunks_path,
                lane_offset + 8,
                &body_len.checked_add(1).unwrap().to_le_bytes(),
            );
        }
        ScalarLaneCorruption::BodyCrc => {
            let byte_offset = lane_offset + TYPED_SCALAR_LANE_HEADER_LEN as u64;
            let mut byte = read_bytes(&chunks_path, byte_offset, 1);
            byte[0] ^= 0x80;
            overwrite_bytes(&chunks_path, byte_offset, &byte);
        }
        ScalarLaneCorruption::TrailingBytes => {
            let mut lane = read_bytes(
                &chunks_path,
                lane_offset,
                located.entry.scalar_lane_len as usize,
            );
            lane.push(0);
            let body_len = u32::try_from(lane.len() - TYPED_SCALAR_LANE_HEADER_LEN).unwrap();
            lane[8..12].copy_from_slice(&body_len.to_le_bytes());
            let body_crc = crc32c::crc32c(&lane[TYPED_SCALAR_LANE_HEADER_LEN..]);
            lane[12..16].copy_from_slice(&body_crc.to_le_bytes());
            overwrite_bytes(&chunks_path, lane_offset, &lane);
            entry.scalar_lane_len = entry.scalar_lane_len.checked_add(1).unwrap();
            rewrite_index = true;
        }
        ScalarLaneCorruption::FallbackFullRecordCrc => {
            entry.scalar_lane_offset = 0;
            entry.scalar_lane_len = 0;
            rewrite_index = true;
            let byte_offset = located
                .entry
                .offset
                .checked_add(u64::from(located.entry.length))
                .and_then(|end| end.checked_sub(1))
                .unwrap();
            let mut byte = read_bytes(&chunks_path, byte_offset, 1);
            byte[0] ^= 0x80;
            overwrite_bytes(&chunks_path, byte_offset, &byte);
        }
    }
    if rewrite_index {
        rewrite_chunk_index(&located, &entries);
    }
    located.entry
}

fn range_error_and_summary(
    fixture: &TypedRangeFixture,
    entry: &ChunkIndexEntry,
    cache_budget_bytes: u64,
) -> (PromqlQueryError, RangeScalarCacheSummary) {
    let mut session = fixture.store.query_session().unwrap();
    session
        .set_range_scalar_cache_budget_bytes(cache_budget_bytes)
        .unwrap();
    let step_ms = entry.max_time_ms.saturating_sub(entry.min_time_ms).max(1);
    let error = session
        .query_promql_range("cache_count", entry.min_time_ms, entry.max_time_ms, step_ms)
        .unwrap_err();
    let summary = session.last_range_scalar_cache_summary().copied().unwrap();
    (error, summary)
}

fn write_two_chunk_hit_precedence_fixture() -> TypedRangeFixture {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(60))
            .with_deterministic_segment_ids(0x0ca5_e009),
    )
    .unwrap();
    for (timestamp_ms, count) in [(1_000, 1), (2_000, 2)] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(
                    timestamp_ms,
                    HistogramValue {
                        count,
                        sum: Some(count as f64),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![count, 0],
                    },
                )],
                |visit| visit(METRIC_NAME_LABEL, "hit_precedence"),
            )
            .unwrap();
    }
    writer.flush().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    TypedRangeFixture { tempdir, store }
}

#[test]
fn task4_error_oracle_rows_match_with_cache_disabled_and_enabled() {
    let _guard = CACHE_ERROR_TEST_LOCK.lock().unwrap();
    let expected: support::ErrorOracleDocument =
        serde_json::from_slice(&fs::read(ERROR_ARTIFACT).unwrap()).unwrap();

    let cache_off = build_error_oracle_document_with_session_budget(0);
    let cache_on = build_error_oracle_document_with_session_budget(4 * MIB);

    assert_eq!(cache_off.rows.len(), 22);
    assert_eq!(cache_on.rows.len(), 22);
    assert_eq!(cache_off, expected);
    assert_eq!(cache_on, expected);
}

#[test]
fn scalar_lane_corruption_variants_are_exact_with_cache_disabled_and_enabled() {
    let _guard = CACHE_ERROR_TEST_LOCK.lock().unwrap();
    let cases = [
        (
            ScalarLaneCorruption::MissingOffset,
            "chunk scalar lane range is incomplete",
        ),
        (
            ScalarLaneCorruption::MissingLength,
            "chunk scalar lane range is incomplete",
        ),
        (
            ScalarLaneCorruption::OffsetIntoChunkHeader,
            "chunk scalar lane offset points into chunk header",
        ),
        (
            ScalarLaneCorruption::OffsetOverflow,
            "chunk scalar lane range overflow",
        ),
        (
            ScalarLaneCorruption::RangeExceedsChunkLength,
            "chunk scalar lane range exceeds chunk length",
        ),
        (
            ScalarLaneCorruption::DeclaredHeaderTruncation,
            "typed scalar lane header short read",
        ),
        (
            ScalarLaneCorruption::PhysicalTruncation,
            "failed to fill whole buffer",
        ),
        (
            ScalarLaneCorruption::Magic,
            "typed scalar lane magic mismatch",
        ),
        (
            ScalarLaneCorruption::Version,
            "unsupported typed scalar lane version",
        ),
        (
            ScalarLaneCorruption::BodyLength,
            "typed scalar lane body length mismatch",
        ),
        (
            ScalarLaneCorruption::BodyCrc,
            "typed scalar lane crc mismatch",
        ),
        (
            ScalarLaneCorruption::TrailingBytes,
            "typed scalar lane has trailing bytes",
        ),
        (
            ScalarLaneCorruption::FallbackFullRecordCrc,
            "chunk crc mismatch",
        ),
    ];

    for (corruption, expected_message) in cases {
        let fixture = write_stale_reset_delta_fixture();
        let entry = apply_scalar_lane_corruption(&fixture, corruption);
        for cache_budget_bytes in [0, 4 * MIB] {
            let (error, summary) = range_error_and_summary(&fixture, &entry, cache_budget_bytes);
            assert_eq!(
                error,
                PromqlQueryError::Storage(expected_message.to_string()),
                "unexpected {corruption:?} error with budget {cache_budget_bytes}"
            );
            assert_eq!(
                error.to_string(),
                format!("storage query failed: {expected_message}")
            );
            assert_eq!(summary.configured_budget_bytes, cache_budget_bytes);
            assert_eq!(summary.admitted_entries, 0);
            assert_eq!(summary.retained_charge_after_finalize, 0);
            assert!(summary.peak_retained_charge_bytes <= cache_budget_bytes);
        }
        assert_eq!(range_scalar_cache_governor_stats().current_leased_bytes, 0);
    }
}

#[test]
fn later_physical_miss_failure_precedes_processing_an_earlier_cached_hit() {
    let _guard = CACHE_ERROR_TEST_LOCK.lock().unwrap();
    let fixture = write_two_chunk_hit_precedence_fixture();
    let segment_dir = deterministic_segment_dirs(fixture.path())
        .into_iter()
        .next()
        .unwrap();
    let mut index = File::open(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    let entries = read_chunk_index(&mut index)
        .unwrap()
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    assert_eq!(entries.len(), 2);
    let later = &entries[1];
    assert_eq!(later.min_time_ms, 2_000);
    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let truncated_len = later
        .offset
        .checked_add(u64::from(later.scalar_projection_read_len()))
        .and_then(|end| end.checked_sub(1))
        .unwrap();
    OpenOptions::new()
        .write(true)
        .open(chunks_path)
        .unwrap()
        .set_len(truncated_len)
        .unwrap();

    let mut cache_off = fixture.store.query_session().unwrap();
    cache_off.set_range_scalar_cache_budget_bytes(0).unwrap();
    let cache_off_error = cache_off
        .query_promql_range(
            "last_over_time(hit_precedence_count[2s])",
            1_000,
            2_000,
            1_000,
        )
        .unwrap_err();
    let cache_off_summary = cache_off
        .last_range_scalar_cache_summary()
        .copied()
        .unwrap();

    let mut cache_on = fixture.store.query_session().unwrap();
    cache_on
        .set_range_scalar_cache_budget_bytes(4 * MIB)
        .unwrap();
    let cache_on_error = cache_on
        .query_promql_range(
            "last_over_time(hit_precedence_count[2s])",
            1_000,
            2_000,
            1_000,
        )
        .unwrap_err();
    let cache_on_summary = cache_on.last_range_scalar_cache_summary().copied().unwrap();

    let expected = PromqlQueryError::Storage("failed to fill whole buffer".to_string());
    assert_eq!(cache_off_error, expected);
    assert_eq!(cache_on_error, expected);
    assert_eq!(cache_off_summary.hits, 0);
    assert!(
        cache_on_summary.hits > 0,
        "first step must prime the good chunk"
    );
    assert!(cache_on_summary.admitted_entries > 0);
    assert!(cache_on_summary.misses > cache_on_summary.hits);
    assert_eq!(cache_off_summary.retained_charge_after_finalize, 0);
    assert_eq!(cache_on_summary.retained_charge_after_finalize, 0);
    assert_eq!(range_scalar_cache_governor_stats().current_leased_bytes, 0);
}

#[test]
fn corrupted_scalar_lane_errors_match_with_cache_disabled_and_enabled() {
    let _guard = CACHE_ERROR_TEST_LOCK.lock().unwrap();
    let fixture = write_stale_reset_delta_fixture();
    let (segment_dir, entry) = deterministic_segment_dirs(fixture.path())
        .into_iter()
        .find_map(|segment_dir| {
            let mut index_file =
                File::open(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
            let entry = read_chunk_index(&mut index_file)
                .unwrap()
                .into_iter()
                .flatten()
                .find(|entry| entry.scalar_lane_offset > 0 && entry.scalar_lane_len > 16)?;
            Some((segment_dir, entry))
        })
        .expect("fixture must contain a dedicated scalar lane");

    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let original_len = fs::metadata(&chunks_path).unwrap().len();
    let corrupt_offset = entry
        .offset
        .checked_add(u64::from(entry.scalar_lane_offset))
        .and_then(|offset| offset.checked_add(16))
        .unwrap();
    let mut chunks = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&chunks_path)
        .unwrap();
    chunks.seek(SeekFrom::Start(corrupt_offset)).unwrap();
    let mut byte = [0_u8; 1];
    chunks.read_exact(&mut byte).unwrap();
    chunks.seek(SeekFrom::Start(corrupt_offset)).unwrap();
    byte[0] ^= 0x80;
    chunks.write_all(&byte).unwrap();
    chunks.flush().unwrap();
    assert_eq!(fs::metadata(&chunks_path).unwrap().len(), original_len);

    let step_ms = entry.max_time_ms.saturating_sub(entry.min_time_ms).max(1);

    let mut cache_off = fixture.store.query_session().unwrap();
    cache_off.set_range_scalar_cache_budget_bytes(0).unwrap();
    let cache_off_error = cache_off
        .query_promql_range("cache_count", entry.min_time_ms, entry.max_time_ms, step_ms)
        .unwrap_err();
    let cache_off_summary = cache_off
        .last_range_scalar_cache_summary()
        .copied()
        .unwrap();

    let mut cache_on = fixture.store.query_session().unwrap();
    cache_on
        .set_range_scalar_cache_budget_bytes(4 * MIB)
        .unwrap();
    let cache_on_error = cache_on
        .query_promql_range("cache_count", entry.min_time_ms, entry.max_time_ms, step_ms)
        .unwrap_err();
    let cache_on_summary = cache_on.last_range_scalar_cache_summary().copied().unwrap();

    assert!(matches!(cache_off_error, PromqlQueryError::Storage(_)));
    assert!(matches!(cache_on_error, PromqlQueryError::Storage(_)));
    assert_eq!(cache_off_error, cache_on_error);
    assert_eq!(cache_off_summary.retained_charge_after_finalize, 0);
    assert_eq!(cache_on_summary.retained_charge_after_finalize, 0);
    assert_eq!(cache_on_summary.admitted_entries, 0);
    assert_eq!(cache_on_summary.hits, 0);
    assert_eq!(range_scalar_cache_governor_stats().current_leased_bytes, 0);
}
