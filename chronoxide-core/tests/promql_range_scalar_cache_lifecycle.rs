#[allow(dead_code)]
#[path = "support/promql_range_scalar_cache.rs"]
mod support;

use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};

use chronoxide_core::promql::PromqlQueryError;
use chronoxide_core::storage::chunk::{ChunkIndexEntry, read_chunk_index};
use chronoxide_core::storage::segment::{
    DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES, MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES, QueryLimits,
    RangeScalarCacheConfigError, RangeScalarCacheSummary, SegmentFile,
};
use support::{TypedRangeFixture, deterministic_segment_dirs, write_stale_reset_delta_fixture};

const MIB: u64 = 1024 * 1024;

fn assert_finalized_summary(summary: RangeScalarCacheSummary, configured_budget_bytes: u64) {
    assert_eq!(summary.configured_budget_bytes, configured_budget_bytes);
    assert_eq!(summary.retained_charge_after_finalize, 0);
}

fn error_kind(error: &PromqlQueryError) -> &'static str {
    match error {
        PromqlQueryError::Invalid(_) => "invalid",
        PromqlQueryError::Unsupported(_) => "unsupported",
        PromqlQueryError::LimitExceeded { .. } => "limit_exceeded",
        PromqlQueryError::Storage(_) => "storage",
    }
}

fn first_scalar_lane(fixture: &TypedRangeFixture) -> (std::path::PathBuf, ChunkIndexEntry) {
    deterministic_segment_dirs(fixture.path())
        .into_iter()
        .find_map(|segment_dir| {
            let mut index =
                File::open(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
            let entry = read_chunk_index(&mut index)
                .unwrap()
                .into_iter()
                .flatten()
                .find(|entry| entry.scalar_lane_offset > 0 && entry.scalar_lane_len > 16)?;
            Some((segment_dir, entry))
        })
        .expect("fixture must contain a scalar lane")
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

#[test]
fn session_cache_budget_defaults_accepts_boundaries_and_rejects_above_maximum() {
    assert_eq!(DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES, 16 * MIB);
    assert_eq!(MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES, 32 * MIB);

    let fixture = write_stale_reset_delta_fixture();
    let mut session = fixture.store.query_session().unwrap();
    assert_eq!(session.last_range_scalar_cache_summary(), None);

    session
        .query_promql_range("cache_count", 10_000, 10_000, 10_000)
        .unwrap();
    assert_finalized_summary(
        session.last_range_scalar_cache_summary().copied().unwrap(),
        DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES,
    );

    for budget in [0, 1, 4 * MIB, 16 * MIB, MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES] {
        assert_eq!(session.set_range_scalar_cache_budget_bytes(budget), Ok(()));
    }
    assert_eq!(
        session.set_range_scalar_cache_budget_bytes(MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES + 1),
        Err(RangeScalarCacheConfigError::BudgetTooLarge {
            requested_bytes: MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES + 1,
            maximum_bytes: MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES,
        })
    );

    let error = session
        .query_promql_range("cache_count", 20_000, 10_000, 0)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid PromQL selector: query_range step_ms must be greater than zero"
    );
    assert_finalized_summary(
        session.last_range_scalar_cache_summary().copied().unwrap(),
        MAX_RANGE_SCALAR_CACHE_BUDGET_BYTES,
    );
}

#[test]
fn every_range_exit_replaces_the_last_summary_and_instant_queries_leave_it_alone() {
    let fixture = write_stale_reset_delta_fixture();
    let mut session = fixture.store.query_session().unwrap();

    session
        .query_promql_with_limits("cache_count", 10_000, 10_000, QueryLimits::unlimited())
        .unwrap();
    assert_eq!(session.last_range_scalar_cache_summary(), None);

    session.set_range_scalar_cache_budget_bytes(0).unwrap();
    session
        .query_promql_range_with_limits(
            "cache_count",
            10_000,
            10_000,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_finalized_summary(
        session.last_range_scalar_cache_summary().copied().unwrap(),
        0,
    );

    session.set_range_scalar_cache_budget_bytes(1).unwrap();
    let error = session
        .query_promql_range("cache_count{", 10_000, 10_000, 10_000)
        .unwrap_err();
    assert_eq!(error_kind(&error), "invalid");
    assert_finalized_summary(
        session.last_range_scalar_cache_summary().copied().unwrap(),
        1,
    );

    session.set_range_scalar_cache_budget_bytes(2).unwrap();
    let error = session
        .query_promql_range("cache_count", 10_000, 10_000, 0)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid PromQL selector: query_range step_ms must be greater than zero"
    );
    assert_finalized_summary(
        session.last_range_scalar_cache_summary().copied().unwrap(),
        2,
    );

    session.set_range_scalar_cache_budget_bytes(3).unwrap();
    let error = session
        .query_promql_range("cache_count", 20_000, 10_000, 10_000)
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "invalid PromQL selector: query_range end_ms must be greater than or equal to start_ms"
    );
    assert_finalized_summary(
        session.last_range_scalar_cache_summary().copied().unwrap(),
        3,
    );

    session.set_range_scalar_cache_budget_bytes(4).unwrap();
    let error = session
        .query_promql_range_with_limits(
            "cache_count",
            10_000,
            10_000,
            10_000,
            QueryLimits {
                max_matched_series: Some(0),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();
    assert_eq!(error_kind(&error), "limit_exceeded");
    assert_finalized_summary(
        session.last_range_scalar_cache_summary().copied().unwrap(),
        4,
    );

    let before_instant = session.last_range_scalar_cache_summary().copied().unwrap();
    session
        .query_promql_with_limits("cache_count", 10_000, 10_000, QueryLimits::unlimited())
        .unwrap();
    assert_eq!(
        session.last_range_scalar_cache_summary().copied(),
        Some(before_instant)
    );
}

#[test]
fn decode_io_and_limit_errors_replace_success_summary_and_finalize_every_charge() {
    let fixture = write_stale_reset_delta_fixture();
    let (segment_dir, entry) = first_scalar_lane(&fixture);
    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let lane_offset = entry
        .offset
        .checked_add(u64::from(entry.scalar_lane_offset))
        .unwrap();
    let mut original_magic = [0; 4];
    let mut chunks = File::open(&chunks_path).unwrap();
    chunks.seek(SeekFrom::Start(lane_offset)).unwrap();
    chunks.read_exact(&mut original_magic).unwrap();
    drop(chunks);

    let mut session = fixture.store.query_session().unwrap();
    session.set_range_scalar_cache_budget_bytes(MIB).unwrap();
    session
        .query_promql_range("cache_count", 10_000, 50_000, 10_000)
        .unwrap();
    let success = session.last_range_scalar_cache_summary().copied().unwrap();
    assert_finalized_summary(success, MIB);
    assert!(success.admitted_entries > 0);

    overwrite_bytes(&chunks_path, lane_offset, &[0; 4]);
    session
        .set_range_scalar_cache_budget_bytes(2 * MIB)
        .unwrap();
    let decode_error = session
        .query_promql_range("cache_count", 10_000, 50_000, 10_000)
        .unwrap_err();
    assert_eq!(
        decode_error,
        PromqlQueryError::Storage("typed scalar lane magic mismatch".to_string())
    );
    let decode = session.last_range_scalar_cache_summary().copied().unwrap();
    assert_finalized_summary(decode, 2 * MIB);
    assert_eq!(decode.admitted_entries, 0);
    assert_ne!(decode, success);

    overwrite_bytes(&chunks_path, lane_offset, &original_magic);
    let truncated_len = entry
        .offset
        .checked_add(u64::from(entry.scalar_projection_read_len()))
        .and_then(|end| end.checked_sub(1))
        .unwrap();
    OpenOptions::new()
        .write(true)
        .open(&chunks_path)
        .unwrap()
        .set_len(truncated_len)
        .unwrap();
    session
        .set_range_scalar_cache_budget_bytes(3 * MIB)
        .unwrap();
    let io_error = session
        .query_promql_range("cache_count", 10_000, 50_000, 10_000)
        .unwrap_err();
    assert_eq!(
        io_error,
        PromqlQueryError::Storage("failed to fill whole buffer".to_string())
    );
    let io = session.last_range_scalar_cache_summary().copied().unwrap();
    assert_finalized_summary(io, 3 * MIB);
    assert_eq!(io.admitted_entries, 0);
    assert_ne!(io, decode);

    session
        .set_range_scalar_cache_budget_bytes(4 * MIB)
        .unwrap();
    let limit_error = session
        .query_promql_range_with_limits(
            "cache_count",
            10_000,
            50_000,
            10_000,
            QueryLimits {
                max_matched_series: Some(0),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();
    assert_eq!(
        limit_error,
        PromqlQueryError::LimitExceeded {
            limit: "matched_series".to_string(),
            max: 0,
        }
    );
    let limit = session.last_range_scalar_cache_summary().copied().unwrap();
    assert_finalized_summary(limit, 4 * MIB);
    assert_eq!(limit.governor_lease_bytes, 0);
    assert_eq!(limit.peak_retained_charge_bytes, 0);
    assert_ne!(limit, io);
}

#[test]
fn direct_and_session_range_calls_preserve_parse_then_bounds_precedence() {
    let fixture = write_stale_reset_delta_fixture();

    let direct_parse = fixture
        .store
        .query_promql_range("cache_count{", 20_000, 10_000, 0)
        .unwrap_err();
    let mut session = fixture.store.query_session().unwrap();
    let session_parse = session
        .query_promql_range("cache_count{", 20_000, 10_000, 0)
        .unwrap_err();
    assert_eq!(error_kind(&direct_parse), "invalid");
    assert_eq!(direct_parse.to_string(), session_parse.to_string());
    assert_ne!(
        direct_parse.to_string(),
        "invalid PromQL selector: query_range step_ms must be greater than zero"
    );

    let direct_bounds = fixture
        .store
        .query_promql_range("cache_count", 20_000, 10_000, 0)
        .unwrap_err();
    let session_bounds = session
        .query_promql_range("cache_count", 20_000, 10_000, 0)
        .unwrap_err();
    assert_eq!(direct_bounds.to_string(), session_bounds.to_string());
    assert_eq!(
        direct_bounds.to_string(),
        "invalid PromQL selector: query_range step_ms must be greater than zero"
    );
}

#[test]
fn direct_range_delegates_to_the_session_executor() {
    let fixture = write_stale_reset_delta_fixture();
    let direct = fixture
        .store
        .query_promql_range_with_limits(
            "cache_count",
            10_000,
            50_000,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    let mut session = fixture.store.query_session().unwrap();
    let session_result = session
        .query_promql_range_with_limits(
            "cache_count",
            10_000,
            50_000,
            10_000,
            QueryLimits::unlimited(),
        )
        .unwrap();
    assert_eq!(direct, session_result);
}
