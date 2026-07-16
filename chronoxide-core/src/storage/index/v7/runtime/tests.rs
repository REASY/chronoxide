use std::fs;
use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::TempDir;

use crate::storage::index::{
    ExactPostingsIndex, LabelValueTimeRange, LabelValueTimeRangeIndex, SegmentIndexes,
};
use crate::storage::metadata_governor::{
    MetadataCharge, MetadataGovernorConfig, MetadataUsageClass,
};
use crate::storage::metadata_runtime::{
    MetadataIssuedReadCount, SegmentArtifactRegistration, StoreMetadataRuntime,
};
use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;
use crate::storage::series::v2_runtime::GovernedSchema6SeriesReader;
use crate::storage::series::{
    GovernedSeriesCountBinding, SERIES_KIND_FLOAT, SeriesEntry, write_series_bin,
};
use crate::storage::symbols::{
    GovernedSymbolCountBinding, GovernedSymbolReader, GovernedSymbolSession, write_symbols_bin_v3,
};

use super::*;
use crate::storage::index::v7::{
    TRAILER_CRC_OFFSET, TRAILER_EXACT_PAGES_LOCATOR_OFFSET, write_segment_indexes_v7,
};

struct Fixture {
    _directory: TempDir,
    runtime: StoreMetadataRuntime,
    registered: RegisteredSegment,
    num_series: u32,
    symbol_count: u32,
}

fn runtime(retained_max_bytes: u64, in_flight_max_bytes: u64) -> StoreMetadataRuntime {
    StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes,
        in_flight_max_bytes,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .expect("valid schema-6 index test runtime")
}

fn encoded_index() -> Vec<u8> {
    let mut bytes = Vec::new();
    write_segment_indexes_v7(&mut bytes, &SegmentIndexes::default()).expect("encode index fixture");
    bytes
}

fn encoded_exact_index(entries: &[(u32, Vec<u32>)]) -> Vec<u8> {
    let mut exact_postings = ExactPostingsIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    for (label_value_sym, refs) in entries {
        for series_ref in refs {
            exact_postings.insert(7, *label_value_sym, *series_ref);
        }
        label_value_time_ranges.insert(
            7,
            *label_value_sym,
            1_000 + u64::from(*label_value_sym),
            2_000 + u64::from(*label_value_sym),
        );
    }
    let indexes = SegmentIndexes {
        exact_postings,
        label_value_time_ranges,
        ..SegmentIndexes::default()
    };
    let mut bytes = Vec::new();
    write_segment_indexes_v7(&mut bytes, &indexes).expect("encode exact index fixture");
    bytes
}

fn fixture(identity: &str, runtime: StoreMetadataRuntime, index_bytes: &[u8]) -> Fixture {
    fixture_with_metadata(identity, runtime, index_bytes, 0, 4_096)
}

fn fixture_with_metadata(
    identity: &str,
    runtime: StoreMetadataRuntime,
    index_bytes: &[u8],
    num_series: u32,
    symbol_count: u32,
) -> Fixture {
    let directory = TempDir::new().expect("create schema-6 index fixture directory");
    let mut series_bytes = Vec::new();
    let entries = (0..num_series)
        .map(|series_ref| SeriesEntry {
            series_id: u64::from(series_ref),
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: crate::storage::chunk::ChunkIndexRange { offset: 0, len: 0 },
            labels: Vec::new(),
        })
        .collect::<Vec<_>>();
    write_series_bin(&mut series_bytes, &entries).expect("encode schema-6 series root fixture");
    let mut symbol_values = Vec::with_capacity(symbol_count as usize);
    if symbol_count != 0 {
        symbol_values.push("api".to_owned());
        for symbol_id in 0..symbol_count.saturating_sub(2) {
            symbol_values.push(format!("s{symbol_id:010}"));
        }
        if symbol_count > 1 {
            symbol_values.push("worker".to_owned());
        }
    }
    let mut symbol_bytes = Vec::new();
    write_symbols_bin_v3(&mut symbol_bytes, symbol_values.iter())
        .expect("encode schema-6 symbol root fixture");
    let mut chunk_index_bytes = Vec::new();
    let empty_chunk_entries = (0..num_series).map(|_| Vec::new()).collect::<Vec<_>>();
    crate::storage::chunk::write_chunk_index(&mut chunk_index_bytes, &empty_chunk_entries)
        .expect("encode schema-6 chunk-index root fixture");
    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let path = directory.path().join(file.filename());
        let contents: &[u8] = match file {
            SegmentFile::Indexes => index_bytes,
            SegmentFile::Series => &series_bytes,
            SegmentFile::Symbols => &symbol_bytes,
            SegmentFile::ChunkIndex => &chunk_index_bytes,
            _ => b"fixture",
        };
        fs::write(&path, contents).expect("write schema-6 index fixture");
        SegmentArtifactRegistration::new(file, path, contents.len() as u64)
    });
    let registered = runtime
        .register_segment(identity, &artifacts)
        .expect("register schema-6 index fixture");
    Fixture {
        _directory: directory,
        runtime,
        registered,
        num_series,
        symbol_count,
    }
}

fn metadata_bindings(
    fixture: &Fixture,
) -> (GovernedSeriesCountBinding, GovernedSymbolCountBinding) {
    let series_reader = GovernedSchema6SeriesReader::open(&fixture.registered, fixture.num_series)
        .expect("open bound schema-6 series fixture");
    let series_session = series_reader
        .query_session()
        .expect("open bound schema-6 series session");
    let series_root = series_session
        .load_root()
        .expect("load bound schema-6 series root");
    let series = series_session
        .series_count_binding(&series_root)
        .expect("mint bound schema-6 series-count capability");

    let symbol_reader = GovernedSymbolReader::open(&fixture.registered)
        .expect("open bound schema-6 symbol fixture");
    let symbol_session = symbol_reader
        .query_session()
        .expect("open bound schema-6 symbol session");
    let symbols = symbol_session.symbol_count_binding();
    (series, symbols)
}

fn symbol_session(registered: &RegisteredSegment) -> GovernedSymbolSession {
    GovernedSymbolReader::open(registered)
        .expect("open bound schema-6 symbols")
        .query_session()
        .expect("open bound schema-6 symbol session")
}

fn bind_index_root(
    fixture: &Fixture,
    session: &GovernedSchema6IndexSession,
    root: GovernedSchema6IndexRoot,
) -> GovernedSchema6BoundIndexRoot {
    let (series, symbols) = metadata_bindings(fixture);
    session
        .bind_segment_roots(root, series, symbols)
        .expect("bind index root to authoritative series and symbol roots")
}

fn class_reads(
    runtime: &StoreMetadataRuntime,
    class: MetadataCacheClass,
) -> MetadataIssuedReadCount {
    runtime.snapshot().reads.classes[class.stable_index()].issued
}

fn block_all_but_one_byte(runtime: &StoreMetadataRuntime) -> MetadataCharge {
    let snapshot = runtime.snapshot().governor;
    runtime
        .governor()
        .reserve_in_flight_for_usage(
            snapshot
                .in_flight_max_bytes
                .checked_sub(snapshot.in_flight_bytes)
                .and_then(|remaining| remaining.checked_sub(1))
                .expect("fixture leaves capacity for a competing reservation"),
            MetadataUsageClass::Scratch,
        )
        .expect("reserve all but one in-flight byte")
}

fn trailer_locator(bytes: &[u8], locator_offset: usize) -> (u64, u64) {
    let trailer_start = bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN;
    let read = |offset: usize| {
        u64::from_le_bytes(
            bytes[trailer_start + offset..trailer_start + offset + 8]
                .try_into()
                .expect("fixed trailer locator field"),
        )
    };
    (read(locator_offset), read(locator_offset + 8))
}

#[test]
fn governed_root_reads_only_the_two_fixed_ranges_and_reuses_them() {
    let fixture = fixture(
        "schema6-index-root",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded_index(),
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open governed schema-6 index reader");
    assert_eq!(reader.segment_identity(), "schema6-index-root");
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(fixture.runtime.snapshot().files.peak_open_files, 1);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
        MetadataIssuedReadCount {
            calls: 2,
            bytes: (SEGMENT_INDEX_V7_HEADER_LEN + SEGMENT_INDEX_V7_TRAILER_LEN) as u64,
        }
    );

    let session = reader.query_session().expect("open query session");
    let root = session.load_root().expect("reuse governed roots");
    let value = session.root(&root).expect("consume root in owning session");
    assert_eq!(value.file_len(), encoded_index().len() as u64);
    assert_eq!(value.exact_entry_count(), 0);
    assert_eq!(value.exact_page_count(), 0);
    assert_eq!(value.auxiliary_entry_count(), 0);
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
        MetadataIssuedReadCount {
            calls: 2,
            bytes: (SEGMENT_INDEX_V7_HEADER_LEN + SEGMENT_INDEX_V7_TRAILER_LEN) as u64,
        }
    );
}

#[test]
fn zero_retention_reissues_roots_only_after_the_last_pin_drops() {
    let fixture = fixture(
        "schema6-index-zero-retention",
        runtime(0, 1024 * 1024),
        &encoded_index(),
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open zero-retention reader");
    let after_open = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
    let session = reader.query_session().expect("open query session");
    let first = session.load_root().expect("reload released roots");
    let after_first = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
    assert_eq!(after_first.calls - after_open.calls, 2);
    let second = session.load_root().expect("reuse live query-local pins");
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
        after_first
    );
    drop(first);
    drop(second);
    let third = session.load_root().expect("reload after final pin drop");
    assert_eq!(third.file_len(), encoded_index().len() as u64);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot).calls - after_first.calls,
        2
    );
}

#[test]
fn budget_refusal_before_root_io_is_retryable() {
    let fixture = fixture(
        "schema6-index-budget",
        runtime(0, 16 * 1024),
        &encoded_index(),
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open governed reader before blocking budget");
    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(8_000, MetadataUsageClass::Scratch)
        .expect("reserve competing budget");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
    let session = reader.query_session().expect("open query session");
    let error = session
        .load_root()
        .expect_err("budget refusal must happen before root I/O");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
        before
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    drop(blocker);
    session
        .load_root()
        .expect("root load retries after budget is released");
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot).calls - before.calls,
        2
    );
}

#[test]
fn structural_root_corruption_is_sticky_across_retries() {
    let mut bytes = encoded_index();
    let trailer_start = bytes.len() - SEGMENT_INDEX_V7_TRAILER_LEN;
    bytes[trailer_start + TRAILER_CRC_OFFSET] ^= 1;
    let fixture = fixture(
        "schema6-index-corrupt-root",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );

    let first = match GovernedSchema6IndexReader::open(&fixture.registered) {
        Ok(_) => panic!("corrupt trailer must fail open"),
        Err(error) => error,
    };
    assert!(matches!(
        first,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_first = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
    let second = match GovernedSchema6IndexReader::open(&fixture.registered) {
        Ok(_) => panic!("sticky corruption must fail without another read"),
        Err(error) => error,
    };
    assert!(matches!(
        second,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
        after_first
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn roots_cannot_cross_segment_sessions() {
    let bytes = encoded_index();
    let runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture("schema6-index-first", runtime.clone(), &bytes);
    let second = fixture("schema6-index-second", runtime, &bytes);
    let first_reader =
        GovernedSchema6IndexReader::open(&first.registered).expect("open first index reader");
    let second_reader =
        GovernedSchema6IndexReader::open(&second.registered).expect("open second index reader");
    let first_session = first_reader.query_session().expect("open first session");
    let second_session = second_reader.query_session().expect("open second session");
    let root = first_session.load_root().expect("load first root");

    assert!(matches!(
        second_session.root(&root),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
}

#[test]
fn exact_lookup_reads_one_directory_page_and_postings_then_reuses_them() {
    let bytes = encoded_exact_index(&[(12, vec![3, 7, 11])]);
    let fixture = fixture_with_metadata(
        "schema6-index-exact",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        12,
        13,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open exact index reader");
    let session = reader.query_session().expect("open exact query session");
    let root = session.load_root().expect("load exact root");
    let bound = bind_index_root(&fixture, &session, root);
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let before_postings = class_reads(&fixture.runtime, MetadataCacheClass::Postings);

    let selection = session
        .select_exact_postings(&bound, 7, 12)
        .expect("select exact postings")
        .expect("exact key exists");
    assert_eq!(
        session
            .selection_metadata(&bound, &selection)
            .expect("read bound selection metadata"),
        ExactPostingsMetadata {
            byte_len: 16,
            time_range: LabelValueTimeRange {
                min_time_ms: 1_012,
                max_time_ms: 2_012,
            },
        }
    );
    let after_selection_directory =
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let after_selection_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after_selection_directory.calls - before_directory.calls, 1);
    assert_eq!(after_selection_directory.bytes - before_directory.bytes, 96);
    assert_eq!(after_selection_page.calls - before_page.calls, 1);
    assert_eq!(
        after_selection_page.bytes - before_page.bytes,
        EXACT_PAGE_LEN as u64
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        before_postings
    );

    let postings = session
        .read_exact_postings(&bound, &selection)
        .expect("read exact postings");
    assert_eq!(
        session
            .postings(&postings)
            .expect("consume postings through owning session"),
        &[3, 7, 11]
    );
    assert!(postings.charged_bytes() > 0);
    let after_postings = class_reads(&fixture.runtime, MetadataCacheClass::Postings);
    assert_eq!(after_postings.calls - before_postings.calls, 1);
    assert_eq!(after_postings.bytes - before_postings.bytes, 16);

    let repeated_selection = session
        .select_exact_postings(&bound, 7, 12)
        .expect("repeat exact selection")
        .expect("repeated key exists");
    let repeated_postings = session
        .read_exact_postings(&bound, &repeated_selection)
        .expect("reuse exact postings");
    assert_eq!(
        session
            .postings(&repeated_postings)
            .expect("consume reused postings"),
        &[3, 7, 11]
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        after_selection_directory
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after_selection_page
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        after_postings
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn exact_descriptor_gap_avoids_page_and_missing_page_key_avoids_postings() {
    let mut gap_entries = (0..409)
        .map(|label_value_sym| (label_value_sym, vec![0]))
        .collect::<Vec<_>>();
    gap_entries.push((1_000, vec![0]));
    let gap_bytes = encoded_exact_index(&gap_entries);
    let gap = fixture_with_metadata(
        "schema6-index-gap",
        runtime(1024 * 1024, 1024 * 1024),
        &gap_bytes,
        1,
        1_001,
    );
    let gap_reader = GovernedSchema6IndexReader::open(&gap.registered).expect("open gap reader");
    let gap_session = gap_reader.query_session().expect("open gap session");
    let gap_root = gap_session.load_root().expect("load gap root");
    let gap_bound = bind_index_root(&gap, &gap_session, gap_root);
    assert!(
        gap_session
            .select_exact_postings(&gap_bound, 7, 500)
            .expect("descriptor gap is a clean miss")
            .is_none()
    );
    assert_eq!(
        class_reads(&gap.runtime, MetadataCacheClass::IndexPage),
        MetadataIssuedReadCount::default()
    );

    let missing_bytes = encoded_exact_index(&[(12, vec![0]), (14, vec![0])]);
    let missing = fixture_with_metadata(
        "schema6-index-missing-page-key",
        runtime(1024 * 1024, 1024 * 1024),
        &missing_bytes,
        1,
        15,
    );
    let missing_reader =
        GovernedSchema6IndexReader::open(&missing.registered).expect("open missing-key reader");
    let missing_session = missing_reader
        .query_session()
        .expect("open missing-key session");
    let missing_root = missing_session.load_root().expect("load missing-key root");
    let missing_bound = bind_index_root(&missing, &missing_session, missing_root);
    assert!(
        missing_session
            .select_exact_postings(&missing_bound, 7, 13)
            .expect("in-page gap is a clean miss")
            .is_none()
    );
    assert_eq!(
        class_reads(&missing.runtime, MetadataCacheClass::IndexPage).calls,
        1
    );
    assert_eq!(
        class_reads(&missing.runtime, MetadataCacheClass::Postings),
        MetadataIssuedReadCount::default()
    );
}

#[test]
fn out_of_range_postings_are_sticky_and_foreign_root_bindings_are_not() {
    let bytes = encoded_exact_index(&[(12, vec![3])]);
    let corrupt = fixture_with_metadata(
        "schema6-index-postings-bounds",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        3,
        13,
    );
    let reader =
        GovernedSchema6IndexReader::open(&corrupt.registered).expect("open postings-bounds reader");
    let session = reader
        .query_session()
        .expect("open postings-bounds session");
    let root = session.load_root().expect("load postings-bounds root");
    let corrupt_bound = bind_index_root(&corrupt, &session, root);
    let selection = session
        .select_exact_postings(&corrupt_bound, 7, 12)
        .expect("select postings-bounds key")
        .expect("postings-bounds key exists");
    let error = session
        .read_exact_postings(&corrupt_bound, &selection)
        .expect_err("series ref equal to the count is corruption");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_error = corrupt.runtime.snapshot().reads;
    let repeated = session
        .select_exact_postings(&corrupt_bound, 7, 12)
        .expect_err("artifact corruption must survive cache hits");
    assert!(matches!(
        repeated,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(corrupt.runtime.snapshot().reads, after_error);
    assert_eq!(corrupt.runtime.snapshot().cache.sticky_artifacts, 1);

    let shared_runtime = runtime(1024 * 1024, 1024 * 1024);
    let valid = fixture_with_metadata(
        "schema6-index-postings-binding",
        shared_runtime.clone(),
        &bytes,
        4,
        13,
    );
    let foreign = fixture_with_metadata(
        "schema6-index-postings-foreign-binding",
        shared_runtime,
        &bytes,
        5,
        13,
    );
    let valid_reader =
        GovernedSchema6IndexReader::open(&valid.registered).expect("open valid binding reader");
    let valid_session = valid_reader
        .query_session()
        .expect("open valid binding session");
    let valid_root = valid_session.load_root().expect("load valid binding root");
    let first_binding = bind_index_root(&valid, &valid_session, valid_root);
    let valid_selection = valid_session
        .select_exact_postings(&first_binding, 7, 12)
        .expect("select valid binding key")
        .expect("valid binding key exists");
    valid_session
        .read_exact_postings(&first_binding, &valid_selection)
        .expect("read under the first exact count binding");
    let (foreign_series, foreign_symbols) = metadata_bindings(&foreign);
    let rebound_root = valid_session
        .load_root()
        .expect("reuse valid root before foreign binding");
    let before_binding_error = valid.runtime.snapshot().reads;
    assert!(matches!(
        valid_session.bind_segment_roots(rebound_root, foreign_series, foreign_symbols),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(valid.runtime.snapshot().reads, before_binding_error);
    assert_eq!(valid.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn exact_selection_and_cached_page_context_cannot_cross_roots() {
    let bytes = encoded_exact_index(&[(12, vec![0])]);
    let runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture_with_metadata("schema6-index-select-first", runtime.clone(), &bytes, 1, 13);
    let second = fixture_with_metadata("schema6-index-select-second", runtime, &bytes, 1, 13);
    let first_reader =
        GovernedSchema6IndexReader::open(&first.registered).expect("open first selection reader");
    let second_reader =
        GovernedSchema6IndexReader::open(&second.registered).expect("open second selection reader");
    let first_session = first_reader.query_session().expect("open first session");
    let second_session = second_reader.query_session().expect("open second session");
    let first_root = first_session
        .load_root()
        .expect("load first selection root");
    let second_root = second_session
        .load_root()
        .expect("load second selection root");
    let first_bound = bind_index_root(&first, &first_session, first_root);
    let second_bound = bind_index_root(&second, &second_session, second_root);
    let selection = first_session
        .select_exact_postings(&first_bound, 7, 12)
        .expect("select first key")
        .expect("first key exists");
    let before = second.runtime.snapshot().reads;
    assert!(matches!(
        second_session.selection_metadata(&second_bound, &selection),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(second.runtime.snapshot().reads, before);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);

    let directory = first_session
        .load_exact_directory(&first_bound)
        .expect("load first exact directory");
    let descriptor = directory.value.descriptors[0];
    let page = first_session
        .load_exact_page(&first_bound, 0, descriptor)
        .expect("load first exact page");
    let before_context_error = first.runtime.snapshot().reads;
    let substituted = ExactPageDescriptor {
        first_key: descriptor.first_key,
        last_key: descriptor.last_key,
        record_count: descriptor.record_count,
        page_crc32c: descriptor.page_crc32c ^ 1,
    };
    drop(page);
    let rebound = first_session
        .load_exact_page(&first_bound, 0, substituted)
        .expect_err("cached page must reject a substituted descriptor");
    assert!(matches!(
        rebound,
        Schema6IndexReaderError::ForeignRootContext
    ));
    assert_eq!(first.runtime.snapshot().reads, before_context_error);
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn exact_page_corruption_is_sticky_for_every_later_index_accessor() {
    let mut bytes = encoded_exact_index(&[(12, vec![0])]);
    let (pages_offset, pages_len) = trailer_locator(&bytes, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
    assert_eq!(pages_len, EXACT_PAGE_LEN as u64);
    bytes[pages_offset as usize + 20] ^= 1;
    let fixture = fixture_with_metadata(
        "schema6-index-corrupt-page",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        1,
        13,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("root remains valid when an exact page is corrupt");
    let session = reader.query_session().expect("open corrupt-page session");
    let root = session.load_root().expect("load corrupt-page root");
    let bound = bind_index_root(&fixture, &session, root);
    let error = session
        .select_exact_postings(&bound, 7, 12)
        .expect_err("touched exact page CRC corruption must fail");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_error = fixture.runtime.snapshot().reads;
    let repeated = session
        .exact_postings_metadata(&bound, 7, 99)
        .expect_err("sticky page corruption must poison another accessor");
    assert!(matches!(
        repeated,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after_error);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn exact_page_symbols_are_bound_to_the_authoritative_symbol_root() {
    let bytes = encoded_exact_index(&[(12, vec![0])]);
    let fixture = fixture_with_metadata(
        "schema6-index-exact-symbol-bound",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        1,
        12,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open exact symbol-bound reader");
    let session = reader
        .query_session()
        .expect("open exact symbol-bound session");
    let root = session.load_root().expect("load exact symbol-bound root");
    let bound = bind_index_root(&fixture, &session, root);
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);

    let error = session
        .select_exact_postings(&bound, 7, 12)
        .expect_err("descriptor symbol equal to symbol count must fail");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(after.calls - before.calls, 1);
    assert!(matches!(
        session.select_exact_postings(&bound, 7, 12),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        after
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn exact_budget_refusals_happen_before_directory_and_postings_io() {
    let bytes = encoded_exact_index(&[(12, vec![0])]);
    let fixture = fixture_with_metadata(
        "schema6-index-exact-budget",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        1,
        13,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open exact-budget reader");
    let session = reader.query_session().expect("open exact-budget session");
    let root = session.load_root().expect("load exact-budget root");
    let bound = bind_index_root(&fixture, &session, root);
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .select_exact_postings(&bound, 7, 12)
        .expect_err("directory load must be refused before I/O");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        before_directory
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_page
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    drop(blocker);

    let selection = session
        .select_exact_postings(&bound, 7, 12)
        .expect("retry exact selection after releasing budget")
        .expect("exact-budget key exists");
    let before_postings = class_reads(&fixture.runtime, MetadataCacheClass::Postings);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .read_exact_postings(&bound, &selection)
        .expect_err("postings load must be refused before I/O");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        before_postings
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    drop(blocker);

    let postings = session
        .read_exact_postings(&bound, &selection)
        .expect("retry postings after releasing budget");
    assert_eq!(session.postings(&postings).expect("consume retry"), &[0]);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings).calls - before_postings.calls,
        1
    );
}

#[test]
fn concurrent_exact_lookups_single_flight_each_governed_range() {
    const THREADS: usize = 8;

    let bytes = encoded_exact_index(&[(12, vec![0, 1, 2])]);
    let fixture = fixture_with_metadata(
        "schema6-index-exact-concurrent",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        3,
        13,
    );
    let reader = Arc::new(
        GovernedSchema6IndexReader::open(&fixture.registered)
            .expect("open concurrent exact reader"),
    );
    let setup_session = reader.query_session().expect("open setup session");
    let setup_root = setup_session.load_root().expect("load setup root");
    let bound = Arc::new(bind_index_root(&fixture, &setup_session, setup_root));
    drop(setup_session);
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles = (0..THREADS)
        .map(|_| {
            let reader = Arc::clone(&reader);
            let bound = Arc::clone(&bound);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let session = reader.query_session().expect("open worker session");
                barrier.wait();
                let selection = session
                    .select_exact_postings(&bound, 7, 12)
                    .expect("worker selects exact key")
                    .expect("worker exact key exists");
                let postings = session
                    .read_exact_postings(&bound, &selection)
                    .expect("worker reads exact postings");
                assert_eq!(
                    session.postings(&postings).expect("worker consumes refs"),
                    &[0, 1, 2]
                );
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("exact worker completes");
    }

    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls,
        1
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls,
        1
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings).calls,
        1
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(fixture.runtime.snapshot().cache.active_loads, 0);
}

#[test]
fn zero_retention_releases_exact_directory_page_and_postings_allocations() {
    let bytes = encoded_exact_index(&[(12, vec![0])]);
    let fixture = fixture_with_metadata(
        "schema6-index-exact-zero-retention",
        runtime(0, 1024 * 1024),
        &bytes,
        1,
        13,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open zero-retention exact reader");
    let session = reader
        .query_session()
        .expect("open zero-retention exact session");
    let root = session.load_root().expect("load zero-retention root");
    let bound = bind_index_root(&fixture, &session, root);

    let first_selection = session
        .select_exact_postings(&bound, 7, 12)
        .expect("first zero-retention selection")
        .expect("zero-retention key exists");
    let first_postings = session
        .read_exact_postings(&bound, &first_selection)
        .expect("first zero-retention postings read");
    assert_eq!(
        session.postings(&first_postings).expect("consume first"),
        &[0]
    );
    let after_first_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let after_first_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let after_first_postings = class_reads(&fixture.runtime, MetadataCacheClass::Postings);
    drop(first_postings);
    drop(first_selection);

    let second_selection = session
        .select_exact_postings(&bound, 7, 12)
        .expect("second zero-retention selection")
        .expect("zero-retention key still exists");
    let second_postings = session
        .read_exact_postings(&bound, &second_selection)
        .expect("second zero-retention postings read");
    assert_eq!(
        session.postings(&second_postings).expect("consume second"),
        &[0]
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls
            - after_first_directory.calls,
        1
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls - after_first_page.calls,
        1
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings).calls
            - after_first_postings.calls,
        1
    );
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

mod auxiliary;
mod auxiliary_payloads;
mod metric;
mod routing;
