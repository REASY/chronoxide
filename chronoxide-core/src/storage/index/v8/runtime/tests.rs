use std::fs;
use std::io::Cursor;
use std::sync::{Arc, Barrier};
use std::thread;

use tempfile::TempDir;

use crate::storage::chunk::ChunkIndexRange;
use crate::storage::index::{
    LabelValueFstIndex, LabelValueTimeRange, MetricSeriesRange, SegmentIndexes,
};
use crate::storage::metadata_governor::{MetadataGovernorConfig, MetadataUsageClass};
use crate::storage::metadata_runtime::{
    MetadataIssuedReadCount, SegmentArtifactRegistration, StoreMetadataRuntime,
};
use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;
use crate::storage::series::v2_runtime::GovernedSchema6SeriesReader;
use crate::storage::series::{SERIES_KIND_FLOAT, SeriesEntry, write_series_bin};
use crate::storage::symbols::{GovernedSymbolReader, GovernedSymbolSession, write_symbols_bin_v3};

use super::super::codec::decode_root;
use super::super::{
    RootCounts, TRAILER_LEN, corrupt_exact_postings_payload_for_test, encode_segment_indexes_v8,
    encode_segment_indexes_v9,
};
use super::*;

mod authority_acceptance;
mod resource_acceptance;

const SYMBOLS: [&str; 3] = ["alpha", "beta", "label"];
const LABEL_NAME_SYM: u32 = 2;

struct Fixture {
    _directory: TempDir,
    runtime: StoreMetadataRuntime,
    registered: RegisteredSegment,
    num_series: u32,
}

fn runtime(retained_max_bytes: u64, in_flight_max_bytes: u64) -> StoreMetadataRuntime {
    StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes,
        in_flight_max_bytes,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .expect("valid schema-7 index runtime")
}

fn indexes(num_series: u32, fst_values: &[&[u8]]) -> SegmentIndexes {
    let mut indexes = SegmentIndexes::default();
    if num_series != 0 {
        indexes.metric_series_ranges.insert_range(
            0,
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: num_series,
                kind_mask: u16::from(SERIES_KIND_FLOAT),
                min_time_ms: 10,
                max_time_ms: 20,
            },
        );
        for series_ref in 0..num_series {
            indexes.exact_postings.insert(LABEL_NAME_SYM, 0, series_ref);
        }
        indexes
            .label_value_time_ranges
            .insert(LABEL_NAME_SYM, 0, 10, 20);
        indexes
            .label_value_time_ranges
            .insert(LABEL_NAME_SYM, 1, 11, 21);
        let mut fsts = LabelValueFstIndex::default();
        fsts.insert_fst(LABEL_NAME_SYM, build_fst(fst_values));
        indexes.label_values = fsts;
    }
    indexes
}

fn build_fst(values: &[&[u8]]) -> Vec<u8> {
    let mut builder = fst::SetBuilder::memory();
    for value in values {
        builder
            .insert(value)
            .expect("insert sorted FST fixture value");
    }
    builder.into_inner().expect("finish FST fixture")
}

fn encode_index(num_series: u32, fst_values: &[&[u8]]) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    encode_segment_indexes_v8(
        &mut bytes,
        &indexes(num_series, fst_values),
        RootCounts {
            series: num_series,
            symbols: SYMBOLS.len() as u32,
        },
    )
    .expect("encode schema-7 v8 runtime fixture");
    bytes.into_inner()
}

fn encode_index_v9(num_series: u32, fst_values: &[&[u8]]) -> Vec<u8> {
    let mut bytes = Cursor::new(Vec::new());
    encode_segment_indexes_v9(
        &mut bytes,
        &indexes(num_series, fst_values),
        RootCounts {
            series: num_series,
            symbols: SYMBOLS.len() as u32,
        },
    )
    .expect("encode schema-8 v9 runtime fixture");
    bytes.into_inner()
}

fn fixture(
    identity: &str,
    runtime: StoreMetadataRuntime,
    num_series: u32,
    index_bytes: &[u8],
) -> Fixture {
    let directory = TempDir::new().expect("create schema-7 v8 runtime fixture");
    let entries = (0..num_series)
        .map(|series_ref| SeriesEntry {
            series_id: u64::from(series_ref),
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: ChunkIndexRange { offset: 0, len: 0 },
            labels: Vec::new(),
        })
        .collect::<Vec<_>>();
    let mut series_bytes = Vec::new();
    write_series_bin(&mut series_bytes, &entries).expect("encode series fixture");
    let mut symbols_bytes = Vec::new();
    write_symbols_bin_v3(&mut symbols_bytes, SYMBOLS).expect("encode paged symbol fixture");
    let mut chunk_index_bytes = Vec::new();
    let empty_chunks = (0..num_series).map(|_| Vec::new()).collect::<Vec<_>>();
    crate::storage::chunk::write_chunk_index(&mut chunk_index_bytes, &empty_chunks)
        .expect("encode chunk-index fixture");

    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let path = directory.path().join(file.filename());
        let contents: &[u8] = match file {
            SegmentFile::Indexes => index_bytes,
            SegmentFile::Series => &series_bytes,
            SegmentFile::Symbols => &symbols_bytes,
            SegmentFile::ChunkIndex => &chunk_index_bytes,
            _ => b"fixture",
        };
        fs::write(&path, contents).expect("write schema-7 v8 runtime artifact");
        SegmentArtifactRegistration::new(file, path, contents.len() as u64)
    });
    let registered = runtime
        .register_segment(identity, &artifacts)
        .expect("register schema-7 v8 runtime fixture");
    Fixture {
        _directory: directory,
        runtime,
        registered,
        num_series,
    }
}

fn count_bindings(
    registered: &RegisteredSegment,
    num_series: u32,
) -> (GovernedSeriesCountBinding, GovernedSymbolCountBinding) {
    let series_reader = GovernedSchema6SeriesReader::open(registered, num_series)
        .expect("open series count authority");
    let series_session = series_reader
        .query_session()
        .expect("open series count session");
    let series_root = series_session.load_root().expect("load series root");
    let series = series_session
        .series_count_binding(&series_root)
        .expect("mint series count binding");

    let symbol_reader = GovernedSymbolReader::open(registered).expect("open symbol authority");
    let symbol_session = symbol_reader
        .query_session()
        .expect("open symbol count session");
    let symbols = symbol_session.symbol_count_binding();
    (series, symbols)
}

fn symbol_session(registered: &RegisteredSegment) -> GovernedSymbolSession {
    GovernedSymbolReader::open(registered)
        .expect("open symbols")
        .query_session()
        .expect("open symbol query session")
}

fn bind(fixture: &Fixture, session: &GovernedSchema7IndexSession) -> GovernedSchema7BoundIndexRoot {
    let (series, symbols) = count_bindings(&fixture.registered, fixture.num_series);
    session
        .bind_segment_roots(series, symbols)
        .expect("bind schema-7 v8 root")
}

fn class_reads(
    runtime: &StoreMetadataRuntime,
    class: MetadataCacheClass,
) -> MetadataIssuedReadCount {
    runtime.snapshot().reads.classes[class.stable_index()].issued
}

#[test]
fn open_reads_only_fixed_ranges_and_binding_reuses_them() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-fixed-root",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).expect("open v8 reader");
    assert_eq!(reader.segment_identity(), "schema7-v8-fixed-root");
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
        MetadataIssuedReadCount {
            calls: 2,
            bytes: (HEADER_LEN + TRAILER_LEN) as u64,
        }
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls,
        0
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls,
        0
    );

    let session = reader.query_session().expect("open v8 session");
    let root = bind(&fixture, &session);
    let value = session.root(&root).expect("consume bound root");
    assert_eq!(value.file_len(), bytes.len() as u64);
    assert_eq!(value.series_count(), 2);
    assert_eq!(value.symbol_count(), SYMBOLS.len() as u32);
    assert_eq!(value.exact_entry_count(), 1);
    assert_eq!(value.exact_page_count(), 1);
    assert_eq!(value.auxiliary_entry_count(), 2);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot).calls,
        2
    );
}

#[test]
fn zero_retention_reuses_live_pins_then_reissues_fixed_ranges() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-zero-retention",
        runtime(0, 1024 * 1024),
        2,
        &bytes,
    );
    let baseline_in_flight = fixture.runtime.snapshot().governor.in_flight_bytes;
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).expect("open v8 reader");
    let after_open = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
    let session = reader.query_session().expect("open v8 session");
    let first = bind(&fixture, &session);
    let after_first = class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot);
    assert_eq!(after_first.calls - after_open.calls, 2);
    let second = bind(&fixture, &session);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot),
        after_first
    );
    drop(first);
    drop(second);
    let third = bind(&fixture, &session);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot).calls - after_first.calls,
        2
    );
    drop(third);
    drop(session);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline_in_flight
    );
}

#[test]
fn same_generation_root_count_mismatch_is_sticky_before_directory_io() {
    let bytes = encode_index(1, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-count-mismatch",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).expect("stage v8 roots");
    let session = reader.query_session().expect("open v8 session");
    let (series, symbols) = count_bindings(&fixture.registered, 2);
    let error = session
        .bind_segment_roots(series, symbols)
        .err()
        .expect("count mismatch must fail");
    assert!(matches!(
        error,
        Schema7IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls,
        0
    );
    let (series, symbols) = count_bindings(&fixture.registered, 2);
    assert!(matches!(
        session.bind_segment_roots(series, symbols),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
}

#[test]
fn foreign_count_bindings_fail_without_io_or_poisoning() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let first = fixture(
        "schema7-v8-foreign-first",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let second = fixture(
        "schema7-v8-foreign-second",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&first.registered).expect("open first reader");
    let session = reader.query_session().expect("open first session");
    let before = class_reads(&first.runtime, MetadataCacheClass::IndexRoot);
    let (series, symbols) = count_bindings(&second.registered, 2);
    assert!(matches!(
        session.bind_segment_roots(series, symbols),
        Err(Schema7IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(
        class_reads(&first.runtime, MetadataCacheClass::IndexRoot),
        before
    );
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn exact_directory_page_and_postings_are_lazy_and_context_bound() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-exact",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).expect("open v8 reader");
    let session = reader.query_session().expect("open v8 session");
    let root = bind(&fixture, &session);
    let selection = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .expect("select exact postings")
        .expect("fixture exact key exists");
    assert_eq!(
        session.selection_metadata(&root, &selection).unwrap(),
        ExactPostingsMetadata {
            byte_len: 12,
            time_range: LabelValueTimeRange {
                min_time_ms: 10,
                max_time_ms: 20,
            },
        }
    );
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
        0
    );
    let postings = session
        .read_exact_postings(&root, &selection)
        .expect("read exact postings");
    assert_eq!(session.postings(&root, &postings).unwrap(), [0, 1]);
    let after = fixture.runtime.snapshot().reads;

    let selection_again = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .unwrap()
        .unwrap();
    let postings_again = session
        .read_exact_postings(&root, &selection_again)
        .unwrap();
    assert_eq!(session.postings(&root, &postings_again).unwrap(), [0, 1]);
    assert_eq!(fixture.runtime.snapshot().reads, after);
    assert!(postings.charged_bytes() > 0);
}

#[test]
fn substituted_opaque_selection_is_nonsticky_foreign_context() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-selection-context",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();
    let root = bind(&fixture, &session);
    let mut selection = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .unwrap()
        .unwrap();
    selection.substitute_record_for_test();
    let before = class_reads(&fixture.runtime, MetadataCacheClass::Postings);
    assert!(matches!(
        session.read_exact_postings(&root, &selection),
        Err(Schema7IndexReaderError::ForeignRootContext)
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        before
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn same_key_selection_context_substitutions_fail_before_postings_io() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-same-key-selection-context",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();
    let root = bind(&fixture, &session);
    let substitutions: [fn(&mut GovernedSchema7ExactPostingsSelection); 5] = [
        GovernedSchema7ExactPostingsSelection::substitute_locator_for_test,
        GovernedSchema7ExactPostingsSelection::substitute_ref_count_for_test,
        GovernedSchema7ExactPostingsSelection::substitute_payload_crc_for_test,
        GovernedSchema7ExactPostingsSelection::substitute_page_index_for_test,
        GovernedSchema7ExactPostingsSelection::substitute_descriptor_for_test,
    ];
    let before = class_reads(&fixture.runtime, MetadataCacheClass::Postings);
    for substitute in substitutions {
        let mut selection = session
            .select_exact_postings(&root, LABEL_NAME_SYM, 0)
            .unwrap()
            .unwrap();
        substitute(&mut selection);
        assert!(matches!(
            session.read_exact_postings(&root, &selection),
            Err(Schema7IndexReaderError::ForeignRootContext)
        ));
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::Postings),
            before
        );
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    }
}

#[test]
fn substituted_postings_and_auxiliary_handles_are_nonsticky_context_errors() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-handle-context",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();
    let root = bind(&fixture, &session);

    let selection = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .unwrap()
        .unwrap();
    let mut postings = session
        .read_exact_postings(&root, &selection)
        .expect("load valid postings before substituting the wrapper");
    postings.substitute_record_for_test();
    let reads_before_postings_context = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.postings(&root, &postings),
        Err(Schema7IndexReaderError::ForeignRootContext)
    ));
    assert_eq!(
        fixture.runtime.snapshot().reads,
        reads_before_postings_context
    );

    let mut directory = session.load_auxiliary_directory(&root).unwrap();
    let mut fst = session
        .load_label_value_fst(&root, &directory, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();
    let mut ranges = session
        .load_label_value_time_ranges(&root, &directory, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();
    let symbols = symbol_session(&fixture.registered);
    let reads_before_aux_context = fixture.runtime.snapshot().reads;

    fst.substitute_record_for_test();
    assert!(matches!(
        session.visit_label_values_with_prefix(&root, &fst, &symbols, None, |_, _| true),
        Err(Schema7IndexReaderError::ForeignRootContext)
    ));
    ranges.substitute_record_for_test();
    assert!(matches!(
        session.label_value_time_ranges(&root, &ranges),
        Err(Schema7IndexReaderError::ForeignRootContext)
    ));
    directory.substitute_root_for_test();
    assert!(matches!(
        session.has_label_values(&root, &directory),
        Err(Schema7IndexReaderError::ForeignRootContext)
    ));
    assert_eq!(fixture.runtime.snapshot().reads, reads_before_aux_context);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn exact_payload_crc_failure_is_sticky_across_retry_and_fd_eviction() {
    let mut bytes = encode_index(2, &[b"alpha", b"beta"]);
    let trailer_offset = bytes.len() - TRAILER_LEN;
    let root = decode_root(
        bytes.len() as u64,
        &bytes[..HEADER_LEN],
        &bytes[trailer_offset..],
        RootCounts {
            series: 2,
            symbols: SYMBOLS.len() as u32,
        },
        AuthenticatedIndexFormat::V8Raw,
    )
    .unwrap();
    bytes[root.exact_postings.offset as usize + 4] ^= 1;
    let fixture = fixture(
        "schema7-v8-postings-crc",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();
    let bound = bind(&fixture, &session);
    let selection = session
        .select_exact_postings(&bound, LABEL_NAME_SYM, 0)
        .unwrap()
        .unwrap();
    assert!(matches!(
        session.read_exact_postings(&bound, &selection),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    let after_error = class_reads(&fixture.runtime, MetadataCacheClass::Postings);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    assert!(matches!(
        session.read_exact_postings(&bound, &selection),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        after_error
    );
}

#[test]
fn v9_exact_payload_crc_failure_is_sticky_across_retry_cache_and_fd_eviction() {
    let mut bytes = encode_index_v9(2, &[b"alpha", b"beta"]);
    corrupt_exact_postings_payload_for_test(&mut bytes, (LABEL_NAME_SYM, 0))
        .expect("corrupt protected v9 postings payload");
    let fixture = fixture(
        "schema8-v9-postings-crc",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open_v9(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();
    let bound = bind(&fixture, &session);
    let selection = session
        .select_exact_postings(&bound, LABEL_NAME_SYM, 0)
        .unwrap()
        .unwrap();

    assert!(matches!(
        session.read_exact_postings(&bound, &selection),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    let after_error = fixture.runtime.snapshot();
    assert_eq!(after_error.cache.sticky_artifacts, 1);
    assert_eq!(after_error.files.open_files, 0);
    assert!(after_error.cache.resident_entries > 0);

    assert!(matches!(
        session.read_exact_postings(&bound, &selection),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after_error.reads);

    drop(selection);
    drop(bound);
    drop(session);
    drop(reader);
    fixture.runtime.evict_all_resident_metadata();
    let after_eviction = fixture.runtime.snapshot();
    assert_eq!(after_eviction.cache.resident_entries, 0);
    assert_eq!(after_eviction.cache.sticky_artifacts, 1);
    assert_eq!(after_eviction.files.open_files, 0);

    assert!(matches!(
        GovernedSchema7IndexReader::open_v9(&fixture.registered),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    let after_reopen = fixture.runtime.snapshot();
    assert_eq!(after_reopen.reads, after_error.reads);
    assert_eq!(
        after_reopen.files.descriptor_opens,
        after_eviction.files.descriptor_opens
    );
    assert_eq!(after_reopen.cache.sticky_artifacts, 1);
}

#[test]
fn auxiliary_fst_and_ranges_are_lazy_cached_and_symbol_bound() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-auxiliary",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();
    let root = bind(&fixture, &session);
    let directory = session.load_auxiliary_directory(&root).unwrap();
    assert!(session.has_label_values(&root, &directory).unwrap());
    assert_eq!(
        session
            .label_name_symbols(&root, &directory)
            .unwrap()
            .collect::<Vec<_>>(),
        [LABEL_NAME_SYM]
    );
    assert_eq!(
        session
            .label_time_range(&root, &directory, LABEL_NAME_SYM)
            .unwrap(),
        Some(LabelValueTimeRange {
            min_time_ms: 10,
            max_time_ms: 21,
        })
    );
    let fst = session
        .load_label_value_fst(&root, &directory, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();
    let symbols = symbol_session(&fixture.registered);
    let mut visited = Vec::new();
    assert!(
        session
            .visit_label_values_with_prefix(&root, &fst, &symbols, Some("a"), |symbol_id, value| {
                assert_eq!(symbol_id, 0);
                visited.push(value.to_owned());
                true
            })
            .unwrap()
    );
    assert_eq!(visited, ["alpha"]);
    let ranges = session
        .load_label_value_time_ranges(&root, &directory, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();
    assert_eq!(
        session.label_value_time_range(&root, &ranges, 1).unwrap(),
        Some(LabelValueTimeRange {
            min_time_ms: 11,
            max_time_ms: 21,
        })
    );
    assert!(directory.charged_bytes() > 0);
    assert!(fst.charged_bytes() > 0);
    assert!(ranges.charged_bytes() > 0);
    let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    drop(fst);
    drop(ranges);
    let _ = session
        .load_label_value_fst(&root, &directory, LABEL_NAME_SYM)
        .unwrap();
    let _ = session
        .load_label_value_time_ranges(&root, &directory, LABEL_NAME_SYM)
        .unwrap();
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after
    );
}

#[test]
fn unresolved_fst_value_is_sticky_index_corruption_not_empty_enumeration() {
    let bytes = encode_index(2, &[b"ghost", b"phantom"]);
    let fixture = fixture(
        "schema7-v8-unresolved-fst",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();
    let root = bind(&fixture, &session);
    let directory = session.load_auxiliary_directory(&root).unwrap();
    let fst = session
        .load_label_value_fst(&root, &directory, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();
    let symbols = symbol_session(&fixture.registered);
    let mut called = false;
    assert!(matches!(
        session.visit_label_values_with_prefix(&root, &fst, &symbols, None, |_, _| {
            called = true;
            true
        }),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert!(!called);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    symbols
        .visit_required_resolved(0, |value| {
            assert_eq!(value, "alpha");
            Ok(())
        })
        .expect("symbol artifact remains healthy");
}

#[test]
fn concurrent_identical_exact_misses_are_single_flight() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-concurrent",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = Arc::new(GovernedSchema7IndexReader::open(&fixture.registered).unwrap());
    let barrier = Arc::new(Barrier::new(8));
    let mut threads = Vec::new();
    for _ in 0..8 {
        let reader = Arc::clone(&reader);
        let barrier = Arc::clone(&barrier);
        let registered = fixture.registered.clone();
        threads.push(thread::spawn(move || {
            let session = reader.query_session().unwrap();
            let (series, symbols) = count_bindings(&registered, 2);
            let root = session.bind_segment_roots(series, symbols).unwrap();
            barrier.wait();
            let selection = session
                .select_exact_postings(&root, LABEL_NAME_SYM, 0)
                .unwrap()
                .unwrap();
            let postings = session.read_exact_postings(&root, &selection).unwrap();
            assert_eq!(session.postings(&root, &postings).unwrap(), [0, 1]);
        }));
    }
    for thread in threads {
        thread.join().unwrap();
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
    assert!(fixture.runtime.snapshot().cache.single_flight_waits > 0);
}

#[test]
fn hard_budget_refusal_happens_before_fixed_root_io_and_is_not_sticky() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-budget-refusal",
        runtime(0, 1024 * 1024),
        2,
        &bytes,
    );
    let before = fixture.runtime.snapshot().governor;
    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(
            before
                .in_flight_max_bytes
                .checked_sub(before.in_flight_bytes)
                .and_then(|remaining| remaining.checked_sub(1))
                .expect("leave one byte of in-flight capacity"),
            MetadataUsageClass::Scratch,
        )
        .expect("reserve competing in-flight capacity");
    let error = GovernedSchema7IndexReader::open(&fixture.registered)
        .err()
        .expect("tiny budget must fail");
    assert!(matches!(
        error,
        Schema7IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexRoot).calls,
        0
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    drop(blocker);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        before.in_flight_bytes
    );
}
