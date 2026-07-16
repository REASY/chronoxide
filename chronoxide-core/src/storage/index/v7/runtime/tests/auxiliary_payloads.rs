use crate::storage::index::v7::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET;
use crate::storage::index::{
    LabelValueFstIndex, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
    SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
};

use super::auxiliary::{LABEL_NAME_SYM, encoded_auxiliary_index};
use super::*;

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(
        bytes[offset..offset + 2]
            .try_into()
            .expect("fixed u16 field"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("fixed u32 field"),
    )
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("fixed u64 field"),
    )
}

fn auxiliary_payload_locator(bytes: &[u8], kind: u16, label_name_sym: u32) -> (u64, u64) {
    let (directory_offset, directory_len) =
        trailer_locator(bytes, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    let directory_start = directory_offset as usize;
    let record_count = (directory_len as usize - 64) / 40;
    for record_index in 0..record_count {
        let record = directory_start + 64 + record_index * 40;
        if read_u16(bytes, record) == kind && read_u32(bytes, record + 4) == label_name_sym {
            return (read_u64(bytes, record + 8), read_u64(bytes, record + 16));
        }
    }
    panic!("auxiliary payload record is missing");
}

fn encoded_unresolved_fst_index() -> Vec<u8> {
    let mut builder = fst::SetBuilder::memory();
    builder
        .insert("ghost")
        .expect("insert unresolved FST fixture value");
    let mut label_values = LabelValueFstIndex::default();
    label_values.insert_fst(
        LABEL_NAME_SYM,
        builder.into_inner().expect("finish unresolved FST fixture"),
    );
    let indexes = SegmentIndexes {
        label_values,
        ..SegmentIndexes::default()
    };
    let mut bytes = Vec::new();
    write_segment_indexes_v7(&mut bytes, &indexes).expect("encode unresolved FST fixture");
    bytes
}

#[test]
fn governed_fst_and_time_ranges_read_each_touched_payload_once() {
    let bytes = encoded_auxiliary_index();
    let (_, fst_len) =
        auxiliary_payload_locator(&bytes, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, LABEL_NAME_SYM);
    let (_, ranges_len) = auxiliary_payload_locator(
        &bytes,
        SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
        LABEL_NAME_SYM,
    );
    let fixture = fixture(
        "schema6-index-auxiliary-payloads",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open auxiliary payload reader");
    let session = reader
        .query_session()
        .expect("open auxiliary payload session");
    let root = session.load_root().expect("load auxiliary payload root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load auxiliary payload directory");
    let before_pages = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

    let fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("load label-value FST")
        .expect("label-value FST exists");
    let symbols = symbol_session(&fixture.registered);
    assert!(fst.charged_bytes() > 0);
    let mut all_values = Vec::new();
    assert!(
        session
            .visit_label_values_with_prefix(&fst, &symbols, None, |symbol_id, value| {
                all_values.push((symbol_id, value.to_string()));
                true
            })
            .expect("visit all FST values")
    );
    assert_eq!(
        all_values,
        [
            (0, "api".to_owned()),
            (fixture.symbol_count - 1, "worker".to_owned()),
        ]
    );

    let mut prefixed = Vec::new();
    assert!(
        session
            .visit_label_values_with_prefix(&fst, &symbols, Some("ap"), |symbol_id, value| {
                assert_eq!(symbol_id, 0);
                prefixed.push(value.to_string());
                true
            })
            .expect("visit prefix FST values")
    );
    assert_eq!(prefixed, ["api"]);

    let mut stopped = Vec::new();
    assert!(
        !session
            .visit_label_values_with_prefix(&fst, &symbols, None, |symbol_id, value| {
                assert_eq!(symbol_id, 0);
                stopped.push(value.to_string());
                false
            })
            .expect("stop FST visitor after its cap")
    );
    assert_eq!(stopped, ["api"]);
    let after_fst = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after_fst.calls - before_pages.calls, 1);
    assert_eq!(after_fst.bytes - before_pages.bytes, fst_len);

    let ranges = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect("load label-value time ranges")
        .expect("label-value time ranges exist");
    assert!(ranges.charged_bytes() > 0);
    assert_eq!(
        session
            .label_value_time_ranges(&ranges)
            .expect("borrow decoded time ranges"),
        &[
            (
                11,
                LabelValueTimeRange {
                    min_time_ms: 100,
                    max_time_ms: 199,
                },
            ),
            (
                12,
                LabelValueTimeRange {
                    min_time_ms: 300,
                    max_time_ms: 399,
                },
            ),
        ]
    );
    assert_eq!(
        session
            .label_value_time_range(&ranges, 12)
            .expect("look up one decoded time range"),
        Some(LabelValueTimeRange {
            min_time_ms: 300,
            max_time_ms: 399,
        })
    );
    assert_eq!(
        session
            .label_value_time_range(&ranges, u32::MAX)
            .expect("missing decoded time range is a clean miss"),
        None
    );
    let after_ranges = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after_ranges.calls - after_fst.calls, 1);
    assert_eq!(after_ranges.bytes - after_fst.bytes, ranges_len);

    let repeated_fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("reuse label-value FST")
        .expect("reused FST exists");
    let repeated_ranges = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect("reuse label-value time ranges")
        .expect("reused time ranges exist");
    assert_eq!(
        session
            .label_value_time_ranges(&repeated_ranges)
            .expect("consume reused ranges"),
        session
            .label_value_time_ranges(&ranges)
            .expect("consume original ranges")
    );
    assert!(
        session
            .visit_label_values_with_prefix(&repeated_fst, &symbols, Some("missing"), |_, _| true,)
            .expect("consume reused FST")
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after_ranges
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn missing_auxiliary_payloads_are_clean_misses_without_page_io() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-missing-auxiliary-payload",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open missing auxiliary payload reader");
    let session = reader
        .query_session()
        .expect("open missing auxiliary payload session");
    let root = session
        .load_root()
        .expect("load missing auxiliary payload root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load missing auxiliary payload directory");

    assert!(
        session
            .load_label_value_fst(&directory, u32::MAX)
            .expect("missing FST is a clean miss")
            .is_none()
    );
    assert!(
        session
            .load_label_value_time_ranges(&directory, u32::MAX)
            .expect("missing time ranges are a clean miss")
            .is_none()
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        MetadataIssuedReadCount::default()
    );
}

#[test]
fn invalid_fst_payload_is_sticky_across_auxiliary_accessors() {
    let mut bytes = encoded_auxiliary_index();
    let (fst_offset, fst_len) =
        auxiliary_payload_locator(&bytes, SEGMENT_INDEX_BLOB_LABEL_VALUE_FST, LABEL_NAME_SYM);
    bytes[fst_offset as usize..(fst_offset + fst_len) as usize].fill(0xff);
    let fixture = fixture(
        "schema6-index-corrupt-auxiliary-fst",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("root remains valid when a lazy FST is corrupt");
    let session = reader.query_session().expect("open corrupt FST session");
    let root = session.load_root().expect("load corrupt FST root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load directory before touching corrupt FST");

    let error = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect_err("invalid FST must fail publication");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_error = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.label_time_range(&directory, u32::MAX),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    let repeated = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect_err("sticky FST corruption must poison another index accessor");
    assert!(matches!(
        repeated,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after_error);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn invalid_time_range_payload_is_sticky_after_one_page_read() {
    let mut bytes = encoded_auxiliary_index();
    let (ranges_offset, ranges_len) = auxiliary_payload_locator(
        &bytes,
        SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
        LABEL_NAME_SYM,
    );
    bytes[ranges_offset as usize..ranges_offset as usize + 4]
        .copy_from_slice(&u32::MAX.to_le_bytes());
    let fixture = fixture(
        "schema6-index-corrupt-auxiliary-ranges",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("root remains valid when lazy time ranges are corrupt");
    let session = reader
        .query_session()
        .expect("open corrupt time-range session");
    let root = session.load_root().expect("load corrupt time-range root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load directory before touching corrupt time ranges");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

    let error = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect_err("invalid time-range count must fail before allocation");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after.calls - before.calls, 1);
    assert_eq!(after.bytes - before.bytes, ranges_len);
    let after_error = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.load_label_value_fst(&directory, LABEL_NAME_SYM),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after_error);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn label_value_time_range_symbols_are_bound_to_the_authoritative_symbol_root() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture_with_metadata(
        "schema6-index-auxiliary-range-symbol-bound",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        0,
        12,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open auxiliary range symbol-bound reader");
    let session = reader
        .query_session()
        .expect("open auxiliary range symbol-bound session");
    let root = session
        .load_root()
        .expect("load auxiliary range symbol-bound root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load symbol-bound auxiliary directory");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

    let error = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect_err("label-value symbol equal to symbol count must fail");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after.calls - before.calls, 1);
    assert!(matches!(
        session.load_label_value_time_ranges(&directory, LABEL_NAME_SYM),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn auxiliary_payload_budget_refusal_precedes_io_and_retries() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-payload-budget",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open auxiliary payload budget reader");
    let session = reader
        .query_session()
        .expect("open auxiliary payload budget session");
    let root = session
        .load_root()
        .expect("load auxiliary payload budget root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load auxiliary payload budget directory");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let blocker = block_all_but_one_byte(&fixture.runtime);

    let error = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect_err("FST budget must be refused before I/O");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    drop(blocker);

    let fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("retry FST after releasing budget")
        .expect("retry FST exists");
    let symbols = symbol_session(&fixture.registered);
    assert!(
        session
            .visit_label_values_with_prefix(&fst, &symbols, None, |_, _| true)
            .expect("consume retry FST")
    );
    let after_fst = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after_fst.calls - before.calls, 1);

    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect_err("time-range budget must be refused before I/O");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after_fst
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    drop(blocker);

    let ranges = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect("retry time ranges after releasing budget")
        .expect("retry time ranges exist");
    assert_eq!(
        session
            .label_value_time_ranges(&ranges)
            .expect("consume retry time ranges")
            .len(),
        2
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls - after_fst.calls,
        1
    );
}

#[test]
fn zero_retention_reissues_auxiliary_payloads_after_final_pin_drop() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-payload-zero-retention",
        runtime(0, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open zero-retention payload reader");
    let session = reader
        .query_session()
        .expect("open zero-retention payload session");
    let root = session.load_root().expect("load zero-retention root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load zero-retention directory");

    let first_fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("load first zero-retention FST")
        .expect("first FST exists");
    let second_fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("reuse live zero-retention FST")
        .expect("second FST exists");
    let after_fst = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    drop(first_fst);
    drop(second_fst);
    let third_fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("reload FST after final pin drops")
        .expect("third FST exists");
    let symbols = symbol_session(&fixture.registered);
    assert!(
        session
            .visit_label_values_with_prefix(&third_fst, &symbols, None, |_, _| true)
            .expect("consume reloaded FST")
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls - after_fst.calls,
        1
    );

    let first_ranges = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect("load first zero-retention ranges")
        .expect("first ranges exist");
    let after_ranges = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    drop(first_ranges);
    let second_ranges = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect("reload ranges after final pin drops")
        .expect("second ranges exist");
    assert_eq!(
        session
            .label_value_time_ranges(&second_ranges)
            .expect("consume reloaded ranges")
            .len(),
        2
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls - after_ranges.calls,
        1
    );
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
}

#[test]
fn auxiliary_payload_pins_cannot_cross_segment_generations() {
    let bytes = encoded_auxiliary_index();
    let runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture("schema6-index-payload-first", runtime.clone(), &bytes);
    let second = fixture("schema6-index-payload-second", runtime, &bytes);
    let first_reader = GovernedSchema6IndexReader::open(&first.registered)
        .expect("open first auxiliary payload reader");
    let second_reader = GovernedSchema6IndexReader::open(&second.registered)
        .expect("open second auxiliary payload reader");
    let first_session = first_reader.query_session().expect("open first session");
    let second_session = second_reader.query_session().expect("open second session");
    let first_root = first_session.load_root().expect("load first root");
    let first_bound = bind_index_root(&first, &first_session, first_root);
    let first_directory = first_session
        .load_auxiliary_directory(&first_bound)
        .expect("load first directory");
    let fst = first_session
        .load_label_value_fst(&first_directory, LABEL_NAME_SYM)
        .expect("load first FST")
        .expect("first FST exists");
    let ranges = first_session
        .load_label_value_time_ranges(&first_directory, LABEL_NAME_SYM)
        .expect("load first time ranges")
        .expect("first time ranges exist");
    let second_symbols = symbol_session(&second.registered);
    let before = second.runtime.snapshot().reads;

    assert!(matches!(
        second_session.visit_label_values_with_prefix(&fst, &second_symbols, None, |_, _| true,),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert!(matches!(
        second_session.label_value_time_ranges(&ranges),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(second.runtime.snapshot().reads, before);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn concurrent_auxiliary_payload_loads_single_flight_each_range() {
    const THREADS: usize = 8;

    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-payload-concurrent",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = Arc::new(
        GovernedSchema6IndexReader::open(&fixture.registered)
            .expect("open concurrent auxiliary payload reader"),
    );
    let setup_session = reader
        .query_session()
        .expect("open auxiliary payload setup session");
    let setup_root = setup_session
        .load_root()
        .expect("load auxiliary payload setup root");
    let bound = Arc::new(bind_index_root(&fixture, &setup_session, setup_root));
    drop(setup_session);
    let barrier = Arc::new(Barrier::new(THREADS));
    let handles = (0..THREADS)
        .map(|_| {
            let reader = Arc::clone(&reader);
            let bound = Arc::clone(&bound);
            let barrier = Arc::clone(&barrier);
            let registered = fixture.registered.clone();
            thread::spawn(move || {
                let session = reader.query_session().expect("open worker session");
                let symbols = symbol_session(&registered);
                barrier.wait();
                let directory = session
                    .load_auxiliary_directory(&bound)
                    .expect("load worker directory");
                let fst = session
                    .load_label_value_fst(&directory, LABEL_NAME_SYM)
                    .expect("load worker FST")
                    .expect("worker FST exists");
                assert!(
                    session
                        .visit_label_values_with_prefix(&fst, &symbols, Some("ap"), |_, _| true,)
                        .expect("visit worker FST")
                );
                let ranges = session
                    .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
                    .expect("load worker time ranges")
                    .expect("worker time ranges exist");
                assert_eq!(
                    session
                        .label_value_time_ranges(&ranges)
                        .expect("consume worker time ranges")
                        .len(),
                    2
                );
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("auxiliary payload worker completes");
    }

    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls,
        1
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls,
        2
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(fixture.runtime.snapshot().cache.active_loads, 0);
}

#[test]
fn auxiliary_payload_pins_reject_substituted_record_context_without_io() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-payload-substitution",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open substituted auxiliary payload reader");
    let session = reader
        .query_session()
        .expect("open substituted auxiliary payload session");
    let root = session
        .load_root()
        .expect("load substituted auxiliary payload root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load substituted auxiliary payload directory");
    let mut fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("load substituted FST")
        .expect("substituted FST exists");
    let mut ranges = session
        .load_label_value_time_ranges(&directory, LABEL_NAME_SYM)
        .expect("load substituted time ranges")
        .expect("substituted time ranges exist");
    fst.substitute_record_for_test();
    ranges.substitute_record_for_test();
    let symbols = symbol_session(&fixture.registered);
    let before = fixture.runtime.snapshot().reads;

    assert!(matches!(
        session.visit_label_values_with_prefix(&fst, &symbols, None, |_, _| true),
        Err(Schema6IndexReaderError::ForeignRootContext)
    ));
    assert!(matches!(
        session.label_value_time_ranges(&ranges),
        Err(Schema6IndexReaderError::ForeignRootContext)
    ));
    assert_eq!(fixture.runtime.snapshot().reads, before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn foreign_symbol_session_fails_before_payload_io_without_poisoning() {
    let bytes = encoded_auxiliary_index();
    let runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture("schema6-index-fst-symbol-first", runtime.clone(), &bytes);
    let second = fixture("schema6-index-fst-symbol-second", runtime, &bytes);
    let reader = GovernedSchema6IndexReader::open(&first.registered)
        .expect("open first symbol-bound FST reader");
    let session = reader
        .query_session()
        .expect("open first symbol-bound FST session");
    let root = session.load_root().expect("load first symbol-bound root");
    let bound = bind_index_root(&first, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load first symbol-bound directory");
    let fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("load first symbol-bound FST")
        .expect("first symbol-bound FST exists");
    let foreign_symbols = symbol_session(&second.registered);
    let before = first.runtime.snapshot().reads;

    assert!(matches!(
        session.visit_label_values_with_prefix(&fst, &foreign_symbols, None, |_, _| true),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(first.runtime.snapshot().reads, before);
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn unresolved_fst_value_is_sticky_index_corruption_not_an_empty_expansion() {
    let bytes = encoded_unresolved_fst_index();
    let fixture = fixture(
        "schema6-index-unresolved-fst-value",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open unresolved FST reader");
    let session = reader.query_session().expect("open unresolved FST session");
    let root = session.load_root().expect("load unresolved FST root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load unresolved FST directory");
    let fst = session
        .load_label_value_fst(&directory, LABEL_NAME_SYM)
        .expect("load structurally valid unresolved FST")
        .expect("unresolved FST exists");
    let symbols = symbol_session(&fixture.registered);
    let mut visited = false;

    assert!(matches!(
        session.visit_label_values_with_prefix(&fst, &symbols, None, |_, _| {
            visited = true;
            true
        }),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert!(!visited);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    symbols
        .visit_required_resolved(0, |value| {
            assert_eq!(value, "api");
            Ok(())
        })
        .expect("symbol artifact remains healthy");

    let after = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.visit_label_values_with_prefix(&fst, &symbols, None, |_, _| true),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after);
}
