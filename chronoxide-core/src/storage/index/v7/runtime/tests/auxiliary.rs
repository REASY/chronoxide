use fst::SetBuilder;

use crate::storage::index::v7::TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET;
use crate::storage::index::{LabelValueFstIndex, LabelValueTimeRangeIndex};

use super::*;

pub(super) const LABEL_NAME_SYM: u32 = 7;

pub(super) fn encoded_auxiliary_index() -> Vec<u8> {
    let mut builder = SetBuilder::memory();
    builder.insert("api").expect("insert first FST value");
    builder.insert("worker").expect("insert second FST value");
    let mut label_values = LabelValueFstIndex::default();
    label_values.insert_fst(
        LABEL_NAME_SYM,
        builder.into_inner().expect("finish auxiliary FST"),
    );
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    label_value_time_ranges.insert(LABEL_NAME_SYM, 11, 100, 199);
    label_value_time_ranges.insert(LABEL_NAME_SYM, 12, 300, 399);
    let indexes = SegmentIndexes {
        label_values,
        label_value_time_ranges,
        ..SegmentIndexes::default()
    };
    let mut bytes = Vec::new();
    write_segment_indexes_v7(&mut bytes, &indexes).expect("encode auxiliary index fixture");
    bytes
}

#[test]
fn auxiliary_directory_reads_once_and_exposes_only_validated_summaries() {
    let bytes = encoded_auxiliary_index();
    let (directory_offset, directory_len) =
        trailer_locator(&bytes, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    assert_ne!(directory_offset, 0);
    let fixture = fixture(
        "schema6-index-auxiliary",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open auxiliary index reader");
    let session = reader.query_session().expect("open auxiliary session");
    let root = session.load_root().expect("load auxiliary root");
    let bound = bind_index_root(&fixture, &session, root);
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);

    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load auxiliary directory");
    assert!(directory.charged_bytes() > 0);
    assert!(
        session
            .has_label_values(&directory)
            .expect("read label-value presence")
    );
    assert_eq!(
        session
            .label_name_symbols(&directory)
            .expect("read label name symbols")
            .collect::<Vec<_>>(),
        vec![LABEL_NAME_SYM]
    );
    assert_eq!(
        session
            .label_time_range(&directory, LABEL_NAME_SYM)
            .expect("read label summary"),
        Some(LabelValueTimeRange {
            min_time_ms: 100,
            max_time_ms: 399,
        })
    );
    assert_eq!(
        session
            .label_time_range(&directory, u32::MAX)
            .expect("missing label summary is a clean miss"),
        None
    );
    let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(after.calls - before.calls, 1);
    assert_eq!(after.bytes - before.bytes, directory_len);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        MetadataIssuedReadCount::default()
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange),
        MetadataIssuedReadCount::default()
    );

    let repeated = session
        .load_auxiliary_directory(&bound)
        .expect("reuse auxiliary directory");
    assert_eq!(
        session
            .label_name_symbols(&repeated)
            .expect("consume reused directory")
            .collect::<Vec<_>>(),
        vec![LABEL_NAME_SYM]
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        after
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn required_empty_auxiliary_directory_is_a_cached_clean_result() {
    let bytes = encoded_index();
    let (_, directory_len) = trailer_locator(&bytes, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    assert_eq!(directory_len, 64);
    let fixture = fixture(
        "schema6-index-empty-auxiliary",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open empty auxiliary reader");
    let session = reader
        .query_session()
        .expect("open empty auxiliary session");
    let root = session.load_root().expect("load empty auxiliary root");
    let bound = bind_index_root(&fixture, &session, root);
    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("load required empty directory");

    assert!(
        !session
            .has_label_values(&directory)
            .expect("read empty state")
    );
    assert!(
        session
            .label_name_symbols(&directory)
            .expect("read empty symbols")
            .len()
            == 0
    );
    assert_eq!(
        session
            .label_time_range(&directory, LABEL_NAME_SYM)
            .expect("read missing summary"),
        None
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: directory_len,
        }
    );
}

#[test]
fn auxiliary_directory_corruption_is_sticky_without_payload_io() {
    let mut bytes = encoded_auxiliary_index();
    let (directory_offset, _) = trailer_locator(&bytes, TRAILER_AUX_DIRECTORY_LOCATOR_OFFSET);
    bytes[directory_offset as usize] ^= 1;
    let fixture = fixture(
        "schema6-index-corrupt-auxiliary",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("root remains valid when the lazy auxiliary directory is corrupt");
    let session = reader
        .query_session()
        .expect("open corrupt auxiliary session");
    let root = session.load_root().expect("load corrupt auxiliary root");
    let bound = bind_index_root(&fixture, &session, root);

    let error = session
        .load_auxiliary_directory(&bound)
        .expect_err("touched directory corruption must fail");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_error = fixture.runtime.snapshot().reads;
    let repeated = session
        .load_auxiliary_directory(&bound)
        .expect_err("sticky directory corruption must fail without another read");
    assert!(matches!(
        repeated,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after_error);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        MetadataIssuedReadCount::default()
    );
}

#[test]
fn auxiliary_directory_symbols_are_bound_to_the_authoritative_symbol_root() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture_with_metadata(
        "schema6-index-auxiliary-symbol-bound",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        0,
        LABEL_NAME_SYM,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open auxiliary symbol-bound reader");
    let session = reader
        .query_session()
        .expect("open auxiliary symbol-bound session");
    let root = session
        .load_root()
        .expect("load auxiliary symbol-bound root");
    let bound = bind_index_root(&fixture, &session, root);
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);

    let error = session
        .load_auxiliary_directory(&bound)
        .expect_err("label-name symbol equal to symbol count must fail");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    assert_eq!(after.calls - before.calls, 1);
    assert!(matches!(
        session.load_auxiliary_directory(&bound),
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
fn auxiliary_directory_budget_refusal_precedes_io_and_is_retryable() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-budget",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open auxiliary-budget reader");
    let session = reader
        .query_session()
        .expect("open auxiliary-budget session");
    let root = session.load_root().expect("load auxiliary-budget root");
    let bound = bind_index_root(&fixture, &session, root);
    let before = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let blocker = block_all_but_one_byte(&fixture.runtime);

    let error = session
        .load_auxiliary_directory(&bound)
        .expect_err("directory budget must be refused before I/O");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        before
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    drop(blocker);

    let directory = session
        .load_auxiliary_directory(&bound)
        .expect("retry directory after releasing budget");
    assert!(session.has_label_values(&directory).expect("consume retry"));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls - before.calls,
        1
    );
}

#[test]
fn zero_retention_reuses_live_auxiliary_pin_then_reissues_after_drop() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-zero-retention",
        runtime(0, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open zero-retention auxiliary reader");
    let session = reader
        .query_session()
        .expect("open zero-retention auxiliary session");
    let root = session.load_root().expect("load zero-retention root");
    let bound = bind_index_root(&fixture, &session, root);

    let first = session
        .load_auxiliary_directory(&bound)
        .expect("load first auxiliary pin");
    let after_first = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let second = session
        .load_auxiliary_directory(&bound)
        .expect("reuse live auxiliary pin");
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        after_first
    );
    drop(first);
    drop(second);

    let third = session
        .load_auxiliary_directory(&bound)
        .expect("reload after final auxiliary pin drops");
    assert!(session.has_label_values(&third).expect("consume reload"));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls - after_first.calls,
        1
    );
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
}

#[test]
fn concurrent_auxiliary_directory_loads_single_flight() {
    const THREADS: usize = 8;

    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-concurrent",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = Arc::new(
        GovernedSchema6IndexReader::open(&fixture.registered)
            .expect("open concurrent auxiliary reader"),
    );
    let setup_session = reader
        .query_session()
        .expect("open auxiliary setup session");
    let setup_root = setup_session
        .load_root()
        .expect("load auxiliary setup root");
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
                let directory = session
                    .load_auxiliary_directory(&bound)
                    .expect("load worker auxiliary directory");
                assert!(
                    session
                        .has_label_values(&directory)
                        .expect("consume worker directory")
                );
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("auxiliary worker completes");
    }

    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls,
        1
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(fixture.runtime.snapshot().cache.active_loads, 0);
}

#[test]
fn auxiliary_directory_pin_cannot_cross_segment_generations() {
    let bytes = encoded_auxiliary_index();
    let runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture("schema6-index-auxiliary-first", runtime.clone(), &bytes);
    let second = fixture("schema6-index-auxiliary-second", runtime, &bytes);
    let first_reader =
        GovernedSchema6IndexReader::open(&first.registered).expect("open first auxiliary reader");
    let second_reader =
        GovernedSchema6IndexReader::open(&second.registered).expect("open second auxiliary reader");
    let first_session = first_reader.query_session().expect("open first session");
    let second_session = second_reader.query_session().expect("open second session");
    let first_root = first_session.load_root().expect("load first root");
    let first_bound = bind_index_root(&first, &first_session, first_root);
    let directory = first_session
        .load_auxiliary_directory(&first_bound)
        .expect("load first auxiliary directory");
    let before = second.runtime.snapshot().reads;

    assert!(matches!(
        second_session.has_label_values(&directory),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(second.runtime.snapshot().reads, before);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn auxiliary_directory_pin_rejects_substituted_root_context_without_io() {
    let bytes = encoded_auxiliary_index();
    let fixture = fixture(
        "schema6-index-auxiliary-substituted-root",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open substituted-root auxiliary reader");
    let session = reader
        .query_session()
        .expect("open substituted-root session");
    let root = session.load_root().expect("load substituted-root root");
    let bound = bind_index_root(&fixture, &session, root);
    let mut directory = session
        .load_auxiliary_directory(&bound)
        .expect("load substituted-root directory");
    let before = fixture.runtime.snapshot().reads;
    directory.substitute_root_for_test();

    assert!(matches!(
        session.has_label_values(&directory),
        Err(Schema6IndexReaderError::ForeignRootContext)
    ));
    assert_eq!(fixture.runtime.snapshot().reads, before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}
