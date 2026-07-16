use crate::storage::index::v7::TRAILER_METRIC_LOCATOR_OFFSET;
use crate::storage::index::{MetricSeriesRange, MetricSeriesRangeIndex};

use super::*;

const FIRST_METRIC_SYM: u32 = 100;
const SECOND_METRIC_SYM: u32 = 200;

fn encoded_metric_index() -> Vec<u8> {
    let mut metric_series_ranges = MetricSeriesRangeIndex::default();
    metric_series_ranges.insert_range(
        FIRST_METRIC_SYM,
        MetricSeriesRange {
            start_series_ref: 0,
            series_count: 1,
            kind_mask: 1,
            min_time_ms: 100,
            max_time_ms: 199,
        },
    );
    metric_series_ranges.insert_range(
        FIRST_METRIC_SYM,
        MetricSeriesRange {
            start_series_ref: 1,
            series_count: 1,
            kind_mask: 2,
            min_time_ms: 150,
            max_time_ms: 249,
        },
    );
    metric_series_ranges.insert_range(
        SECOND_METRIC_SYM,
        MetricSeriesRange {
            start_series_ref: 2,
            series_count: 3,
            kind_mask: 4,
            min_time_ms: 200,
            max_time_ms: 299,
        },
    );
    let indexes = SegmentIndexes {
        metric_series_ranges,
        ..SegmentIndexes::default()
    };
    let mut bytes = Vec::new();
    write_segment_indexes_v7(&mut bytes, &indexes).expect("encode metric-range fixture");
    bytes
}

fn mutate_metric_u32(bytes: &mut [u8], relative_offset: usize, value: u32) {
    let (metric_offset, _) = trailer_locator(bytes, TRAILER_METRIC_LOCATOR_OFFSET);
    let offset =
        usize::try_from(metric_offset).expect("metric offset fits usize") + relative_offset;
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn mutate_metric_u16(bytes: &mut [u8], relative_offset: usize, value: u16) {
    let (metric_offset, _) = trailer_locator(bytes, TRAILER_METRIC_LOCATOR_OFFSET);
    let offset =
        usize::try_from(metric_offset).expect("metric offset fits usize") + relative_offset;
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

#[test]
fn governed_metric_ranges_read_once_and_visit_only_the_requested_group() {
    let bytes = encoded_metric_index();
    let (_, metric_len) = trailer_locator(&bytes, TRAILER_METRIC_LOCATOR_OFFSET);
    let fixture = fixture_with_metadata(
        "schema6-index-metric-ranges",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        5,
        201,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open metric-range reader");
    let session = reader.query_session().expect("open metric-range session");
    let root = session.load_root().expect("load metric-range root");
    let bound = bind_index_root(&fixture, &session, root);
    let before = class_reads(&fixture.runtime, MetadataCacheClass::MetricRange);

    let ranges = session
        .load_metric_series_ranges(&bound)
        .expect("load governed metric ranges");
    assert!(ranges.charged_bytes() > 0);
    let mut first = Vec::new();
    assert!(
        session
            .visit_metric_series_ranges(&ranges, FIRST_METRIC_SYM, |range| {
                first.push(range);
                true
            })
            .expect("visit first metric ranges")
    );
    assert_eq!(
        first,
        [
            MetricSeriesRange {
                start_series_ref: 0,
                series_count: 1,
                kind_mask: 1,
                min_time_ms: 100,
                max_time_ms: 199,
            },
            MetricSeriesRange {
                start_series_ref: 1,
                series_count: 1,
                kind_mask: 2,
                min_time_ms: 150,
                max_time_ms: 249,
            },
        ]
    );
    let mut second = Vec::new();
    assert!(
        session
            .visit_metric_series_ranges(&ranges, SECOND_METRIC_SYM, |range| {
                second.push(range);
                true
            })
            .expect("visit second metric ranges")
    );
    assert_eq!(second.len(), 1);
    let mut stopped = Vec::new();
    assert!(
        !session
            .visit_metric_series_ranges(&ranges, FIRST_METRIC_SYM, |range| {
                stopped.push(range);
                false
            })
            .expect("stop metric visitor after its cap")
    );
    assert_eq!(stopped.len(), 1);
    let mut missing = Vec::new();
    assert!(
        session
            .visit_metric_series_ranges(&ranges, u32::MAX, |range| {
                missing.push(range);
                true
            })
            .expect("missing metric is a clean exhausted visit")
    );
    assert!(missing.is_empty());
    let after = class_reads(&fixture.runtime, MetadataCacheClass::MetricRange);
    assert_eq!(after.calls - before.calls, 1);
    assert_eq!(after.bytes - before.bytes, metric_len);

    let repeated = session
        .load_metric_series_ranges(&bound)
        .expect("reuse metric ranges");
    assert!(
        session
            .visit_metric_series_ranges(&repeated, SECOND_METRIC_SYM, |_| true)
            .expect("consume reused metric ranges")
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange),
        after
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn required_empty_metric_range_blob_is_valid_for_an_empty_series_root() {
    let bytes = encoded_index();
    let (_, metric_len) = trailer_locator(&bytes, TRAILER_METRIC_LOCATOR_OFFSET);
    assert_eq!(metric_len, 12);
    let fixture = fixture(
        "schema6-index-empty-metric-ranges",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open empty metric-range reader");
    let session = reader
        .query_session()
        .expect("open empty metric-range session");
    let root = session.load_root().expect("load empty metric-range root");
    let bound = bind_index_root(&fixture, &session, root);
    let ranges = session
        .load_metric_series_ranges(&bound)
        .expect("load required empty metric-range blob");
    let mut visited = false;
    assert!(
        session
            .visit_metric_series_ranges(&ranges, FIRST_METRIC_SYM, |_| {
                visited = true;
                true
            })
            .expect("visit empty metric ranges")
    );
    assert!(!visited);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: metric_len,
        }
    );
}

#[test]
fn metric_range_series_bounds_are_sticky_and_foreign_bindings_are_not() {
    let bytes = encoded_metric_index();
    let corrupt = fixture_with_metadata(
        "schema6-index-metric-range-bounds",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        4,
        201,
    );
    let reader = GovernedSchema6IndexReader::open(&corrupt.registered)
        .expect("open metric-range bounds reader");
    let session = reader
        .query_session()
        .expect("open metric-range bounds session");
    let root = session.load_root().expect("load metric-range bounds root");
    let too_small = bind_index_root(&corrupt, &session, root);

    let error = session
        .load_metric_series_ranges(&too_small)
        .expect_err("range ending at five exceeds a four-series root");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_error = corrupt.runtime.snapshot().reads;
    assert!(matches!(
        session.load_auxiliary_directory(&too_small),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(corrupt.runtime.snapshot().reads, after_error);
    assert_eq!(corrupt.runtime.snapshot().cache.sticky_artifacts, 1);

    let shared_runtime = runtime(1024 * 1024, 1024 * 1024);
    let valid = fixture_with_metadata(
        "schema6-index-metric-range-binding",
        shared_runtime.clone(),
        &bytes,
        5,
        201,
    );
    let foreign = fixture_with_metadata(
        "schema6-index-metric-range-foreign-binding",
        shared_runtime,
        &bytes,
        6,
        201,
    );
    let valid_reader = GovernedSchema6IndexReader::open(&valid.registered)
        .expect("open valid metric-range binding reader");
    let valid_session = valid_reader
        .query_session()
        .expect("open valid metric-range binding session");
    let valid_root = valid_session
        .load_root()
        .expect("load valid metric-range binding root");
    let first_bound = bind_index_root(&valid, &valid_session, valid_root);
    valid_session
        .load_metric_series_ranges(&first_bound)
        .expect("load under first metric series count");
    let (foreign_series, foreign_symbols) = metadata_bindings(&foreign);
    let rebound_root = valid_session
        .load_root()
        .expect("reuse valid metric root before foreign binding");
    let before_binding_error = valid.runtime.snapshot().reads;
    assert!(matches!(
        valid_session.bind_segment_roots(rebound_root, foreign_series, foreign_symbols),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(valid.runtime.snapshot().reads, before_binding_error);
    assert_eq!(valid.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn metric_ranges_reject_incomplete_partitions_unknown_kinds_and_foreign_symbols() {
    const FIRST_RANGE_START: usize = 20;
    const FIRST_RANGE_KIND: usize = 28;
    const SECOND_RANGE_START: usize = 48;
    const SECOND_GROUP_RANGE_START: usize = 84;

    let mut leading_gap = encoded_metric_index();
    mutate_metric_u32(&mut leading_gap, FIRST_RANGE_START, 1);
    let mut internal_gap = encoded_metric_index();
    mutate_metric_u32(&mut internal_gap, SECOND_RANGE_START, 2);
    let mut overlap = encoded_metric_index();
    mutate_metric_u32(&mut overlap, SECOND_RANGE_START, 0);
    let mut cross_group_overlap = encoded_metric_index();
    mutate_metric_u32(&mut cross_group_overlap, SECOND_GROUP_RANGE_START, 1);
    let mut zero_kind = encoded_metric_index();
    mutate_metric_u16(&mut zero_kind, FIRST_RANGE_KIND, 0);
    let mut unknown_kind = encoded_metric_index();
    mutate_metric_u16(&mut unknown_kind, FIRST_RANGE_KIND, 0x8000);
    let mut malformed_group_count = encoded_metric_index();
    mutate_metric_u32(&mut malformed_group_count, 8, u32::MAX);

    let cases = [
        ("empty-nonempty", encoded_index(), 1, 1),
        ("leading-gap", leading_gap, 5, 201),
        ("internal-gap", internal_gap, 5, 201),
        ("overlap", overlap, 5, 201),
        ("cross-group-overlap", cross_group_overlap, 5, 201),
        ("trailing-gap", encoded_metric_index(), 6, 201),
        ("zero-kind", zero_kind, 5, 201),
        ("unknown-kind", unknown_kind, 5, 201),
        ("malformed-group-count", malformed_group_count, 5, 201),
        ("foreign-symbol", encoded_metric_index(), 5, 200),
    ];

    for (name, bytes, num_series, symbol_count) in cases {
        let identity = format!("schema6-index-metric-corrupt-{name}");
        let fixture = fixture_with_metadata(
            &identity,
            runtime(1024 * 1024, 1024 * 1024),
            &bytes,
            num_series,
            symbol_count,
        );
        let reader = GovernedSchema6IndexReader::open(&fixture.registered)
            .expect("metric corruption leaves fixed index root valid");
        let session = reader
            .query_session()
            .expect("open metric corruption session");
        let root = session.load_root().expect("load metric corruption root");
        let bound = bind_index_root(&fixture, &session, root);
        let before = class_reads(&fixture.runtime, MetadataCacheClass::MetricRange);

        let error = session
            .load_metric_series_ranges(&bound)
            .expect_err("malformed metric partition must fail");
        assert!(
            matches!(
                error,
                Schema6IndexReaderError::Cache(MetadataCacheError::Structural(_))
            ),
            "unexpected {name} error: {error:?}"
        );
        let after = class_reads(&fixture.runtime, MetadataCacheClass::MetricRange);
        assert_eq!(after.calls - before.calls, 1, "case {name}");
        assert!(matches!(
            session.load_metric_series_ranges(&bound),
            Err(Schema6IndexReaderError::Cache(
                MetadataCacheError::Structural(_)
            ))
        ));
        assert_eq!(
            class_reads(&fixture.runtime, MetadataCacheClass::MetricRange),
            after,
            "case {name} re-read corrupt bytes"
        );
        assert_eq!(
            fixture.runtime.snapshot().cache.sticky_artifacts,
            1,
            "case {name} did not remain sticky"
        );
    }
}

#[test]
fn metric_range_budget_refusal_precedes_io_and_retries() {
    let bytes = encoded_metric_index();
    let fixture = fixture_with_metadata(
        "schema6-index-metric-range-budget",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        5,
        201,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open metric-range budget reader");
    let session = reader
        .query_session()
        .expect("open metric-range budget session");
    let root = session.load_root().expect("load metric-range budget root");
    let bound = bind_index_root(&fixture, &session, root);
    let before = class_reads(&fixture.runtime, MetadataCacheClass::MetricRange);
    let blocker = block_all_but_one_byte(&fixture.runtime);

    let error = session
        .load_metric_series_ranges(&bound)
        .expect_err("metric-range budget must be refused before I/O");
    assert!(matches!(
        error,
        Schema6IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange),
        before
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    drop(blocker);

    let ranges = session
        .load_metric_series_ranges(&bound)
        .expect("retry metric ranges after releasing budget");
    assert!(
        session
            .visit_metric_series_ranges(&ranges, FIRST_METRIC_SYM, |_| true)
            .expect("consume retried metric ranges")
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange).calls - before.calls,
        1
    );
}

#[test]
fn zero_retention_reuses_live_metric_pin_then_reissues_after_drop() {
    let bytes = encoded_metric_index();
    let fixture = fixture_with_metadata(
        "schema6-index-metric-range-zero-retention",
        runtime(0, 1024 * 1024),
        &bytes,
        5,
        201,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open zero-retention metric-range reader");
    let session = reader
        .query_session()
        .expect("open zero-retention metric-range session");
    let root = session.load_root().expect("load zero-retention root");
    let bound = bind_index_root(&fixture, &session, root);

    let first = session
        .load_metric_series_ranges(&bound)
        .expect("load first metric pin");
    let after_first = class_reads(&fixture.runtime, MetadataCacheClass::MetricRange);
    let second = session
        .load_metric_series_ranges(&bound)
        .expect("reuse live metric pin");
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange),
        after_first
    );
    drop(first);
    drop(second);

    let third = session
        .load_metric_series_ranges(&bound)
        .expect("reload metric ranges after final pin drops");
    assert!(
        session
            .visit_metric_series_ranges(&third, FIRST_METRIC_SYM, |_| true)
            .expect("consume reloaded metric ranges")
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange).calls - after_first.calls,
        1
    );
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
}

#[test]
fn concurrent_metric_range_loads_single_flight() {
    const THREADS: usize = 8;

    let bytes = encoded_metric_index();
    let fixture = fixture_with_metadata(
        "schema6-index-metric-range-concurrent",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        5,
        201,
    );
    let reader = Arc::new(
        GovernedSchema6IndexReader::open(&fixture.registered)
            .expect("open concurrent metric-range reader"),
    );
    let setup_session = reader.query_session().expect("open metric setup session");
    let setup_root = setup_session.load_root().expect("load metric setup root");
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
                let ranges = session
                    .load_metric_series_ranges(&bound)
                    .expect("load worker metric ranges");
                assert!(
                    session
                        .visit_metric_series_ranges(&ranges, SECOND_METRIC_SYM, |_| true)
                        .expect("visit worker metric ranges")
                );
            })
        })
        .collect::<Vec<_>>();
    for handle in handles {
        handle.join().expect("metric-range worker completes");
    }

    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::MetricRange).calls,
        1
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(fixture.runtime.snapshot().cache.active_loads, 0);
}

#[test]
fn metric_range_pin_rejects_foreign_generation_and_substituted_context() {
    let bytes = encoded_metric_index();
    let runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture_with_metadata(
        "schema6-index-metric-first",
        runtime.clone(),
        &bytes,
        5,
        201,
    );
    let second = fixture_with_metadata("schema6-index-metric-second", runtime, &bytes, 5, 201);
    let first_reader = GovernedSchema6IndexReader::open(&first.registered)
        .expect("open first metric-range reader");
    let second_reader = GovernedSchema6IndexReader::open(&second.registered)
        .expect("open second metric-range reader");
    let first_session = first_reader.query_session().expect("open first session");
    let second_session = second_reader.query_session().expect("open second session");
    let first_root = first_session.load_root().expect("load first root");
    let first_bound = bind_index_root(&first, &first_session, first_root);
    let mut ranges = first_session
        .load_metric_series_ranges(&first_bound)
        .expect("load first metric ranges");
    let before_second = second.runtime.snapshot().reads;

    assert!(matches!(
        second_session.visit_metric_series_ranges(&ranges, FIRST_METRIC_SYM, |_| true),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(second.runtime.snapshot().reads, before_second);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);

    let before_substitution = first.runtime.snapshot().reads;
    ranges.substitute_context_for_test();
    assert!(matches!(
        first_session.visit_metric_series_ranges(&ranges, FIRST_METRIC_SYM, |_| true),
        Err(Schema6IndexReaderError::ForeignRootContext)
    ));
    assert_eq!(first.runtime.snapshot().reads, before_substitution);
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
}
