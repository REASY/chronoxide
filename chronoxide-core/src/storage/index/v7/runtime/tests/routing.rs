use std::sync::{Arc, Barrier};
use std::thread;

use crate::labels::METRIC_NAME_LABEL;
use crate::storage::index::v7::{
    TRAILER_EXACT_PAGES_LOCATOR_OFFSET, TRAILER_ROUTING_LOCATOR_OFFSET,
};
use crate::storage::index::{
    ExactPostingsIndex, ExactPostingsMetadata, LabelValueTimeRange, LabelValueTimeRangeIndex,
    ROUTING_INDEX_BUCKET_LEN, ROUTING_INDEX_HEADER_LEN, RoutingBucketRecord, RoutingIndexHeader,
    SegmentRoutingIndex, routing_key_bytes, routing_key_hash, routing_key_hash_parts,
};
use crate::storage::series::SegmentSymbols;

use super::super::routing::Schema6RoutingLookupResult;
use super::*;

const LABEL_NAME: &str = METRIC_NAME_LABEL;
const THREAD_COUNT: usize = 12;

struct CollisionFixture {
    bytes: Vec<u8>,
    first_value: String,
    target_value: String,
    expected: ExactPostingsMetadata,
}

fn collision_values() -> (String, String) {
    let values = (0u32..256)
        .map(|index| format!("collision-{index:04}"))
        .collect::<Vec<_>>();
    for (left_index, left) in values.iter().enumerate() {
        let left_hash = routing_key_hash_parts(LABEL_NAME, left).expect("hash first route");
        for right in &values[left_index + 1..] {
            let right_hash = routing_key_hash_parts(LABEL_NAME, right).expect("hash second route");
            if left_hash & 7 == 7 && right_hash & 7 == 7 {
                return (left.clone(), right.clone());
            }
        }
    }
    panic!("find deterministic routing collision")
}

fn encoded_collision_index() -> CollisionFixture {
    let (first_value, target_value) = collision_values();
    let mut symbols = SegmentSymbols::default();
    let label_name_sym = symbols.intern(LABEL_NAME);
    let first_sym = symbols.intern(&first_value);
    let target_sym = symbols.intern(&target_value);
    let mut exact_postings = ExactPostingsIndex::default();
    exact_postings.insert(label_name_sym, first_sym, 0);
    exact_postings.insert(label_name_sym, target_sym, 1);
    let mut ranges = LabelValueTimeRangeIndex::default();
    ranges.insert(label_name_sym, first_sym, 100, 199);
    ranges.insert(label_name_sym, target_sym, 200, 299);
    let routing_index =
        SegmentRoutingIndex::from_indexes(&symbols, &exact_postings, &ranges).unwrap();
    let expected = routing_index
        .exact_postings_metadata(LABEL_NAME, &target_value)
        .expect("target routing metadata");
    let indexes = SegmentIndexes {
        exact_postings,
        label_value_time_ranges: ranges,
        routing_index: Some(routing_index),
        ..SegmentIndexes::default()
    };
    let mut bytes = Vec::new();
    write_segment_indexes_v7(&mut bytes, &indexes).expect("encode routing collision fixture");
    CollisionFixture {
        bytes,
        first_value,
        target_value,
        expected,
    }
}

fn routing_layout(bytes: &[u8]) -> (usize, RoutingIndexHeader) {
    let (offset, length) = trailer_locator(bytes, TRAILER_ROUTING_LOCATOR_OFFSET);
    let offset = usize::try_from(offset).expect("routing offset fits usize");
    let header =
        RoutingIndexHeader::decode(&bytes[offset..offset + ROUTING_INDEX_HEADER_LEN], length)
            .expect("decode routing fixture header");
    (offset, header)
}

fn routing_lookup(
    session: &GovernedSchema6IndexSession,
    root: &GovernedSchema6IndexRoot,
    label_name: &str,
    label_value: &str,
) -> Result<Schema6RoutingLookupResult, Schema6IndexReaderError> {
    let lookup = session.routing_exact_postings_metadata(root, label_name, label_value)?;
    session.routing_lookup_result(&lookup)
}

fn bucket_record(
    bytes: &[u8],
    routing_offset: usize,
    header: RoutingIndexHeader,
    index: u32,
) -> RoutingBucketRecord {
    let relative = usize::try_from(header.bucket_offset(index).expect("routing bucket offset"))
        .expect("routing bucket offset fits usize");
    RoutingBucketRecord::decode(
        &bytes[routing_offset + relative..routing_offset + relative + ROUTING_INDEX_BUCKET_LEN],
    )
    .expect("decode routing fixture bucket")
}

fn corrupt_first_collision_key(bytes: &mut [u8], target_value: &str) {
    let (routing_offset, header) = routing_layout(bytes);
    let target_hash = routing_key_hash_parts(LABEL_NAME, target_value).expect("hash target route");
    let bucket_index = (target_hash as u32) & (header.bucket_count - 1);
    let bucket = bucket_record(bytes, routing_offset, header, bucket_index);
    let key_range = bucket
        .validate_touched(header)
        .expect("validate collision bucket")
        .expect("occupied collision bucket");
    let last_key_byte = routing_offset
        + usize::try_from(key_range.offset).expect("key offset fits usize")
        + key_range.len
        - 1;
    bytes[last_key_byte] ^= 1;
}

fn fill_every_routing_bucket(bytes: &mut [u8]) {
    let (routing_offset, header) = routing_layout(bytes);
    let template = (0..header.bucket_count)
        .find_map(|index| {
            let relative = usize::try_from(header.bucket_offset(index).ok()?).ok()?;
            let start = routing_offset + relative;
            let record: [u8; ROUTING_INDEX_BUCKET_LEN] = bytes
                .get(start..start + ROUTING_INDEX_BUCKET_LEN)?
                .try_into()
                .ok()?;
            (!RoutingBucketRecord::decode(&record).ok()?.is_empty()).then_some(record)
        })
        .expect("routing fixture has an occupied bucket");
    for index in 0..header.bucket_count {
        let relative = usize::try_from(header.bucket_offset(index).expect("routing bucket offset"))
            .expect("routing bucket offset fits usize");
        let start = routing_offset + relative;
        let bucket = RoutingBucketRecord::decode(&bytes[start..start + ROUTING_INDEX_BUCKET_LEN])
            .expect("decode routing bucket");
        if bucket.is_empty() {
            bytes[start..start + ROUTING_INDEX_BUCKET_LEN].copy_from_slice(&template);
        }
    }
}

fn missing_value_starting_at_an_empty_bucket(bytes: &[u8]) -> String {
    let (routing_offset, header) = routing_layout(bytes);
    for index in 0u32..1024 {
        let candidate = format!("missing-{index:04}");
        let hash = routing_key_hash_parts(LABEL_NAME, &candidate).expect("hash missing route");
        let bucket_index = (hash as u32) & (header.bucket_count - 1);
        if bucket_record(bytes, routing_offset, header, bucket_index).is_empty() {
            return candidate;
        }
    }
    panic!("find missing route whose initial bucket is empty")
}

#[test]
fn streaming_routing_hash_matches_the_encoded_key_without_lookup_allocation() {
    let encoded = routing_key_bytes(LABEL_NAME, "request_duration_seconds").unwrap();
    assert_eq!(
        routing_key_hash_parts(LABEL_NAME, "request_duration_seconds").unwrap(),
        routing_key_hash(&encoded)
    );
}

#[test]
fn routing_point_lookup_reads_exact_collision_spans_and_reuses_cache() {
    let encoded = encoded_collision_index();
    assert_eq!(
        routing_key_hash_parts(LABEL_NAME, &encoded.target_value).unwrap() & 7,
        7,
        "the collision must wrap from the final bucket to bucket zero"
    );
    let first_key_len = routing_key_bytes(LABEL_NAME, &encoded.first_value)
        .unwrap()
        .len() as u64;
    let target_key_len = routing_key_bytes(LABEL_NAME, &encoded.target_value)
        .unwrap()
        .len() as u64;
    let fixture = fixture(
        "schema6-routing-point",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded.bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open governed routing reader");
    let session = reader.query_session().expect("open routing session");
    let root = session.load_root().expect("load routing root");
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

    assert_eq!(
        routing_lookup(&session, &root, LABEL_NAME, &encoded.target_value)
            .expect("lookup collided target"),
        Schema6RoutingLookupResult::Match(encoded.expected)
    );

    let after_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let after_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after_directory.calls - before_directory.calls, 1);
    assert_eq!(
        after_directory.bytes - before_directory.bytes,
        ROUTING_INDEX_HEADER_LEN as u64
    );
    assert_eq!(after_page.calls - before_page.calls, 4);
    assert_eq!(
        after_page.bytes - before_page.bytes,
        2 * ROUTING_INDEX_BUCKET_LEN as u64 + first_key_len + target_key_len
    );
    assert!(
        fixture
            .runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Cache(
                MetadataCacheClass::IndexDirectory
            ))
            .retained_bytes
            > 0
    );
    assert!(
        fixture
            .runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Cache(MetadataCacheClass::IndexPage))
            .retained_bytes
            > 0
    );

    assert_eq!(
        routing_lookup(&session, &root, LABEL_NAME, &encoded.target_value)
            .expect("reuse collided target"),
        Schema6RoutingLookupResult::Match(encoded.expected)
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        after_directory
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        after_page
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn routing_point_miss_reads_only_the_exact_header_and_empty_bucket() {
    let encoded = encoded_collision_index();
    let missing = missing_value_starting_at_an_empty_bucket(&encoded.bytes);
    let fixture = fixture(
        "schema6-routing-missing",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded.bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open governed routing-miss reader");
    let session = reader.query_session().expect("open routing-miss session");
    let root = session.load_root().expect("load routing-miss root");
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

    assert_eq!(
        routing_lookup(&session, &root, LABEL_NAME, &missing).expect("lookup missing route"),
        Schema6RoutingLookupResult::Missing
    );

    let after_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let after_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    assert_eq!(after_directory.calls - before_directory.calls, 1);
    assert_eq!(
        after_directory.bytes - before_directory.bytes,
        ROUTING_INDEX_HEADER_LEN as u64
    );
    assert_eq!(after_page.calls - before_page.calls, 1);
    assert_eq!(
        after_page.bytes - before_page.bytes,
        ROUTING_INDEX_BUCKET_LEN as u64
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn absent_routing_locator_is_a_zero_read_result() {
    let fixture = fixture(
        "schema6-routing-absent",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded_index(),
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open routing-absent reader");
    let session = reader.query_session().expect("open routing-absent session");
    let root = session.load_root().expect("load routing-absent root");
    let before = fixture.runtime.snapshot().reads;

    assert_eq!(
        routing_lookup(&session, &root, LABEL_NAME, "missing").expect("routing locator is absent"),
        Schema6RoutingLookupResult::IndexAbsent
    );
    assert_eq!(fixture.runtime.snapshot().reads, before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn absent_and_cached_routing_results_still_observe_later_sticky_corruption() {
    let mut bytes = encoded_exact_index(&[(12, vec![0])]);
    let (page_offset, page_len) = trailer_locator(&bytes, TRAILER_EXACT_PAGES_LOCATOR_OFFSET);
    assert_eq!(page_len, EXACT_PAGE_LEN as u64);
    let record_offset = usize::try_from(page_offset).unwrap() + 16;
    bytes[record_offset] ^= 1;

    let fixture = fixture_with_metadata(
        "schema6-routing-sticky-ledger",
        runtime(1024 * 1024, 1024 * 1024),
        &bytes,
        1,
        32,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open absent-routing reader");
    let session = reader.query_session().expect("open absent-routing session");
    let root = session.load_root().expect("load absent-routing root");
    let lookup = session
        .routing_exact_postings_metadata(&root, LABEL_NAME, "missing")
        .expect("create an index-absent result before another accessor finds corruption");
    assert_eq!(
        session
            .routing_lookup_result(&lookup)
            .expect("read clean index-absent result"),
        Schema6RoutingLookupResult::IndexAbsent
    );

    let bound = bind_index_root(&fixture, &session, root);
    assert!(matches!(
        session.select_exact_postings(&bound, 7, 12),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    let reads_after_corruption = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.routing_lookup_result(&lookup),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, reads_after_corruption);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn routing_budget_refusal_is_transient_and_retryable_without_io() {
    let encoded = encoded_collision_index();
    let fixture = fixture(
        "schema6-routing-budget",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded.bytes,
    );
    let reader =
        GovernedSchema6IndexReader::open(&fixture.registered).expect("open routing budget reader");
    let session = reader.query_session().expect("open routing budget session");
    let root = session.load_root().expect("load routing budget root");
    let before = fixture.runtime.snapshot().reads;
    let blocker = block_all_but_one_byte(&fixture.runtime);

    assert!(matches!(
        routing_lookup(&session, &root, LABEL_NAME, &encoded.target_value),
        Err(Schema6IndexReaderError::Cache(MetadataCacheError::Budget(
            _
        )))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    drop(blocker);
    assert_eq!(
        routing_lookup(&session, &root, LABEL_NAME, &encoded.target_value)
            .expect("retry routing lookup after budget release"),
        Schema6RoutingLookupResult::Match(encoded.expected)
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn zero_retention_reloads_each_exact_routing_span() {
    let encoded = encoded_collision_index();
    let fixture = fixture(
        "schema6-routing-zero-retention",
        runtime(0, 1024 * 1024),
        &encoded.bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open zero-retention routing reader");
    let session = reader
        .query_session()
        .expect("open zero-retention routing session");
    let root = session
        .load_root()
        .expect("load zero-retention routing root");
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);

    for _ in 0..2 {
        assert_eq!(
            routing_lookup(&session, &root, LABEL_NAME, &encoded.target_value)
                .expect("zero-retention routing lookup"),
            Schema6RoutingLookupResult::Match(encoded.expected)
        );
    }

    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls
            - before_directory.calls,
        2
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls - before_page.calls,
        8
    );
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn concurrent_routing_lookups_single_flight_every_exact_span() {
    let encoded = encoded_collision_index();
    let fixture = fixture(
        "schema6-routing-concurrent",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded.bytes,
    );
    let reader = Arc::new(
        GovernedSchema6IndexReader::open(&fixture.registered)
            .expect("open concurrent routing reader"),
    );
    let barrier = Arc::new(Barrier::new(THREAD_COUNT + 1));
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let mut handles = Vec::new();
    for _ in 0..THREAD_COUNT {
        let reader = Arc::clone(&reader);
        let barrier = Arc::clone(&barrier);
        let target = encoded.target_value.clone();
        let expected = encoded.expected;
        handles.push(thread::spawn(move || {
            let session = reader.query_session().expect("open concurrent session");
            let root = session.load_root().expect("load concurrent root");
            barrier.wait();
            assert_eq!(
                routing_lookup(&session, &root, LABEL_NAME, &target)
                    .expect("concurrent routing lookup"),
                Schema6RoutingLookupResult::Match(expected)
            );
        }));
    }
    barrier.wait();
    for handle in handles {
        handle.join().expect("join concurrent routing lookup");
    }

    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory).calls
            - before_directory.calls,
        1
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage).calls - before_page.calls,
        4
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(fixture.runtime.snapshot().cache.active_loads, 0);
}

#[test]
fn routing_lookup_rejects_foreign_generation_and_root_context_without_poisoning() {
    let encoded = encoded_collision_index();
    let runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture("schema6-routing-first", runtime.clone(), &encoded.bytes);
    let second = fixture("schema6-routing-second", runtime, &encoded.bytes);
    let first_reader =
        GovernedSchema6IndexReader::open(&first.registered).expect("open first routing reader");
    let second_reader =
        GovernedSchema6IndexReader::open(&second.registered).expect("open second routing reader");
    let first_session = first_reader
        .query_session()
        .expect("open first routing session");
    let second_session = second_reader
        .query_session()
        .expect("open second routing session");
    let first_root = first_session.load_root().expect("load first routing root");
    let before = second.runtime.snapshot().reads;

    assert!(matches!(
        second_session.routing_exact_postings_metadata(
            &first_root,
            LABEL_NAME,
            &encoded.target_value
        ),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(second.runtime.snapshot().reads, before);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);

    let mut lookup = first_session
        .routing_exact_postings_metadata(&first_root, LABEL_NAME, &encoded.target_value)
        .expect("create root-bound routing result");
    let before_foreign_result = second.runtime.snapshot().reads;
    assert!(matches!(
        second_session.routing_lookup_result(&lookup),
        Err(Schema6IndexReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(second.runtime.snapshot().reads, before_foreign_result);

    lookup.substitute_root_for_test();
    let before_substitution = first.runtime.snapshot().reads;
    assert!(matches!(
        first_session.routing_lookup_result(&lookup),
        Err(Schema6IndexReaderError::ForeignRootContext)
    ));
    assert_eq!(first.runtime.snapshot().reads, before_substitution);
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn malformed_collision_key_is_sticky_before_the_target_can_match() {
    let mut encoded = encoded_collision_index();
    corrupt_first_collision_key(&mut encoded.bytes, &encoded.target_value);
    let fixture = fixture(
        "schema6-routing-collision-corruption",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded.bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open corrupt collision reader");
    let session = reader
        .query_session()
        .expect("open corrupt collision session");
    let root = session.load_root().expect("load corrupt collision root");

    assert!(matches!(
        routing_lookup(&session, &root, LABEL_NAME, &encoded.target_value),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    let after_first = fixture.runtime.snapshot().reads;
    assert!(matches!(
        routing_lookup(&session, &root, LABEL_NAME, &encoded.target_value),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after_first);
}

#[test]
fn probe_exhaustion_is_sticky_corruption() {
    let mut encoded = encoded_collision_index();
    fill_every_routing_bucket(&mut encoded.bytes);
    let fixture = fixture(
        "schema6-routing-probe-exhaustion",
        runtime(1024 * 1024, 1024 * 1024),
        &encoded.bytes,
    );
    let reader = GovernedSchema6IndexReader::open(&fixture.registered)
        .expect("open exhausted routing reader");
    let session = reader
        .query_session()
        .expect("open exhausted routing session");
    let root = session.load_root().expect("load exhausted routing root");

    assert!(matches!(
        routing_lookup(&session, &root, LABEL_NAME, "not-present"),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    let after_first = fixture.runtime.snapshot().reads;
    assert!(matches!(
        routing_lookup(&session, &root, LABEL_NAME, "still-not-present"),
        Err(Schema6IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().reads, after_first);
}

#[test]
fn routing_match_preserves_exact_metadata() {
    let encoded = encoded_collision_index();
    assert_eq!(
        encoded.expected,
        ExactPostingsMetadata {
            byte_len: 8,
            time_range: LabelValueTimeRange {
                min_time_ms: 200,
                max_time_ms: 299,
            },
        }
    );
}
