use super::*;

fn block_all_but_one_byte(
    runtime: &StoreMetadataRuntime,
) -> crate::storage::metadata_governor::MetadataCharge {
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

fn assert_budget_refusal(fixture: &Fixture, error: Schema7IndexReaderError) {
    assert!(matches!(
        error,
        Schema7IndexReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

fn assert_one_read(
    before: MetadataIssuedReadCount,
    after: MetadataIssuedReadCount,
    expected_bytes: u64,
) {
    assert_eq!(
        after,
        MetadataIssuedReadCount {
            calls: before.calls + 1,
            bytes: before.bytes + expected_bytes,
        }
    );
}

fn auxiliary_payload_lengths(bytes: &[u8], layout: SegmentIndexV8Layout) -> (u64, u64) {
    let offset = usize::try_from(layout.auxiliary_directory.offset)
        .expect("auxiliary directory offset fits usize");
    let end = usize::try_from(
        layout
            .auxiliary_directory
            .offset
            .checked_add(layout.auxiliary_directory.len)
            .expect("auxiliary directory end does not overflow"),
    )
    .expect("auxiliary directory end fits usize");
    let directory =
        super::super::super::codec::decode_auxiliary_directory(&bytes[offset..end], layout)
            .expect("decode fixture auxiliary directory");
    let fst = directory
        .record(
            crate::storage::index::SEGMENT_INDEX_BLOB_LABEL_VALUE_FST,
            LABEL_NAME_SYM,
        )
        .expect("fixture FST record exists")
        .payload
        .len;
    let ranges = directory
        .record(
            crate::storage::index::SEGMENT_INDEX_BLOB_LABEL_VALUE_TIME_RANGES,
            LABEL_NAME_SYM,
        )
        .expect("fixture ranges record exists")
        .payload
        .len;
    (fst, ranges)
}

#[test]
fn every_variable_path_refuses_budget_before_io_then_retries_without_poisoning() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-variable-budget-refusal",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).expect("open v8 reader");
    let session = reader.query_session().expect("open v8 session");
    let root = bind(&fixture, &session);
    let (fst_payload_len, ranges_payload_len) =
        auxiliary_payload_lengths(&bytes, root.value.layout);

    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .expect_err("exact directory must be refused before I/O");
    assert_budget_refusal(&fixture, error);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        before_directory
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_page
    );
    drop(blocker);

    assert!(
        session
            .select_exact_postings(&root, 0, 0)
            .expect("retry exact directory after releasing budget")
            .is_none(),
        "the missing key loads only the exact directory"
    );
    assert_one_read(
        before_directory,
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        root.value.layout.exact_directory.len,
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_page
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .expect_err("exact page must be refused before I/O");
    assert_budget_refusal(&fixture, error);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_page
    );
    drop(blocker);

    let selection = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .expect("retry exact page after releasing budget")
        .expect("fixture exact key exists");
    assert_one_read(
        before_page,
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        EXACT_PAGE_LEN as u64,
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    let expected_postings_bytes = selection.page.records[selection.record_index].postings.len;
    let before_postings = class_reads(&fixture.runtime, MetadataCacheClass::Postings);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .read_exact_postings(&root, &selection)
        .expect_err("exact postings must be refused before I/O");
    assert_budget_refusal(&fixture, error);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        before_postings
    );
    drop(blocker);

    let postings = session
        .read_exact_postings(&root, &selection)
        .expect("retry exact postings after releasing budget");
    assert_eq!(session.postings(&root, &postings).unwrap(), [0, 1]);
    assert_one_read(
        before_postings,
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        expected_postings_bytes,
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    let before_auxiliary = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .load_auxiliary_directory(&root)
        .expect_err("auxiliary directory must be refused before I/O");
    assert_budget_refusal(&fixture, error);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        before_auxiliary
    );
    drop(blocker);

    let directory = session
        .load_auxiliary_directory(&root)
        .expect("retry auxiliary directory after releasing budget");
    assert!(session.has_label_values(&root, &directory).unwrap());
    assert_one_read(
        before_auxiliary,
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        root.value.layout.auxiliary_directory.len,
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    let before_fst = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .load_label_value_fst(&root, &directory, LABEL_NAME_SYM)
        .expect_err("label-value FST must be refused before I/O");
    assert_budget_refusal(&fixture, error);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_fst
    );
    drop(blocker);

    let fst = session
        .load_label_value_fst(&root, &directory, LABEL_NAME_SYM)
        .expect("retry FST after releasing budget")
        .expect("fixture FST exists");
    assert!(fst.charged_bytes() > 0);
    assert_one_read(
        before_fst,
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        fst_payload_len,
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    let before_ranges = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let blocker = block_all_but_one_byte(&fixture.runtime);
    let error = session
        .load_label_value_time_ranges(&root, &directory, LABEL_NAME_SYM)
        .expect_err("label-value ranges must be refused before I/O");
    assert_budget_refusal(&fixture, error);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        before_ranges
    );
    drop(blocker);

    let ranges = session
        .load_label_value_time_ranges(&root, &directory, LABEL_NAME_SYM)
        .expect("retry ranges after releasing budget")
        .expect("fixture ranges exist");
    assert_eq!(
        session
            .label_value_time_ranges(&root, &ranges)
            .unwrap()
            .len(),
        2
    );
    assert_one_read(
        before_ranges,
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        ranges_payload_len,
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn zero_retention_reuses_live_variable_pins_then_reissues_after_final_drop() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-variable-zero-retention",
        runtime(0, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).expect("open v8 reader");
    let session = reader.query_session().expect("open v8 session");
    let root = bind(&fixture, &session);
    let (fst_payload_len, ranges_payload_len) =
        auxiliary_payload_lengths(&bytes, root.value.layout);
    let baseline_in_flight = fixture.runtime.snapshot().governor.in_flight_bytes;
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let before_postings = class_reads(&fixture.runtime, MetadataCacheClass::Postings);

    let exact_directory = session
        .load_exact_directory(&root)
        .expect("load exact directory");
    let selection = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .unwrap()
        .expect("fixture exact key exists");
    let postings = session.read_exact_postings(&root, &selection).unwrap();
    let auxiliary = session.load_auxiliary_directory(&root).unwrap();
    let fst = session
        .load_label_value_fst(&root, &auxiliary, LABEL_NAME_SYM)
        .unwrap()
        .expect("fixture FST exists");
    let ranges = session
        .load_label_value_time_ranges(&root, &auxiliary, LABEL_NAME_SYM)
        .unwrap()
        .expect("fixture ranges exist");
    let expected_directory_bytes =
        root.value.layout.exact_directory.len + root.value.layout.auxiliary_directory.len;
    let expected_page_bytes = EXACT_PAGE_LEN as u64 + fst_payload_len + ranges_payload_len;
    let expected_postings_bytes = selection.page.records[selection.record_index].postings.len;

    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        MetadataIssuedReadCount {
            calls: before_directory.calls + 2,
            bytes: before_directory.bytes + expected_directory_bytes,
        }
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        MetadataIssuedReadCount {
            calls: before_page.calls + 3,
            bytes: before_page.bytes + expected_page_bytes,
        }
    );
    assert_one_read(
        before_postings,
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        expected_postings_bytes,
    );
    let after_first_load = fixture.runtime.snapshot().reads;

    let exact_directory_again = session
        .load_exact_directory(&root)
        .expect("reuse live exact directory");
    let selection_again = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .unwrap()
        .expect("reuse live exact page");
    let postings_again = session
        .read_exact_postings(&root, &selection_again)
        .expect("reuse live exact postings");
    let auxiliary_again = session
        .load_auxiliary_directory(&root)
        .expect("reuse live auxiliary directory");
    let fst_again = session
        .load_label_value_fst(&root, &auxiliary_again, LABEL_NAME_SYM)
        .unwrap()
        .expect("reuse live FST");
    let ranges_again = session
        .load_label_value_time_ranges(&root, &auxiliary_again, LABEL_NAME_SYM)
        .unwrap()
        .expect("reuse live ranges");
    assert_eq!(fixture.runtime.snapshot().reads, after_first_load);

    drop(ranges_again);
    drop(fst_again);
    drop(auxiliary_again);
    drop(postings_again);
    drop(selection_again);
    drop(exact_directory_again);
    drop(ranges);
    drop(fst);
    drop(auxiliary);
    drop(postings);
    drop(selection);
    drop(exact_directory);
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline_in_flight
    );

    let exact_directory = session.load_exact_directory(&root).unwrap();
    let selection = session
        .select_exact_postings(&root, LABEL_NAME_SYM, 0)
        .unwrap()
        .unwrap();
    let postings = session.read_exact_postings(&root, &selection).unwrap();
    let auxiliary = session.load_auxiliary_directory(&root).unwrap();
    let fst = session
        .load_label_value_fst(&root, &auxiliary, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();
    let ranges = session
        .load_label_value_time_ranges(&root, &auxiliary, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        MetadataIssuedReadCount {
            calls: before_directory.calls + 4,
            bytes: before_directory.bytes + expected_directory_bytes * 2,
        }
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        MetadataIssuedReadCount {
            calls: before_page.calls + 6,
            bytes: before_page.bytes + expected_page_bytes * 2,
        }
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::Postings),
        MetadataIssuedReadCount {
            calls: before_postings.calls + 2,
            bytes: before_postings.bytes + expected_postings_bytes * 2,
        }
    );

    drop(ranges);
    drop(fst);
    drop(auxiliary);
    drop(postings);
    drop(selection);
    drop(exact_directory);
    assert_eq!(fixture.runtime.snapshot().cache.resident_entries, 0);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline_in_flight
    );
}

#[test]
fn concurrent_identical_auxiliary_misses_issue_each_range_once() {
    const THREADS: usize = 8;

    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-auxiliary-concurrent",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = Arc::new(GovernedSchema7IndexReader::open(&fixture.registered).unwrap());
    let setup_session = reader.query_session().unwrap();
    let root = Arc::new(bind(&fixture, &setup_session));
    let directory_len = root.value.layout.auxiliary_directory.len;
    let (fst_payload_len, ranges_payload_len) =
        auxiliary_payload_lengths(&bytes, root.value.layout);
    let expected_payload_bytes = fst_payload_len + ranges_payload_len;
    drop(setup_session);

    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::IndexPage);
    let barrier = Arc::new(Barrier::new(THREADS));
    let workers = (0..THREADS)
        .map(|_| {
            let reader = Arc::clone(&reader);
            let root = Arc::clone(&root);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                let session = reader.query_session().expect("open worker session");
                barrier.wait();
                let directory = session
                    .load_auxiliary_directory(&root)
                    .expect("load shared auxiliary directory");
                let fst = session
                    .load_label_value_fst(&root, &directory, LABEL_NAME_SYM)
                    .expect("load shared FST")
                    .expect("fixture FST exists");
                let ranges = session
                    .load_label_value_time_ranges(&root, &directory, LABEL_NAME_SYM)
                    .expect("load shared ranges")
                    .expect("fixture ranges exist");
                assert!(fst.charged_bytes() > 0);
                assert_eq!(
                    session
                        .label_value_time_ranges(&root, &ranges)
                        .expect("consume shared ranges")
                        .len(),
                    2
                );
            })
        })
        .collect::<Vec<_>>();
    for worker in workers {
        worker.join().expect("auxiliary worker completes");
    }

    assert_one_read(
        before_directory,
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        directory_len,
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexPage),
        MetadataIssuedReadCount {
            calls: before_page.calls + 2,
            bytes: before_page.bytes + expected_payload_bytes,
        }
    );
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    assert_eq!(fixture.runtime.snapshot().cache.active_loads, 0);
}
