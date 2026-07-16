use super::*;

fn assert_index_reads_unchanged(
    runtime: &StoreMetadataRuntime,
    before: &crate::storage::metadata_runtime::MetadataReadStats,
) {
    let after = runtime.snapshot().reads;
    for class in [
        MetadataCacheClass::IndexRoot,
        MetadataCacheClass::IndexDirectory,
        MetadataCacheClass::IndexPage,
        MetadataCacheClass::Postings,
    ] {
        assert_eq!(
            after.classes[class.stable_index()].issued,
            before.classes[class.stable_index()].issued,
            "foreign authority unexpectedly issued {class:?} I/O",
        );
    }
}

#[test]
fn copied_bound_root_layout_and_count_substitutions_fail_without_io_or_poisoning() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-bound-root-substitution",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let reader = GovernedSchema7IndexReader::open(&fixture.registered).unwrap();
    let session = reader.query_session().unwrap();

    let mut count_substitution = bind(&fixture, &session);
    count_substitution.value.layout.counts.series ^= 1;
    let before = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.root(&count_substitution),
        Err(Schema7IndexReaderError::ForeignRootContext)
    ));
    assert_index_reads_unchanged(&fixture.runtime, &before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    let mut locator_substitution = bind(&fixture, &session);
    locator_substitution.value.layout.exact_directory.offset ^= 1;
    let before = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.select_exact_postings(&locator_substitution, LABEL_NAME_SYM, 0),
        Err(Schema7IndexReaderError::ForeignRootContext)
    ));
    assert_index_reads_unchanged(&fixture.runtime, &before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn foreign_root_fst_and_symbol_authorities_fail_without_index_io_or_poisoning() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let first = fixture(
        "schema7-v8-authority-first",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );
    let second = fixture(
        "schema7-v8-authority-second",
        runtime(1024 * 1024, 1024 * 1024),
        2,
        &bytes,
    );

    let first_reader = GovernedSchema7IndexReader::open(&first.registered).unwrap();
    let first_session = first_reader.query_session().unwrap();
    let first_root = bind(&first, &first_session);
    let first_selection = first_session
        .select_exact_postings(&first_root, LABEL_NAME_SYM, 0)
        .unwrap()
        .unwrap();
    let first_directory = first_session.load_auxiliary_directory(&first_root).unwrap();
    let first_fst = first_session
        .load_label_value_fst(&first_root, &first_directory, LABEL_NAME_SYM)
        .unwrap()
        .unwrap();

    let second_reader = GovernedSchema7IndexReader::open(&second.registered).unwrap();
    let second_session = second_reader.query_session().unwrap();
    let second_root = bind(&second, &second_session);
    let second_symbols = symbol_session(&second.registered);
    let first_before = first.runtime.snapshot().reads;
    let second_before = second.runtime.snapshot().reads;

    assert!(matches!(
        second_session.root(&first_root),
        Err(Schema7IndexReaderError::ForeignSegmentGeneration)
    ));
    assert!(matches!(
        second_session.read_exact_postings(&second_root, &first_selection),
        Err(Schema7IndexReaderError::ForeignSegmentGeneration)
    ));
    let mut visited = false;
    assert!(matches!(
        second_session.visit_label_values_with_prefix(
            &second_root,
            &first_fst,
            &second_symbols,
            None,
            |_, _| {
                visited = true;
                true
            },
        ),
        Err(Schema7IndexReaderError::ForeignSegmentGeneration)
    ));
    assert!(!visited);
    assert!(matches!(
        first_session.visit_label_values_with_prefix(
            &first_root,
            &first_fst,
            &second_symbols,
            None,
            |_, _| true,
        ),
        Err(Schema7IndexReaderError::ForeignSegmentGeneration)
    ));

    assert_index_reads_unchanged(&first.runtime, &first_before);
    assert_index_reads_unchanged(&second.runtime, &second_before);
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
    assert_eq!(second.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn exact_corruption_remains_sticky_after_metadata_and_descriptor_eviction() {
    let mut bytes = encode_index(2, &[b"alpha", b"beta"]);
    let trailer_offset = bytes.len() - TRAILER_LEN;
    let layout = decode_root(
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
    bytes[layout.exact_postings.offset as usize + 4] ^= 1;

    let fixture = fixture(
        "schema7-v8-sticky-after-eviction",
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
    assert!(matches!(
        session.read_exact_postings(&root, &selection),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    drop(selection);
    drop(root);
    drop(session);
    drop(reader);
    fixture.runtime.evict_all_resident_metadata();
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    let before = fixture.runtime.snapshot().reads;

    assert!(matches!(
        GovernedSchema7IndexReader::open(&fixture.registered),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_index_reads_unchanged(&fixture.runtime, &before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
}

#[test]
fn typed_cache_context_collision_is_sticky_without_payload_io() {
    let bytes = encode_index(2, &[b"alpha", b"beta"]);
    let fixture = fixture(
        "schema7-v8-typed-context-collision",
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
    session
        .inject_exact_postings_cache_context_collision_for_test(&root, &selection)
        .expect("inject typed cache value with a foreign protected context");

    let before = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.read_exact_postings(&root, &selection),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_index_reads_unchanged(&fixture.runtime, &before);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    fixture.runtime.evict_all_resident_metadata();
    let after_error = fixture.runtime.snapshot().reads;
    assert!(matches!(
        session.read_exact_postings(&root, &selection),
        Err(Schema7IndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_index_reads_unchanged(&fixture.runtime, &after_error);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}
