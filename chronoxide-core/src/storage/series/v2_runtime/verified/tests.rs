use std::fs;

use super::super::tests as support;
use super::*;
use support::{
    Fixture, class_reads, default_entries, delta, fixture, open_chunk_index_context, open_reader,
    open_symbol_session, read_metadata, runtime, standard_fixture,
};

fn read_verified(
    fixture: &Fixture,
    session: &GovernedSchema6SeriesSession,
    root: &GovernedSchema6SeriesRoot,
    symbols: &GovernedSymbolSession,
    series_refs: &[u32],
) -> Result<GovernedSchema6VerifiedSeriesBatch, Schema6SeriesReaderError> {
    let (chunk_index, chunk_index_root) = open_chunk_index_context(fixture);
    session.materialize_verified(root, &chunk_index, &chunk_index_root, symbols, series_refs)
}

fn independently_measured_output_bytes(batch: &GovernedSchema6VerifiedSeriesBatch) -> u64 {
    let mut bytes = batch.values.capacity() * std::mem::size_of::<Schema6VerifiedSeries>();
    for value in &batch.values {
        bytes += value.labels.capacity() * std::mem::size_of::<(String, String)>();
        for (name, label_value) in &value.labels {
            bytes += name.capacity() + label_value.capacity();
        }
    }
    bytes as u64
}

fn exact_capacity<T>(len: usize) -> usize {
    let mut values = Vec::<T>::new();
    values
        .try_reserve_exact(len)
        .expect("reserve deterministic test vector capacity");
    values.capacity()
}

fn test_u32_at(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(
        bytes[offset..offset + std::mem::size_of::<u32>()]
            .try_into()
            .expect("fixture u32 range"),
    )
}

fn test_u64_at(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(
        bytes[offset..offset + std::mem::size_of::<u64>()]
            .try_into()
            .expect("fixture u64 range"),
    )
}

fn replace_single_label_key(bytes: &mut [u8], series_ref: usize, replacement: u32) {
    const NUM_VALUE_DICTS_OFFSET: usize = 16;
    const KEYSETS_OFFSET_OFFSET: usize = 32;
    const VALUE_DICTS_OFFSET_OFFSET: usize = 40;
    const TABLE_KEYSET_ID_OFFSET: usize = 24;

    let table_entry = SERIES_HEADER_LEN as usize + series_ref * SERIES_TABLE_ENTRY_LEN as usize;
    let keyset_id = test_u32_at(bytes, table_entry + TABLE_KEYSET_ID_OFFSET) as usize;
    let keysets_offset = test_u64_at(bytes, KEYSETS_OFFSET_OFFSET) as usize;
    let keyset_offset = test_u64_at(bytes, keysets_offset + keyset_id * 8) as usize;
    assert_eq!(test_u32_at(bytes, keyset_offset), 1);
    let old_key = test_u32_at(bytes, keyset_offset + 8);
    bytes[keyset_offset + 8..keyset_offset + 12].copy_from_slice(&replacement.to_le_bytes());

    let value_dicts_offset = test_u64_at(bytes, VALUE_DICTS_OFFSET_OFFSET) as usize;
    let num_value_dicts = test_u32_at(bytes, NUM_VALUE_DICTS_OFFSET) as usize;
    let dict_offset = (0..num_value_dicts)
        .map(|dict_id| test_u64_at(bytes, value_dicts_offset + dict_id * 8) as usize)
        .find(|&offset| test_u32_at(bytes, offset) == old_key)
        .expect("matching value dictionary for single-label keyset");
    bytes[dict_offset..dict_offset + 4].copy_from_slice(&replacement.to_le_bytes());
}

#[test]
fn verified_batch_owns_canonical_labels_and_resolves_each_symbol_once() {
    let fixture = standard_fixture("schema6-series-verified", 0, 1024 * 1024);
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open verified schema-6 session");
    let root = session.load_root().expect("load verified series root");
    let symbols = open_symbol_session(&fixture);
    let refs = [3, 1, 1, 0, 2];
    let before_in_flight = fixture.runtime.snapshot().governor.in_flight_bytes;
    let before_page_reads = class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage);
    let before_logical = symbols.logical_stats();

    let result = read_verified(&fixture, &session, &root, &symbols, &refs)
        .expect("materialize verified schema-6 batch");
    assert_eq!(result.len(), refs.len());
    assert!(!result.is_empty());
    let values = session
        .verified_series(&result)
        .expect("bind verified values to their generation");
    assert_eq!(values.len(), refs.len());
    assert_eq!(values[0].series_ref(), 3);
    assert_eq!(values[0].series_id(), fixture.entries[3].series_id);
    assert_eq!(values[0].kind_mask(), fixture.entries[3].kind_mask);
    assert_eq!(values[0].chunk_index(), fixture.entries[3].chunk_index);
    assert!(values[0].labels().is_empty());
    assert_eq!(
        values[1].labels(),
        &[
            ("s01".to_string(), "s11".to_string()),
            ("s02".to_string(), "s20".to_string()),
        ]
    );
    assert_eq!(values[2], values[1]);
    assert_eq!(
        values[3].labels(),
        &[
            ("s01".to_string(), "s10".to_string()),
            ("s02".to_string(), "s20".to_string()),
        ]
    );
    assert_eq!(
        values[4].labels(),
        &[("s03".to_string(), "s30".to_string())]
    );

    let after_logical = symbols.logical_stats();
    assert_eq!(
        after_logical.returned_values - before_logical.returned_values,
        7,
        "the seven distinct required symbol IDs must each resolve once"
    );
    assert_eq!(
        after_logical.returned_utf8_bytes - before_logical.returned_utf8_bytes,
        21
    );
    assert_eq!(
        delta(
            class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage),
            before_page_reads,
        )
        .calls,
        1,
        "all fixture symbols share one physical page"
    );

    assert_eq!(
        result.charged_bytes(),
        independently_measured_output_bytes(&result)
    );
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        before_in_flight + result.charged_bytes()
    );
    drop(result);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        before_in_flight,
        "dropping the verified batch must release every owned label charge"
    );
}

#[test]
fn verified_batch_rejects_foreign_symbol_generation_before_cold_io() {
    let shared_runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture(
        "schema6-series-verified-generation-first",
        shared_runtime.clone(),
        default_entries(),
        |_| {},
    );
    let second = fixture(
        "schema6-series-verified-generation-second",
        shared_runtime,
        default_entries(),
        |_| {},
    );
    let reader = open_reader(&first);
    let session = reader.query_session().expect("open first series session");
    let root = session.load_root().expect("load first series root");
    let foreign_symbols = open_symbol_session(&second);
    let (chunk_index, chunk_index_root) = open_chunk_index_context(&first);
    let before_cold = class_reads(&first.runtime, MetadataCacheClass::SeriesColdPage);
    let before_symbols = class_reads(&first.runtime, MetadataCacheClass::SymbolPage);

    assert!(matches!(
        session.materialize_verified(
            &root,
            &chunk_index,
            &chunk_index_root,
            &foreign_symbols,
            &[0],
        ),
        Err(Schema6SeriesReaderError::Symbols(
            GovernedSymbolReaderError::ForeignSegmentGeneration
        ))
    ));
    assert_eq!(
        class_reads(&first.runtime, MetadataCacheClass::SeriesColdPage),
        before_cold
    );
    assert_eq!(
        class_reads(&first.runtime, MetadataCacheClass::SymbolPage),
        before_symbols
    );
    assert_eq!(first.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn verified_identity_mismatch_is_sticky_series_corruption_without_partial_output() {
    let fixture = fixture(
        "schema6-series-verified-identity",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            bytes[SERIES_HEADER_LEN as usize..SERIES_HEADER_LEN as usize + 8]
                .copy_from_slice(&u64::MAX.to_le_bytes());
        },
    );
    let reader = open_reader(&fixture);
    let session = reader.query_session().expect("open identity session");
    let root = session.load_root().expect("load identity root");
    let symbols = open_symbol_session(&fixture);

    assert!(matches!(
        read_verified(&fixture, &session, &root, &symbols, &[0]),
        Err(Schema6SeriesReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
    let after_hot = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    let after_cold = class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage);
    let after_symbols = class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage);
    let after_logical = symbols.logical_stats();
    fixture.runtime.evict_all_resident_metadata();
    assert!(read_verified(&fixture, &session, &root, &symbols, &[0]).is_err());
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
        after_hot
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage),
        after_cold
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage),
        after_symbols
    );
    assert_eq!(symbols.logical_stats(), after_logical);

    // Identity disagreement belongs to series.bin; symbols.bin remains a
    // usable, separately governed artifact.
    symbols
        .visit_required_resolved(1, |_| Ok(()))
        .expect("series identity failure must not poison symbols");
}

#[test]
fn symbol_page_corruption_is_attributed_to_symbols_not_series() {
    let fixture = standard_fixture("schema6-series-symbol-corruption", 0, 1024 * 1024);
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open symbol-corruption session");
    let root = session.load_root().expect("load symbol-corruption root");
    let symbols = open_symbol_session(&fixture);
    let mut symbol_bytes = fs::read(&fixture.symbols_path).expect("read symbols fixture");
    *symbol_bytes.last_mut().expect("symbols fixture has a page") ^= 0x80;
    fs::write(&fixture.symbols_path, symbol_bytes).expect("corrupt symbol page in place");
    let before_page = class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage);

    assert!(matches!(
        read_verified(&fixture, &session, &root, &symbols, &[0]),
        Err(Schema6SeriesReaderError::Symbols(
            GovernedSymbolReaderError::Cache(MetadataCacheError::Structural(_))
        ))
    ));
    let after_page = class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage);
    assert_eq!(delta(after_page, before_page).calls, 1);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    let metadata = read_metadata(&fixture, &session, &root, &[0])
        .expect("symbol corruption must not poison series metadata");
    assert_eq!(session.routing_entries(&metadata).unwrap().len(), 1);
    assert!(symbols.visit_required_resolved(1, |_| Ok(())).is_err());
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage),
        after_page,
        "sticky symbol corruption must fail before another page read"
    );
}

#[test]
fn verified_batch_tiny_budget_refuses_before_cold_or_symbol_io_and_releases_scratch() {
    let fixture = standard_fixture("schema6-series-verified-budget", 1024 * 1024, 8192);
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open verified-budget session");
    let root = session.load_root().expect("load verified-budget root");
    let symbols = open_symbol_session(&fixture);
    let (chunk_index, chunk_index_root) = open_chunk_index_context(&fixture);
    let baseline = fixture.runtime.snapshot().governor.in_flight_bytes;
    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(8100, MetadataUsageClass::Scratch)
        .expect("reserve competing verified-batch scratch");
    let before_hot = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    let before_cold = class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage);
    let before_symbols = class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage);

    assert!(matches!(
        session.materialize_verified(&root, &chunk_index, &chunk_index_root, &symbols, &[0, 1, 2],),
        Err(Schema6SeriesReaderError::Cache(MetadataCacheError::Budget(
            _
        )))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
        before_hot
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage),
        before_cold
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage),
        before_symbols
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline + blocker.bytes()
    );

    drop(blocker);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline
    );
    let retried = session
        .materialize_verified(&root, &chunk_index, &chunk_index_root, &symbols, &[0, 1, 2])
        .expect("budget refusal must be retryable without partial output");
    assert_eq!(retried.len(), 3);
    drop(retried);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline
    );
}

#[test]
fn out_of_range_encoded_symbol_is_sticky_series_corruption_before_symbol_io() {
    let fixture = fixture(
        "schema6-series-verified-symbol-range",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| replace_single_label_key(bytes, 2, 31),
    );
    let reader = open_reader(&fixture);
    let session = reader.query_session().expect("open symbol-range session");
    let root = session.load_root().expect("load symbol-range root");
    let symbols = open_symbol_session(&fixture);
    assert_eq!(
        symbols.len(),
        31,
        "symbol ID 31 is exactly one past the root"
    );
    let before_symbols = class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage);

    assert!(matches!(
        read_verified(&fixture, &session, &root, &symbols, &[2]),
        Err(Schema6SeriesReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage),
        before_symbols,
        "the referring series must reject the out-of-range ID before symbol-page I/O"
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    symbols
        .visit_required_resolved(1, |_| Ok(()))
        .expect("series corruption must not poison symbols.bin");
    let after_hot = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    let after_cold = class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage);
    let after_symbols = class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage);
    fixture.runtime.evict_all_resident_metadata();

    assert!(read_verified(&fixture, &session, &root, &symbols, &[2]).is_err());
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
        after_hot
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage),
        after_cold
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SymbolPage),
        after_symbols
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn verified_batch_accessor_rejects_foreign_generation_without_io_or_sticky_state() {
    let shared_runtime = runtime(1024 * 1024, 1024 * 1024);
    let first = fixture(
        "schema6-series-verified-batch-first",
        shared_runtime.clone(),
        default_entries(),
        |_| {},
    );
    let second = fixture(
        "schema6-series-verified-batch-second",
        shared_runtime,
        default_entries(),
        |_| {},
    );
    let first_reader = open_reader(&first);
    let first_session = first_reader
        .query_session()
        .expect("open first series session");
    let first_root = first_session.load_root().expect("load first series root");
    let first_symbols = open_symbol_session(&first);
    let batch = read_verified(&first, &first_session, &first_root, &first_symbols, &[0])
        .expect("materialize first-generation verified batch");
    let second_reader = open_reader(&second);
    let second_session = second_reader
        .query_session()
        .expect("open second series session");
    let before = first.runtime.snapshot();

    assert!(matches!(
        second_session.verified_series(&batch),
        Err(Schema6SeriesReaderError::ForeignSegmentGeneration)
    ));
    assert_eq!(first.runtime.snapshot(), before);
}

#[test]
fn deferred_string_budget_refusal_is_nonsticky_and_releases_partial_output() {
    const IN_FLIGHT_LIMIT: u64 = 8192;

    let fixture = standard_fixture(
        "schema6-series-verified-deferred-budget",
        1024 * 1024,
        IN_FLIGHT_LIMIT,
    );
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open deferred-budget session");
    let root = session.load_root().expect("load deferred-budget root");
    let symbols = open_symbol_session(&fixture);
    let refs = [0];
    let required_symbols = [1, 2, 10, 20];

    // Warm every page used below and measure the allocator's exact output
    // capacities independently from the production accounting helper.
    let warm = read_verified(&fixture, &session, &root, &symbols, &refs)
        .expect("warm verified metadata and symbol pages");
    let output_without_strings = warm.values.capacity()
        * std::mem::size_of::<Schema6VerifiedSeries>()
        + warm
            .values
            .iter()
            .map(|value| value.labels.capacity() * std::mem::size_of::<(String, String)>())
            .sum::<usize>();
    drop(warm);

    let baseline = fixture.runtime.snapshot().governor.in_flight_bytes;
    let mut resolver_scratch = None;
    symbols
        .visit_resolved_many(&required_symbols, |request_index, _| {
            if request_index == 0 {
                resolver_scratch =
                    Some(fixture.runtime.snapshot().governor.in_flight_bytes - baseline);
            }
            Ok(())
        })
        .expect("measure governed symbol resolver scratch");
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline
    );

    let component_count = required_symbols.len();
    let outer_scratch = output_without_strings as u64
        + (exact_capacity::<u32>(component_count) * std::mem::size_of::<u32>()) as u64
        + (exact_capacity::<SymbolOccurrence>(component_count)
            * std::mem::size_of::<SymbolOccurrence>()) as u64;
    let mut first_string = String::new();
    first_string
        .try_reserve_exact(3)
        .expect("reserve fixture symbol-sized string");
    let first_string_capacity = first_string.capacity() as u64;
    let headroom = outer_scratch
        + resolver_scratch.expect("resolver callback observed its scratch charge")
        + first_string_capacity;
    assert!(headroom < IN_FLIGHT_LIMIT);
    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(IN_FLIGHT_LIMIT - headroom, MetadataUsageClass::Scratch)
        .expect("leave room for exactly one canonical string allocation");
    let before_logical = symbols.logical_stats();

    assert!(matches!(
        read_verified(&fixture, &session, &root, &symbols, &refs),
        Err(Schema6SeriesReaderError::Cache(MetadataCacheError::Budget(
            _
        )))
    ));
    let after_logical = symbols.logical_stats();
    assert_eq!(
        after_logical.returned_values - before_logical.returned_values,
        1
    );
    assert_eq!(
        after_logical.returned_utf8_bytes - before_logical.returned_utf8_bytes,
        3
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline + blocker.bytes(),
        "all partial verified output and nested resolver scratch must be released"
    );

    drop(blocker);
    let retried = read_verified(&fixture, &session, &root, &symbols, &refs)
        .expect("deferred budget refusal must be retryable");
    drop(retried);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline
    );
}
