use std::fs;
use std::io::Cursor;

use tempfile::TempDir;

use crate::storage::chunk::{
    ChunkIndexEntry, ChunkIndexRange, ChunkKind, GovernedSchema6ChunkIndexReader, write_chunk_index,
};
use crate::storage::metadata_governor::MetadataGovernorConfig;
use crate::storage::metadata_runtime::{
    MetadataIssuedReadCount, SegmentArtifactRegistration, StoreMetadataRuntime,
};
use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;
use crate::storage::symbols::{GovernedSymbolReader, write_symbols_bin_v3};

use super::super::{SeriesReader, build_series_bin_v2};
use super::*;

const CHUNKS_LEN: usize = 4096;
const OOO_CHUNKS_LEN: usize = 2048;

pub(super) struct Fixture {
    _directory: TempDir,
    pub(super) runtime: StoreMetadataRuntime,
    registered: Option<RegisteredSegment>,
    pub(super) entries: Vec<SeriesEntry>,
    series_bytes: Vec<u8>,
    series_path: std::path::PathBuf,
    pub(super) symbols_path: std::path::PathBuf,
}

pub(super) fn runtime(retained_max_bytes: u64, in_flight_max_bytes: u64) -> StoreMetadataRuntime {
    StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes,
        in_flight_max_bytes,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .expect("valid schema-6 series test runtime")
}

pub(super) fn default_entries() -> Vec<SeriesEntry> {
    // Four series imply a 52-byte v1 chunk-index root/directory. Each
    // pointer below is therefore exact, aligned, and within the fixture.
    let mut entries = vec![
        SeriesEntry {
            series_id: 0,
            kind_mask: SERIES_KIND_FLOAT,
            chunk_index: ChunkIndexRange {
                offset: 52,
                len: 40,
            },
            labels: vec![(1, 10), (2, 20)],
        },
        SeriesEntry {
            series_id: 0,
            kind_mask: SERIES_KIND_HISTOGRAM,
            chunk_index: ChunkIndexRange {
                offset: 92,
                len: 40,
            },
            labels: vec![(1, 11), (2, 20)],
        },
        SeriesEntry {
            series_id: 0,
            kind_mask: SERIES_KIND_EXPONENTIAL_HISTOGRAM,
            chunk_index: ChunkIndexRange {
                offset: 132,
                len: 40,
            },
            labels: vec![(3, 30)],
        },
        SeriesEntry {
            series_id: 0,
            kind_mask: SERIES_KIND_SUMMARY,
            chunk_index: ChunkIndexRange {
                offset: 172,
                len: 40,
            },
            labels: Vec::new(),
        },
    ];
    for entry in &mut entries {
        entry.series_id = fixture_series_id(&entry.labels);
    }
    entries
}

fn fixture_symbols() -> Vec<String> {
    (0..=30)
        .map(|symbol_id| format!("s{symbol_id:02}"))
        .collect()
}

fn fixture_series_id(labels: &[(u32, u32)]) -> u64 {
    let symbols = fixture_symbols();
    let mut hash = XxHash64::default();
    for &(key_sym, value_sym) in labels {
        hash.update(symbols[key_sym as usize].as_bytes());
        hash.update(&[0]);
        hash.update(symbols[value_sym as usize].as_bytes());
        hash.update(&[0xff]);
    }
    hash.finish()
}

pub(super) fn fixture(
    identity: &str,
    runtime: StoreMetadataRuntime,
    entries: Vec<SeriesEntry>,
    mutate_series: impl FnOnce(&mut Vec<u8>),
) -> Fixture {
    let chunk_series_count = entries.len();
    fixture_with_chunk_series_count(
        identity,
        runtime,
        entries,
        chunk_series_count,
        mutate_series,
    )
}

fn fixture_with_chunk_series_count(
    identity: &str,
    runtime: StoreMetadataRuntime,
    entries: Vec<SeriesEntry>,
    chunk_series_count: usize,
    mutate_series: impl FnOnce(&mut Vec<u8>),
) -> Fixture {
    let mut series_bytes = build_series_bin_v2(&entries).expect("encode series fixture");
    mutate_series(&mut series_bytes);
    let mut chunk_index_bytes = Vec::new();
    let chunk_entries = (0..chunk_series_count)
        .map(|index| {
            vec![ChunkIndexEntry {
                file_id: 0,
                kind: ChunkKind::Float,
                flags: 0,
                min_time_ms: index as u64,
                max_time_ms: index as u64,
                offset: (index * 64) as u64,
                length: 40,
                scalar_lane_offset: 0,
                scalar_lane_len: 0,
            }]
        })
        .collect::<Vec<_>>();
    write_chunk_index(&mut chunk_index_bytes, &chunk_entries).expect("encode chunk-index fixture");
    let directory = TempDir::new().expect("create schema-6 series fixture directory");
    let mut series_path = None;
    let mut symbols_path = None;
    let symbols = fixture_symbols();
    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let path = directory.path().join(file.filename());
        match file {
            SegmentFile::MetaJson => fs::write(&path, b"{}").expect("write meta fixture"),
            SegmentFile::Symbols => {
                let mut encoded = Vec::new();
                write_symbols_bin_v3(&mut encoded, symbols.iter()).expect("encode symbols fixture");
                fs::write(&path, encoded).expect("write symbols fixture");
                symbols_path = Some(path.clone());
            }
            SegmentFile::Series => {
                fs::write(&path, &series_bytes).expect("write series fixture");
                series_path = Some(path.clone());
            }
            SegmentFile::Chunks => {
                fs::write(&path, vec![0; CHUNKS_LEN]).expect("write chunks fixture")
            }
            SegmentFile::OooChunks => {
                fs::write(&path, vec![0; OOO_CHUNKS_LEN]).expect("write OOO fixture")
            }
            SegmentFile::ChunkIndex => {
                fs::write(&path, &chunk_index_bytes).expect("write chunk-index fixture")
            }
            SegmentFile::Indexes => fs::write(&path, b"indexes").expect("write indexes fixture"),
            SegmentFile::Footer => unreachable!("footer is not runtime-inventoried"),
        }
        let len = fs::metadata(&path).expect("stat fixture artifact").len();
        SegmentArtifactRegistration::new(file, path, len)
    });
    let registered = runtime
        .register_segment(identity, &artifacts)
        .expect("register schema-6 series fixture");
    Fixture {
        _directory: directory,
        runtime,
        registered: Some(registered),
        entries,
        series_bytes,
        series_path: series_path.expect("series path captured"),
        symbols_path: symbols_path.expect("symbols path captured"),
    }
}

pub(super) fn standard_fixture(
    identity: &str,
    retained_max_bytes: u64,
    in_flight_max_bytes: u64,
) -> Fixture {
    fixture(
        identity,
        runtime(retained_max_bytes, in_flight_max_bytes),
        default_entries(),
        |_| {},
    )
}

pub(super) fn open_reader(fixture: &Fixture) -> GovernedSchema6SeriesReader {
    GovernedSchema6SeriesReader::open(
        fixture.registered.as_ref().expect("fixture owner exists"),
        fixture.entries.len() as u32,
    )
    .expect("open governed schema-6 series reader")
}

pub(super) fn open_symbol_session(fixture: &Fixture) -> GovernedSymbolSession {
    GovernedSymbolReader::open(fixture.registered.as_ref().expect("fixture owner exists"))
        .expect("open governed symbol reader")
        .query_session()
        .expect("open governed symbol session")
}

pub(super) fn open_chunk_index_context(
    fixture: &Fixture,
) -> (
    GovernedSchema6ChunkIndexSession,
    GovernedSchema6ChunkIndexRoot,
) {
    let reader = GovernedSchema6ChunkIndexReader::open(
        fixture.registered.as_ref().expect("fixture owner exists"),
        fixture.entries.len() as u32,
    )
    .expect("open governed chunk-index reader");
    let session = reader
        .query_session()
        .expect("open governed chunk-index session");
    let root = session.load_root().expect("load governed chunk-index root");
    (session, root)
}

pub(super) fn read_metadata(
    fixture: &Fixture,
    session: &GovernedSchema6SeriesSession,
    root: &GovernedSchema6SeriesRoot,
    series_refs: &[u32],
) -> Result<GovernedSchema6SeriesMetadata, Schema6SeriesReaderError> {
    let (chunk_index, chunk_index_root) = open_chunk_index_context(fixture);
    session.read_metadata_entries(root, &chunk_index, &chunk_index_root, series_refs)
}

fn read_full_entries(
    fixture: &Fixture,
    session: &GovernedSchema6SeriesSession,
    root: &GovernedSchema6SeriesRoot,
    symbols: &GovernedSymbolSession,
    series_refs: &[u32],
) -> Result<GovernedSchema6SeriesEntries, Schema6SeriesReaderError> {
    let (chunk_index, chunk_index_root) = open_chunk_index_context(fixture);
    session.read_entries(root, &chunk_index, &chunk_index_root, symbols, series_refs)
}

pub(super) fn class_reads(
    runtime: &StoreMetadataRuntime,
    class: MetadataCacheClass,
) -> MetadataIssuedReadCount {
    runtime.snapshot().reads.classes[class.stable_index()].issued
}

pub(super) fn delta(
    after: MetadataIssuedReadCount,
    before: MetadataIssuedReadCount,
) -> MetadataIssuedReadCount {
    MetadataIssuedReadCount {
        calls: after.calls - before.calls,
        bytes: after.bytes - before.bytes,
    }
}

#[test]
fn exact_root_and_coalesced_table_spans_are_cached_and_ordered() {
    let fixture = standard_fixture("schema6-series-cached", 1024 * 1024, 1024 * 1024);
    let before_root = class_reads(&fixture.runtime, MetadataCacheClass::SeriesRoot);
    let reader = open_reader(&fixture);
    assert_eq!(reader.segment_identity(), "schema6-series-cached");
    assert_eq!(
        delta(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesRoot),
            before_root
        ),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: SERIES_HEADER_LEN,
        }
    );

    let session = reader
        .query_session()
        .expect("open schema-6 series session");
    let root = session.load_root().expect("reuse cached series root");
    assert_eq!(root.num_series(), 4);
    let before_table = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    let before_in_flight = fixture.runtime.snapshot().governor.in_flight_bytes;
    let metadata =
        read_metadata(&fixture, &session, &root, &[2, 0, 1, 1]).expect("read coalesced table span");
    let metadata_entries = session
        .routing_entries(&metadata)
        .expect("bind metadata to its session");
    assert_eq!(
        metadata_entries
            .iter()
            .map(|(series_ref, entry)| (*series_ref, entry.kind_mask, entry.chunk_index))
            .collect::<Vec<_>>(),
        vec![
            (
                2,
                fixture.entries[2].kind_mask,
                fixture.entries[2].chunk_index,
            ),
            (
                0,
                fixture.entries[0].kind_mask,
                fixture.entries[0].chunk_index,
            ),
            (
                1,
                fixture.entries[1].kind_mask,
                fixture.entries[1].chunk_index,
            ),
            (
                1,
                fixture.entries[1].kind_mask,
                fixture.entries[1].chunk_index,
            ),
        ]
    );
    assert!(metadata.charged_bytes() > 0);
    let charged_bytes = metadata.charged_bytes();
    let with_output = fixture.runtime.snapshot().governor;
    assert_eq!(
        with_output.in_flight_bytes,
        before_in_flight + charged_bytes
    );
    assert!(with_output.peak_in_flight_bytes >= with_output.in_flight_bytes);
    let after_table = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    assert_eq!(
        delta(after_table, before_table),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: 3 * SERIES_TABLE_ENTRY_LEN,
        }
    );
    drop(metadata);
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        before_in_flight
    );

    let second =
        read_metadata(&fixture, &session, &root, &[0, 1, 2]).expect("reuse exact table span");
    assert_eq!(session.routing_entries(&second).unwrap().len(), 3);
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
        after_table
    );
}

#[test]
fn out_of_range_refs_fail_before_series_io_without_poisoning_the_series_artifact() {
    let fixture = standard_fixture("schema6-series-invalid-ref", 1024 * 1024, 1024 * 1024);
    let reader = open_reader(&fixture);
    let session = reader.query_session().expect("open invalid-ref session");
    let root = session.load_root().expect("load invalid-ref root");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    assert!(matches!(
        read_metadata(&fixture, &session, &root, &[0, 99]),
        Err(Schema6SeriesReaderError::InvalidSeriesRef {
            series_ref: 99,
            num_series: 4
        })
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
        before
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn independently_valid_same_generation_roots_must_have_the_same_series_count() {
    let fixture = fixture_with_chunk_series_count(
        "schema6-series-root-count-binding",
        runtime(1024 * 1024, 1024 * 1024),
        default_entries(),
        3,
        |_| {},
    );
    let series_reader = open_reader(&fixture);
    let series_session = series_reader
        .query_session()
        .expect("open mismatched-count series session");
    let series_root = series_session
        .load_root()
        .expect("load independently valid series root");
    let chunk_reader = GovernedSchema6ChunkIndexReader::open(
        fixture.registered.as_ref().expect("fixture owner exists"),
        3,
    )
    .expect("open independently valid chunk-index root");
    let chunk_session = chunk_reader
        .query_session()
        .expect("open mismatched-count chunk-index session");
    let chunk_root = chunk_session
        .load_root()
        .expect("load mismatched-count chunk-index root");
    let before_series = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    let before_directory = class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory);

    let error = series_session
        .read_metadata_entries(&series_root, &chunk_session, &chunk_root, &[])
        .expect_err("same-generation roots with different counts must fail");
    assert!(matches!(
        error,
        Schema6SeriesReaderError::ChunkIndex(Schema6ChunkIndexReaderError::Cache(
            MetadataCacheError::Structural(_)
        ))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
        before_series
    );
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::IndexDirectory),
        before_directory
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn full_materialization_matches_legacy_reader_with_duplicates_and_empty_labels() {
    let fixture = standard_fixture("schema6-series-equivalence", 1024 * 1024, 1024 * 1024);
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open schema-6 series session");
    let symbols = open_symbol_session(&fixture);
    let root = session.load_root().expect("load series root");
    let refs = [3, 1, 1, 0, 2];
    let actual = read_full_entries(&fixture, &session, &root, &symbols, &refs)
        .expect("materialize governed schema-6 entries");
    let actual_entries = session
        .entries(&actual)
        .expect("bind series entries to their session");

    let mut legacy = SeriesReader::open(Cursor::new(fixture.series_bytes.clone()))
        .expect("open legacy series reader");
    let (expected, _) = legacy
        .read_entries_with_bytes(&refs)
        .expect("materialize legacy entries");
    assert_eq!(actual_entries, expected.as_slice());
    assert!(actual_entries[0].1.labels.is_empty());
    assert_eq!(actual_entries[1].1.labels, vec![(1, 11), (2, 20)]);
    assert_eq!(actual_entries[2], actual_entries[1]);
    assert!(actual.charged_bytes() > 0);
    assert!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage).calls > 0,
        "full materialization must use governed cold ranges"
    );
}

#[test]
fn zero_retention_reissues_table_only_after_operation_pins_drop() {
    let fixture = standard_fixture("schema6-series-zero-retention", 0, 1024 * 1024);
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open schema-6 series session");
    let root = session.load_root().expect("load transient root");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    {
        let first =
            read_metadata(&fixture, &session, &root, &[0, 1]).expect("read transient table span");
        assert_eq!(session.routing_entries(&first).unwrap().len(), 2);
    }
    let middle = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    assert_eq!(delta(middle, before).calls, 1);
    let second =
        read_metadata(&fixture, &session, &root, &[0, 1]).expect("reload released table span");
    assert_eq!(session.routing_entries(&second).unwrap().len(), 2);
    assert_eq!(
        delta(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
            middle
        ),
        MetadataIssuedReadCount {
            calls: 1,
            bytes: 2 * SERIES_TABLE_ENTRY_LEN,
        }
    );
}

#[test]
fn tiny_budget_refuses_before_table_io_and_retry_succeeds() {
    let fixture = standard_fixture("schema6-series-budget", 1024 * 1024, 8192);
    let reader = open_reader(&fixture);
    let session = reader
        .query_session()
        .expect("open schema-6 series session");
    let root = session.load_root().expect("reuse cached root");
    let (chunk_index, chunk_index_root) = open_chunk_index_context(&fixture);
    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(8100, MetadataUsageClass::Scratch)
        .expect("reserve competing scratch bytes");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    let error = session
        .read_metadata_entries(&root, &chunk_index, &chunk_index_root, &[0, 1, 2])
        .expect_err("tiny budget must refuse before table I/O");
    assert!(matches!(
        error,
        Schema6SeriesReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    assert_eq!(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage),
        before
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);

    drop(blocker);
    let retried = session
        .read_metadata_entries(&root, &chunk_index, &chunk_index_root, &[0, 1, 2])
        .expect("budget refusal must be retryable");
    assert_eq!(session.routing_entries(&retried).unwrap().len(), 3);
}

#[test]
fn table_and_touched_cold_corruption_are_sticky_but_resource_errors_are_not() {
    let table_corruption = fixture(
        "schema6-series-table-corruption",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            // First SeriesEntryV2.meta_len.
            bytes[SERIES_HEADER_LEN as usize + 36..SERIES_HEADER_LEN as usize + 40]
                .copy_from_slice(&1u32.to_le_bytes());
        },
    );
    let reader = open_reader(&table_corruption);
    let session = reader.query_session().expect("open corruption session");
    let root = session.load_root().expect("load corruption root");
    let before = class_reads(&table_corruption.runtime, MetadataCacheClass::SeriesHotPage);
    assert!(read_metadata(&table_corruption, &session, &root, &[0]).is_err());
    let after = class_reads(&table_corruption.runtime, MetadataCacheClass::SeriesHotPage);
    assert_eq!(delta(after, before).calls, 1);
    table_corruption.runtime.evict_all_resident_metadata();
    assert!(read_metadata(&table_corruption, &session, &root, &[0]).is_err());
    assert_eq!(
        class_reads(&table_corruption.runtime, MetadataCacheClass::SeriesHotPage),
        after
    );
    assert_eq!(
        table_corruption.runtime.snapshot().cache.sticky_artifacts,
        1
    );

    let cold_corruption = fixture(
        "schema6-series-cold-corruption",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            // First SeriesEntryV2.row points beyond its keyset block.
            bytes[SERIES_HEADER_LEN as usize + 28..SERIES_HEADER_LEN as usize + 32]
                .copy_from_slice(&u32::MAX.to_le_bytes());
        },
    );
    let reader = open_reader(&cold_corruption);
    let session = reader
        .query_session()
        .expect("open cold corruption session");
    let symbols = open_symbol_session(&cold_corruption);
    let root = session.load_root().expect("load cold corruption root");
    assert!(read_full_entries(&cold_corruption, &session, &root, &symbols, &[0]).is_err());
    assert_eq!(cold_corruption.runtime.snapshot().cache.sticky_artifacts, 1);

    let row_substitution = fixture(
        "schema6-series-row-substitution",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            // Series zero and one share a keyset. Point series zero at the
            // other valid row; only canonical identity verification can
            // distinguish this from a structurally valid row.
            bytes[SERIES_HEADER_LEN as usize + 28..SERIES_HEADER_LEN as usize + 32]
                .copy_from_slice(&1u32.to_le_bytes());
        },
    );
    let reader = open_reader(&row_substitution);
    let session = reader
        .query_session()
        .expect("open row-substitution session");
    let symbols = open_symbol_session(&row_substitution);
    let root = session.load_root().expect("load row-substitution root");
    assert!(read_full_entries(&row_substitution, &session, &root, &symbols, &[0]).is_err());
    assert_eq!(
        row_substitution.runtime.snapshot().cache.sticky_artifacts,
        1
    );
}

#[test]
fn reserved_fields_are_sticky_at_their_touched_boundaries() {
    let root_corruption = fixture(
        "schema6-series-root-reserved",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            // The fixed header's reserved u32 follows the three counts.
            bytes[20..24].copy_from_slice(&1u32.to_le_bytes());
        },
    );
    let error = GovernedSchema6SeriesReader::open(
        root_corruption
            .registered
            .as_ref()
            .expect("root-corruption owner exists"),
        root_corruption.entries.len() as u32,
    )
    .err()
    .expect("reserved root field must fail");
    assert!(matches!(
        error,
        Schema6SeriesReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(root_corruption.runtime.snapshot().cache.sticky_artifacts, 1);

    let table_corruption = fixture(
        "schema6-series-table-reserved",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            // The first table entry's one-byte flags field follows kind_mask.
            bytes[SERIES_HEADER_LEN as usize + 9] = 1;
        },
    );
    let reader = open_reader(&table_corruption);
    let session = reader.query_session().expect("open table-reserved session");
    let root = session.load_root().expect("load table-reserved root");
    assert!(read_metadata(&table_corruption, &session, &root, &[0]).is_err());
    assert_eq!(
        table_corruption.runtime.snapshot().cache.sticky_artifacts,
        1
    );
}

#[test]
fn lifecycle_and_generation_provenance_do_not_leak_guards_or_files() {
    let shared_runtime = runtime(0, 1024 * 1024);
    let mut first = fixture(
        "schema6-series-owner-first",
        shared_runtime.clone(),
        default_entries(),
        |_| {},
    );
    let second = fixture(
        "schema6-series-owner-second",
        shared_runtime,
        default_entries(),
        |_| {},
    );
    let first_reader = open_reader(&first);
    let second_reader = open_reader(&second);
    let first_symbols = open_symbol_session(&first);
    let second_symbols = open_symbol_session(&second);
    let (first_chunk_index, first_chunk_index_root) = open_chunk_index_context(&first);
    drop(first.registered.take());
    let first_session = first_reader.query_session().expect("open first session");
    let first_root = first_session.load_root().expect("load first root");
    let values = first_session
        .read_metadata_entries(
            &first_root,
            &first_chunk_index,
            &first_chunk_index_root,
            &[0],
        )
        .expect("load first metadata");
    first_session
        .routing_entries(&values)
        .expect("metadata matches first generation");
    let second_session = second_reader.query_session().expect("open second session");
    assert!(matches!(
        second_session.routing_entries(&values),
        Err(Schema6SeriesReaderError::ForeignSegmentGeneration)
    ));
    let (second_chunk_index, second_chunk_index_root) = open_chunk_index_context(&second);
    assert!(matches!(
        first_session.read_metadata_entries(
            &first_root,
            &second_chunk_index,
            &second_chunk_index_root,
            &[0]
        ),
        Err(Schema6SeriesReaderError::ChunkIndex(
            Schema6ChunkIndexReaderError::ForeignSegmentGeneration
        ))
    ));
    assert!(matches!(
        first_session.read_metadata_entries(
            &first_root,
            &first_chunk_index,
            &second_chunk_index_root,
            &[]
        ),
        Err(Schema6SeriesReaderError::ChunkIndex(
            Schema6ChunkIndexReaderError::ForeignSegmentGeneration
        ))
    ));
    assert!(matches!(
        first_session.read_entries(
            &first_root,
            &first_chunk_index,
            &first_chunk_index_root,
            &second_symbols,
            &[0]
        ),
        Err(Schema6SeriesReaderError::Symbols(
            GovernedSymbolReaderError::ForeignSegmentGeneration
        ))
    ));
    first_session
        .read_entries(
            &first_root,
            &first_chunk_index,
            &first_chunk_index_root,
            &first_symbols,
            &[0],
        )
        .expect("matching symbol generation is accepted");

    drop(first_reader);
    assert_eq!(first.runtime.snapshot().cache.registered_artifacts, 14);
    drop(first_root);
    drop(first_session);
    drop(first_symbols);
    drop(first_chunk_index_root);
    drop(first_chunk_index);
    // The provenance token and scratch charge do not own a read guard or
    // registered segment; only the second fixture remains registered.
    assert_eq!(first.runtime.snapshot().cache.registered_artifacts, 7);
    drop(values);
    drop(second_session);
    drop(second_symbols);
    drop(second_chunk_index_root);
    drop(second_chunk_index);
    drop(second_reader);
    drop(second);
    assert_eq!(first.runtime.snapshot().cache.registered_artifacts, 0);
    assert_eq!(first.runtime.snapshot().files.open_files, 0);
}

#[test]
fn root_count_chunk_spans_and_truncation_are_strict() {
    let mismatch = standard_fixture("schema6-series-count-mismatch", 0, 1024 * 1024);
    let error = GovernedSchema6SeriesReader::open(
        mismatch.registered.as_ref().expect("fixture owner exists"),
        5,
    )
    .err()
    .expect("series count mismatch must fail");
    assert!(matches!(
        error,
        Schema6SeriesReaderError::Cache(MetadataCacheError::Structural(_))
    ));

    let invalid_span = fixture(
        "schema6-series-invalid-chunk-span",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            // First SeriesEntryV2.chunk_index_offset is inside the v1
            // directory and is only touched with the table entry.
            bytes[SERIES_HEADER_LEN as usize + 12..SERIES_HEADER_LEN as usize + 20]
                .copy_from_slice(&12u64.to_le_bytes());
        },
    );
    let reader = open_reader(&invalid_span);
    let session = reader.query_session().expect("open invalid-span session");
    let root = session.load_root().expect("load invalid-span root");
    assert!(read_metadata(&invalid_span, &session, &root, &[0]).is_err());
    assert_eq!(invalid_span.runtime.snapshot().cache.sticky_artifacts, 1);

    let aliased_span = fixture(
        "schema6-series-aliased-chunk-span",
        runtime(0, 1024 * 1024),
        default_entries(),
        |bytes| {
            // This is a valid aligned, in-bounds span, but it belongs to
            // series one rather than series zero.
            bytes[SERIES_HEADER_LEN as usize + 12..SERIES_HEADER_LEN as usize + 20]
                .copy_from_slice(&92u64.to_le_bytes());
        },
    );
    let reader = open_reader(&aliased_span);
    let session = reader.query_session().expect("open aliased-span session");
    let root = session.load_root().expect("load aliased-span root");
    assert!(matches!(
        read_metadata(&aliased_span, &session, &root, &[0]),
        Err(Schema6SeriesReaderError::ChunkIndex(
            Schema6ChunkIndexReaderError::Cache(MetadataCacheError::Structural(_))
        ))
    ));
    assert_eq!(aliased_span.runtime.snapshot().cache.sticky_artifacts, 1);

    let truncation = standard_fixture("schema6-series-truncation", 0, 1024 * 1024);
    let reader = open_reader(&truncation);
    let session = reader.query_session().expect("open truncation session");
    let symbols = open_symbol_session(&truncation);
    let root = session.load_root().expect("load truncation root");
    let len = fs::metadata(&truncation.series_path)
        .expect("stat series fixture")
        .len();
    fs::OpenOptions::new()
        .write(true)
        .open(&truncation.series_path)
        .expect("open series fixture for truncation")
        .set_len(len - 1)
        .expect("truncate series fixture");
    assert!(read_full_entries(&truncation, &session, &root, &symbols, &[0]).is_err());
    assert_eq!(truncation.runtime.snapshot().cache.sticky_artifacts, 1);
}
