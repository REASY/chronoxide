use std::fs;

use crc32c::crc32c;
use tempfile::TempDir;

use crate::storage::chunk::{
    ChunkIndexRange, ChunkKind, ChunkOverflowBlobV1, OverflowChunkEntryV1, encode_chunk_index_v2,
};
use crate::storage::metadata_cache::{LIVE_REGISTRY_ENTRY_BYTES, RESIDENT_ENTRY_BYTES};
use crate::storage::metadata_governor::MetadataGovernorConfig;
use crate::storage::metadata_runtime::{SegmentArtifactRegistration, StoreMetadataRuntime};
use crate::storage::segment::SEGMENT_FOOTER_TRACKED_FILES;
use crate::storage::series::SeriesEntry;
use crate::storage::series::cold_v2::SeriesColdV2Plan;
use crate::storage::symbols::{GovernedSymbolReader, write_symbols_bin_v3};

use super::super::{
    InlineChunkV3, OverflowChunksV3, SeriesColdPageDescriptorV1, SeriesHeaderV3Params,
    SeriesHotLocationV3, SeriesHotV3, encode_series_hot_page_v1, encode_series_root_v3,
};
use super::*;

const SEGMENT_START_MS: u64 = 1_000;
const SEGMENT_END_MS: u64 = 2_000;
const CHUNK_FILE_LENS: [u64; 2] = [256, 128];

#[test]
fn canonical_materialization_profile_partitions_outer_elapsed() {
    let mut profile = CanonicalLabelMaterializationProfile {
        canonical_row_decode: Duration::from_nanos(1),
        symbol_resolution: Duration::from_nanos(2),
        canonical_identity: Duration::from_nanos(3),
        label_construction: Duration::from_nanos(4),
    };

    profile.finish_row(Duration::from_nanos(25));

    assert_eq!(profile.canonical_row_decode, Duration::from_nanos(16));
    assert_eq!(profile.attributed(), Duration::from_nanos(25));
    profile.finish_row(Duration::from_nanos(20));
    assert_eq!(profile.attributed(), Duration::from_nanos(25));
}

struct Fixture {
    _directory: TempDir,
    runtime: StoreMetadataRuntime,
    registered: Option<RegisteredSegment>,
    context: Schema7RootBindingContext,
    root_len: u64,
    cold_bytes: Vec<u8>,
    entries: Vec<SeriesEntry>,
}

#[derive(Clone, Copy, Default)]
struct FixtureOptions {
    corrupt_header: bool,
    corrupt_series_root_suffix: bool,
    corrupt_hot: bool,
    corrupt_cold: bool,
    corrupt_overflow_root: bool,
    corrupt_blob: bool,
    duplicate_blob_locator: bool,
    identity_mismatch: bool,
    substitute_row: bool,
    multi_label: bool,
    cross_page_keyset: bool,
    corrupt_second_cold_page: bool,
    corrupt_last_symbol_page: bool,
    symbol_count_limit: Option<usize>,
}

fn runtime() -> StoreMetadataRuntime {
    runtime_with_budgets(1024 * 1024, 1024 * 1024)
}

fn runtime_with_budgets(retained_max_bytes: u64, in_flight_max_bytes: u64) -> StoreMetadataRuntime {
    StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes,
        in_flight_max_bytes,
        max_open_files: 1,
        max_cached_open_files: 0,
    })
    .expect("valid one-descriptor runtime")
}

fn fixture(identity: &str, corrupt_header: bool, corrupt_hot: bool, corrupt_blob: bool) -> Fixture {
    fixture_with_runtime(
        identity,
        FixtureOptions {
            corrupt_header,
            corrupt_hot,
            corrupt_blob,
            ..FixtureOptions::default()
        },
        runtime(),
    )
}

fn fixture_with_runtime(
    identity: &str,
    options: FixtureOptions,
    runtime: StoreMetadataRuntime,
) -> Fixture {
    let directory = TempDir::new().expect("create schema-7 runtime fixture directory");

    let overflow_entries = vec![
        overflow_entry(64, SEGMENT_START_MS + 2),
        overflow_entry(104, SEGMENT_START_MS + 3),
    ];
    let mut chunk_index = encode_chunk_index_v2(
        2,
        &[ChunkOverflowBlobV1 {
            series_ref: 1,
            entries: overflow_entries,
        }],
    )
    .expect("encode schema-7 overflow index");
    let locator = chunk_index.blob_locators[0];

    let (symbols, entries) = if options.cross_page_keyset {
        cross_page_fixture_data()
    } else {
        let symbols = fixture_symbols();
        let entries = if options.multi_label {
            multi_label_fixture_entries(&symbols)
        } else {
            fixture_entries(&symbols)
        };
        (symbols, entries)
    };
    let cold = SeriesColdV2Plan::build(&entries).expect("build schema-7 cold fixture");
    let cold_lengths = cold.lengths();

    let header = SeriesHeaderV3::new(SeriesHeaderV3Params {
        num_series: 2,
        num_keysets: cold.num_keysets(),
        num_value_dicts: cold.num_value_dicts(),
        chunk_index_root_crc32c: chunk_index.root.root_crc32c,
        keysets_len: cold_lengths.keysets,
        value_dicts_len: cold_lengths.value_dicts,
        keyset_blocks_len: cold_lengths.keyset_blocks,
        segment_start_ms: SEGMENT_START_MS,
        segment_end_ms: SEGMENT_END_MS,
        chunk_index_file_len: chunk_index.root.file_len,
    })
    .expect("construct schema-7 series header");
    let cold_rows = cold.series_rows();
    let mut records = vec![
        SeriesHotV3 {
            series_id: entries[0].series_id,
            keyset_id: cold_rows[0].keyset_id,
            row: cold_rows[0].row,
            kind_mask: 1,
            location: SeriesHotLocationV3::Inline(InlineChunkV3 {
                chunk_kind: ChunkKind::Float as u8,
                file_id: 0,
                scalar_lane_len: 0,
                min_time_delta_ms: 0,
                max_time_delta_ms: 1,
                file_offset: 0,
                chunk_length: 40,
                indexed_prefix_crc32c: 0x1111_1111,
            }),
        },
        SeriesHotV3 {
            series_id: entries[1].series_id,
            keyset_id: cold_rows[1].keyset_id,
            row: cold_rows[1].row,
            kind_mask: 1,
            location: SeriesHotLocationV3::Overflow(OverflowChunksV3 {
                blob_offset: locator.blob_offset,
                blob_len: locator.blob_len,
                chunk_count: locator.chunk_count,
            }),
        },
    ];
    if options.identity_mismatch {
        records[0].series_id ^= 1;
    }
    if options.substitute_row {
        assert_eq!(records[0].keyset_id, records[1].keyset_id);
        records[0].row = records[1].row;
    }
    if options.duplicate_blob_locator {
        records[0].location = SeriesHotLocationV3::Overflow(OverflowChunksV3 {
            blob_offset: locator.blob_offset,
            blob_len: locator.blob_len,
            chunk_count: locator.chunk_count,
        });
    }
    let (hot_descriptor, mut hot_page) =
        encode_series_hot_page_v1(header, 0, &records, CHUNK_FILE_LENS)
            .expect("encode schema-7 hot page");
    let cold_offsets = cold
        .section_offsets_at(header.keysets_offset)
        .expect("derive schema-7 cold offsets");
    let mut cold_bytes = Vec::new();
    cold.write_sections_at(&mut cold_bytes, cold_offsets)
        .expect("encode schema-7 cold bytes");
    let cold_descriptors = cold_bytes
        .chunks(super::super::SERIES_COLD_PAGE_LEN_V1 as usize)
        .enumerate()
        .map(|(page_index, bytes)| {
            SeriesColdPageDescriptorV1::new(
                header,
                u32::try_from(page_index).expect("cold page index fits u32"),
                crc32c(bytes),
            )
            .expect("construct schema-7 cold descriptor")
        })
        .collect::<Vec<_>>();
    let (header, mut root) = encode_series_root_v3(header, &[hot_descriptor], &cold_descriptors)
        .expect("encode schema-7 series root");

    if options.corrupt_header {
        root[0] ^= 1;
    }
    if options.corrupt_series_root_suffix {
        root[SERIES_HEADER_LEN_V3] ^= 1;
    }
    if options.corrupt_hot {
        hot_page[8_192] ^= 1;
    }
    if options.corrupt_cold {
        cold_bytes[0] ^= 1;
    }
    if options.corrupt_second_cold_page {
        cold_bytes[super::super::SERIES_COLD_PAGE_LEN_V1 as usize] ^= 1;
    }
    if options.corrupt_overflow_root {
        chunk_index.bytes[0] ^= 1;
    }
    if options.corrupt_blob {
        let last = chunk_index.bytes.len() - 1;
        chunk_index.bytes[last] ^= 1;
    }

    let mut symbol_bytes = Vec::new();
    let encoded_symbol_count = options.symbol_count_limit.unwrap_or(symbols.len());
    write_symbols_bin_v3(
        &mut symbol_bytes,
        symbols
            .get(..encoded_symbol_count)
            .expect("fixture symbol limit is in range")
            .iter(),
    )
    .expect("encode schema-7 symbols fixture");
    if options.corrupt_last_symbol_page {
        *symbol_bytes
            .last_mut()
            .expect("fixture symbols must contain a physical page") ^= 1;
    }

    let mut series = root;
    series.extend_from_slice(&hot_page);
    series.extend_from_slice(&cold_bytes);
    assert_eq!(series.len() as u64, header.file_len);

    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let bytes: &[u8] = match file {
            SegmentFile::MetaJson => b"{}",
            SegmentFile::Symbols => &symbol_bytes,
            SegmentFile::Series => &series,
            SegmentFile::Chunks => &[0; CHUNK_FILE_LENS[0] as usize],
            SegmentFile::OooChunks => &[0; CHUNK_FILE_LENS[1] as usize],
            SegmentFile::ChunkIndex => &chunk_index.bytes,
            SegmentFile::Indexes => b"indexes",
            SegmentFile::Footer => unreachable!("footer is not runtime-inventoried"),
        };
        let path = directory.path().join(file.filename());
        fs::write(&path, bytes).expect("write schema-7 runtime artifact");
        SegmentArtifactRegistration::new(file, path, bytes.len() as u64)
    });
    let registered = runtime
        .register_segment(identity, &artifacts)
        .expect("register schema-7 runtime fixture");

    Fixture {
        _directory: directory,
        runtime,
        registered: Some(registered),
        context: Schema7RootBindingContext {
            series_file_len: header.file_len,
            chunk_index_file_len: chunk_index.root.file_len,
            segment_start_ms: SEGMENT_START_MS,
            segment_end_ms: SEGMENT_END_MS,
            series_count: 2,
        },
        root_len: header.hot_pages_offset,
        cold_bytes,
        entries,
    }
}

fn fixture_symbols() -> Vec<String> {
    (0..=5)
        .map(|symbol_id| format!("s{symbol_id:02}"))
        .collect()
}

fn fixture_entries(symbols: &[String]) -> Vec<SeriesEntry> {
    let mut entries = vec![
        SeriesEntry {
            series_id: 0,
            kind_mask: 1,
            chunk_index: ChunkIndexRange::default(),
            labels: vec![(1, 3)],
        },
        SeriesEntry {
            series_id: 0,
            kind_mask: 1,
            chunk_index: ChunkIndexRange::default(),
            labels: vec![(1, 4)],
        },
    ];
    for entry in &mut entries {
        let mut hash = XxHash64::default();
        for &(key_sym, value_sym) in &entry.labels {
            hash.update(symbols[key_sym as usize].as_bytes());
            hash.update(&[0]);
            hash.update(symbols[value_sym as usize].as_bytes());
            hash.update(&[0xff]);
        }
        entry.series_id = hash.finish();
    }
    entries
}

fn multi_label_fixture_entries(symbols: &[String]) -> Vec<SeriesEntry> {
    let mut entries = fixture_entries(symbols);
    for entry in &mut entries {
        entry.labels.push((2, 5));
        let mut hash = XxHash64::default();
        for &(key_sym, value_sym) in &entry.labels {
            hash.update(symbols[key_sym as usize].as_bytes());
            hash.update(&[0]);
            hash.update(symbols[value_sym as usize].as_bytes());
            hash.update(&[0xff]);
        }
        entry.series_id = hash.finish();
    }
    entries
}

fn cross_page_fixture_data() -> (Vec<String>, Vec<SeriesEntry>) {
    const LARGE_KEY_COUNT: u32 = 4_087;
    let value_sym = LARGE_KEY_COUNT + 1;
    let symbols = (0..=value_sym)
        .map(|symbol_id| format!("s{symbol_id:04}"))
        .collect::<Vec<_>>();
    let mut entries = vec![
        SeriesEntry {
            series_id: 0,
            kind_mask: 1,
            chunk_index: ChunkIndexRange::default(),
            labels: (0..LARGE_KEY_COUNT)
                .map(|key_sym| (key_sym, value_sym))
                .collect(),
        },
        SeriesEntry {
            series_id: 0,
            kind_mask: 1,
            chunk_index: ChunkIndexRange::default(),
            labels: vec![(LARGE_KEY_COUNT, value_sym)],
        },
    ];
    for entry in &mut entries {
        let mut hash = XxHash64::default();
        for &(key_sym, value_sym) in &entry.labels {
            hash.update(symbols[key_sym as usize].as_bytes());
            hash.update(&[0]);
            hash.update(symbols[value_sym as usize].as_bytes());
            hash.update(&[0xff]);
        }
        entry.series_id = hash.finish();
    }
    (symbols, entries)
}

fn open_symbol_session(fixture: &Fixture) -> GovernedSymbolSession {
    let reader = GovernedSymbolReader::open(
        fixture
            .registered
            .as_ref()
            .expect("fixture owner available"),
    )
    .expect("open governed symbol reader");
    reader
        .query_session()
        .expect("open governed symbol session")
}

fn overflow_entry(offset: u64, timestamp_ms: u64) -> OverflowChunkEntryV1 {
    OverflowChunkEntryV1 {
        file_id: 0,
        kind: ChunkKind::Float,
        min_time_ms: timestamp_ms,
        max_time_ms: timestamp_ms,
        offset,
        length: 40,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
        indexed_prefix_crc32c: offset as u32,
    }
}

fn open_fixture(fixture: &mut Fixture) -> Schema7MetadataReader {
    Schema7MetadataReader::open(
        fixture
            .registered
            .as_ref()
            .expect("fixture owner available"),
        fixture.context,
    )
    .expect("open strict schema-7 metadata reader")
}

fn class_reads(
    runtime: &StoreMetadataRuntime,
    class: MetadataCacheClass,
) -> crate::storage::metadata_runtime::MetadataIssuedReadCount {
    runtime.snapshot().reads.classes[class.stable_index()].issued
}

fn read_delta(
    after: crate::storage::metadata_runtime::MetadataIssuedReadCount,
    before: crate::storage::metadata_runtime::MetadataIssuedReadCount,
) -> crate::storage::metadata_runtime::MetadataIssuedReadCount {
    crate::storage::metadata_runtime::MetadataIssuedReadCount {
        calls: after.calls - before.calls,
        bytes: after.bytes - before.bytes,
    }
}

#[test]
fn open_stages_exact_root_ranges_and_warm_roots_issue_no_io() {
    let mut fixture = fixture("schema7-roots", false, false, false);
    let before = fixture.runtime.snapshot();
    let reader = open_fixture(&mut fixture);
    let open_delta = fixture.runtime.snapshot().reads.delta_since(before.reads);

    assert_eq!(reader.segment_identity(), "schema7-roots");
    assert_eq!(reader.root_len(), fixture.root_len);
    assert_eq!(open_delta.issued.calls, 3);
    assert_eq!(open_delta.issued.bytes, fixture.root_len + 64);
    assert_eq!(
        open_delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 2,
            bytes: fixture.root_len,
        }
    );
    assert_eq!(
        open_delta.classes[MetadataCacheClass::OverflowRoot.stable_index()].issued,
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 1,
            bytes: 64,
        }
    );
    assert_eq!(fixture.runtime.snapshot().files.peak_open_files, 1);

    let session = reader.query_session().expect("open query session");
    let before_warm = fixture.runtime.snapshot();
    let roots = session.load_roots().expect("load warm roots");
    let series_root_charge = roots.series.charged_bytes();
    let overflow_root_charge = roots.overflow.charged_bytes();
    let root_usage = fixture.runtime.snapshot().governor;
    assert_eq!(
        root_usage
            .usage(MetadataUsageClass::Cache(MetadataCacheClass::SeriesRoot))
            .retained_bytes,
        series_root_charge + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
    );
    assert_eq!(
        root_usage
            .usage(MetadataUsageClass::Cache(MetadataCacheClass::OverflowRoot,))
            .retained_bytes,
        overflow_root_charge + LIVE_REGISTRY_ENTRY_BYTES + RESIDENT_ENTRY_BYTES
    );
    let bound = session.bind(roots).expect("bind warm roots");
    let warm_delta = fixture.runtime.snapshot();
    assert_eq!(
        warm_delta.reads.delta_since(before_warm.reads).issued.calls,
        0
    );
    assert_eq!(warm_delta.cache.hits - before_warm.cache.hits, 2);
    assert_eq!(bound.series_pages().root_len, fixture.root_len);
    assert_eq!(bound.overflow_blobs().root_len, 64);
}

#[test]
fn zero_retention_reads_transiently_without_resident_cache_entries() {
    let runtime = runtime_with_budgets(0, 1024 * 1024);
    let mut fixture = fixture_with_runtime(
        "schema7-zero-retention",
        FixtureOptions::default(),
        runtime.clone(),
    );

    let reader = open_fixture(&mut fixture);
    let after_open = runtime.snapshot();
    assert_eq!(after_open.cache.resident_entries, 0);
    assert_eq!(after_open.governor.retained_bytes, 0);
    assert_eq!(after_open.cache.active_loads, 0);
    assert_eq!(after_open.files.open_files, 0);

    let session = reader
        .query_session()
        .expect("open transient query session");
    let roots = session.load_roots().expect("load transient roots");
    let bound = session.bind(roots).expect("bind transient roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0, 1])
        .expect("plan transient hot page");
    assert_eq!(planned.len(), 2);

    let while_pinned = runtime.snapshot();
    assert_eq!(while_pinned.cache.resident_entries, 0);
    assert_eq!(while_pinned.governor.retained_bytes, 0);
    assert_eq!(while_pinned.cache.active_loads, 0);
    assert_eq!(while_pinned.files.open_files, 0);
    assert_eq!(
        while_pinned
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        planned.charged_bytes()
    );

    drop(planned);
    drop(bound);
    drop(session);
    drop(reader);
    let after_drop = runtime.snapshot();
    assert_eq!(after_drop.cache.resident_entries, 0);
    assert_eq!(after_drop.cache.live_allocations, 0);
    assert_eq!(after_drop.cache.active_loads, 0);
    assert_eq!(after_drop.governor.retained_bytes, 0);
    assert_eq!(
        after_drop
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        0
    );
    assert_eq!(after_drop.files.open_files, 0);
}

#[test]
fn tiny_in_flight_budget_refuses_hot_page_before_io_without_poisoning() {
    let runtime = runtime_with_budgets(1024 * 1024, 16 * 1024);
    let mut fixture = fixture_with_runtime(
        "schema7-tiny-in-flight",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let reader = open_fixture(&mut fixture);
    let session = reader
        .query_session()
        .expect("open constrained query session");
    let roots = session.load_roots().expect("load constrained roots");
    let bound = session.bind(roots).expect("bind constrained roots");
    let before = runtime.snapshot();

    let error = session
        .plan_hot_page(&bound, 0, &[0, 1])
        .expect_err("hot-page scratch reservation must exceed the budget");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Budget(_))
    ));

    let after = runtime.snapshot();
    assert_eq!(after.reads, before.reads);
    assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
    assert_eq!(after.cache.active_loads, 0);
    assert_eq!(after.cache.live_allocations, before.cache.live_allocations);
    assert_eq!(
        after
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        before
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes
    );
    assert_eq!(
        after.governor.in_flight_bytes,
        before.governor.in_flight_bytes
    );
    assert_eq!(after.files.open_files, 0);
}

#[test]
fn foreign_roots_bounds_and_plans_are_rejected_before_io_or_poisoning() {
    let runtime = runtime();
    let mut first = fixture_with_runtime(
        "schema7-provenance-a",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let mut second = fixture_with_runtime(
        "schema7-provenance-b",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let first_reader = open_fixture(&mut first);
    let second_reader = open_fixture(&mut second);
    let first_session = first_reader.query_session().expect("first query session");
    let second_session = second_reader.query_session().expect("second query session");

    let foreign_roots = first_session.load_roots().expect("first roots");
    let before_foreign_roots = runtime.snapshot();
    let error = second_session
        .bind(foreign_roots)
        .expect_err("foreign root pins must not bind");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::ForeignSegmentGeneration
    ));
    let after_foreign_roots = runtime.snapshot();
    assert_eq!(after_foreign_roots.reads, before_foreign_roots.reads);
    assert_eq!(
        after_foreign_roots.cache.sticky_artifacts,
        before_foreign_roots.cache.sticky_artifacts
    );

    let first_roots = first_session.load_roots().expect("reload first roots");
    let first_bound = first_session.bind(first_roots).expect("bind first roots");
    let second_roots = second_session.load_roots().expect("load second roots");
    let second_bound = second_session
        .bind(second_roots)
        .expect("bind second roots");
    let series_count = first_session
        .series_count_binding(&first_bound)
        .expect("mint first schema-7 series-count capability");
    assert_eq!(series_count.num_series(), 2);
    let before_foreign_count = runtime.snapshot();
    let error = second_session
        .series_count_binding(&first_bound)
        .expect_err("foreign bound roots must not mint a series-count capability");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::ForeignSegmentGeneration
    ));
    assert_eq!(runtime.snapshot().reads, before_foreign_count.reads);
    assert_eq!(
        runtime.snapshot().cache.sticky_artifacts,
        before_foreign_count.cache.sticky_artifacts
    );
    let first_planned = first_session
        .plan_hot_page(&first_bound, 0, &[1])
        .expect("plan first overflow series");

    let before_foreign_values = runtime.snapshot();
    let error = second_session
        .load_hot_page(&first_bound, 0)
        .expect_err("foreign bound roots must not load a page");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::ForeignSegmentGeneration
    ));
    let error = second_session
        .plan_overflow_blob(
            &second_bound,
            first_planned.get(0).expect("first planned overflow series"),
        )
        .expect_err("foreign planned series must not resolve a blob");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::ForeignSegmentGeneration
    ));
    let after_foreign_values = runtime.snapshot();
    assert_eq!(after_foreign_values.reads, before_foreign_values.reads);
    assert_eq!(
        after_foreign_values.cache.sticky_artifacts,
        before_foreign_values.cache.sticky_artifacts
    );
    assert_eq!(
        after_foreign_values
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        before_foreign_values
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes
    );
}

#[test]
fn exact_hot_cold_and_blob_ranges_are_cached_and_planner_output_is_governed() {
    let mut fixture = fixture("schema7-touched", false, false, false);
    let reader = open_fixture(&mut fixture);
    let session = reader.query_session().expect("open query session");
    let roots = session.load_roots().expect("load roots");
    let bound = session.bind(roots).expect("bind roots");

    let before_hot = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    let planned = session
        .plan_hot_page(&bound, 0, &[0, 1])
        .expect("plan selected schema-7 series");
    let after_hot = class_reads(&fixture.runtime, MetadataCacheClass::SeriesHotPage);
    assert_eq!(
        read_delta(after_hot, before_hot),
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 1,
            bytes: SERIES_HOT_PAGE_LEN_V1 as u64,
        }
    );
    assert_eq!(planned.len(), 2);
    assert!(!planned.is_empty());
    assert_eq!(planned.get(0).expect("first planned series").series_ref, 0);
    assert_eq!(planned.get(1).expect("second planned series").series_ref, 1);
    let after_hot_snapshot = fixture.runtime.snapshot();
    assert_eq!(
        after_hot_snapshot
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        planned.charged_bytes()
    );
    assert_eq!(
        after_hot_snapshot
            .governor
            .usage(MetadataUsageClass::Cache(MetadataCacheClass::SeriesHotPage,))
            .retained_bytes,
        std::mem::size_of::<ValidatedSeriesHotPage>() as u64
            + SERIES_HOT_PAGE_LEN_V1 as u64
            + LIVE_REGISTRY_ENTRY_BYTES
            + RESIDENT_ENTRY_BYTES
    );

    let before_hot_hit = fixture.runtime.snapshot();
    let second_page = session
        .load_hot_page(&bound, 0)
        .expect("reuse authenticated hot page");
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .reads
            .delta_since(before_hot_hit.reads)
            .issued
            .calls,
        0
    );

    let before_cold = class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage);
    let cold = session
        .load_cold_page(&bound, 0)
        .expect("load authenticated cold page");
    assert_eq!(
        read_delta(
            class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage),
            before_cold,
        ),
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 1,
            bytes: fixture.cold_bytes.len() as u64,
        }
    );
    assert_eq!(
        cold.bytes_for(
            bound.series_root().header,
            0,
            bound.cold_descriptor(0).expect("cold descriptor"),
        )
        .expect("bind cold cache hit"),
        fixture.cold_bytes
    );
    let before_cold_hit = fixture.runtime.snapshot();
    let _second_cold = session
        .load_cold_page(&bound, 0)
        .expect("reuse authenticated cold page");
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .reads
            .delta_since(before_cold_hit.reads)
            .issued
            .calls,
        0
    );
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Cache(
                MetadataCacheClass::SeriesColdPage,
            ))
            .retained_bytes,
        std::mem::size_of::<ValidatedSeriesColdPage>() as u64
            + fixture.cold_bytes.len() as u64
            + LIVE_REGISTRY_ENTRY_BYTES
            + RESIDENT_ENTRY_BYTES
    );

    let scratch_with_planned = fixture
        .runtime
        .snapshot()
        .governor
        .usage(MetadataUsageClass::Scratch)
        .in_flight_bytes;
    assert_eq!(scratch_with_planned, planned.charged_bytes());
    let before_blob = class_reads(&fixture.runtime, MetadataCacheClass::OverflowBlob);
    let overflow = planned.get(1).expect("overflow planned series");
    let batch = session
        .plan_overflow_blob(&bound, overflow)
        .expect("plan authenticated overflow locators");
    assert_eq!(batch.locators().len(), 2);
    assert_eq!(batch.series_spans().len(), 1);
    assert_eq!(
        read_delta(
            class_reads(&fixture.runtime, MetadataCacheClass::OverflowBlob),
            before_blob,
        ),
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 1,
            bytes: 32 + 2 * 44,
        }
    );
    let after_blob_snapshot = fixture.runtime.snapshot();
    assert_eq!(
        after_blob_snapshot
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        scratch_with_planned + batch.charged_bytes()
    );
    assert_eq!(
        after_blob_snapshot
            .governor
            .usage(MetadataUsageClass::Cache(MetadataCacheClass::OverflowBlob))
            .retained_bytes,
        std::mem::size_of::<ValidatedOverflowBlob>() as u64
            + (32 + 2 * 44) as u64
            + LIVE_REGISTRY_ENTRY_BYTES
            + RESIDENT_ENTRY_BYTES
    );
    let before_blob_hit = fixture.runtime.snapshot();
    let _second_blob = session
        .load_overflow_blob(&bound, overflow)
        .expect("reuse authenticated overflow blob");
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .reads
            .delta_since(before_blob_hit.reads)
            .issued
            .calls,
        0
    );

    drop(batch);
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        scratch_with_planned
    );
    drop(planned);
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        0
    );
    drop(second_page);
}

#[test]
fn materialize_verified_returns_owned_canonical_labels_and_stable_identity() {
    let mut fixture = fixture("schema7-materialize", false, false, false);
    let expected = fixture_entries(&fixture_symbols());
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open materialization session");
    let roots = session.load_roots().expect("load materialization roots");
    let bound = session.bind(roots).expect("bind materialization roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan materialized series");
    let routed = planned.get(0).expect("planned materialized series");

    let before = fixture.runtime.snapshot();
    let before_symbols = symbols.logical_stats();
    let verified = session
        .materialize_verified(&bound, &symbols, routed)
        .expect("materialize and verify canonical labels");
    assert_eq!(verified.series_ref(), 0);
    assert_eq!(verified.series_id(), expected[0].series_id);
    assert_eq!(verified.kind_mask(), expected[0].kind_mask);
    assert_eq!(
        verified.labels(),
        &[(String::from("s01"), String::from("s03"))]
    );
    let expected_charge = (verified.labels.capacity() * std::mem::size_of::<(String, String)>())
        as u64
        + verified
            .labels
            .iter()
            .map(|(key, value)| (key.capacity() + value.capacity()) as u64)
            .sum::<u64>();
    assert_eq!(verified.charged_bytes(), expected_charge);
    let after = fixture.runtime.snapshot();
    assert!(
        after.reads.delta_since(before.reads).classes
            [MetadataCacheClass::SeriesColdPage.stable_index()]
        .issued
        .calls
            >= 1
    );
    assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
    let after_symbols = symbols.logical_stats();
    assert_eq!(
        after_symbols.returned_values - before_symbols.returned_values,
        2,
        "each canonical name/value must be resolved exactly once"
    );
    assert_eq!(
        after_symbols.returned_utf8_bytes - before_symbols.returned_utf8_bytes,
        6
    );

    let before_warm = fixture.runtime.snapshot();
    let before_warm_symbols = symbols.logical_stats();
    let second = session
        .materialize_verified(&bound, &symbols, routed)
        .expect("repeat materialization from authenticated cache values");
    assert_eq!(second.series_id(), verified.series_id());
    assert_eq!(second.labels(), verified.labels());
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .reads
            .delta_since(before_warm.reads)
            .issued
            .calls,
        0
    );
    let after_warm_symbols = symbols.logical_stats();
    assert_eq!(
        after_warm_symbols.returned_values - before_warm_symbols.returned_values,
        2
    );
    assert_eq!(
        after_warm_symbols.returned_utf8_bytes - before_warm_symbols.returned_utf8_bytes,
        6
    );
}

#[test]
fn selective_materialization_owns_only_requested_labels_but_hashes_every_pair() {
    let mut fixture = fixture_with_runtime(
        "schema7-selective-materialization",
        FixtureOptions {
            multi_label: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let expected = multi_label_fixture_entries(&fixture_symbols());
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open selective materialization session");
    let roots = session
        .load_roots()
        .expect("load selective materialization roots");
    let bound = session
        .bind(roots)
        .expect("bind selective materialization roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan selectively materialized series");
    let routed = planned
        .get(0)
        .expect("planned selectively materialized series");
    let mut context = session
        .materialization_context(&bound, planned.len())
        .expect("create selective materialization context");
    let requested = vec![String::from("s02")];

    let before_symbols = symbols.logical_stats();
    let selected = session
        .materialize_verified_selected_cached(
            &bound,
            &symbols,
            &mut context,
            routed,
            &requested,
            true,
        )
        .expect("selectively materialize and verify canonical labels");
    assert_eq!(selected.series_id(), expected[0].series_id);
    assert_eq!(
        selected.metric_name_dropped_series_id(),
        Some(expected[0].series_id),
        "this fixture has no __name__ pair, so the derived identity is unchanged"
    );
    assert!(!selected.labels_complete());
    assert_eq!(
        selected.labels(),
        &[(String::from("s02"), String::from("s05"))]
    );
    let after_symbols = symbols.logical_stats();
    assert_eq!(
        after_symbols.returned_values - before_symbols.returned_values,
        4,
        "both components of both canonical pairs must still resolve"
    );
    assert_eq!(
        after_symbols.returned_utf8_bytes - before_symbols.returned_utf8_bytes,
        12
    );
    let expected_charge = (selected.labels.capacity() * std::mem::size_of::<(String, String)>())
        as u64
        + selected
            .labels
            .iter()
            .map(|(key, value)| (key.capacity() + value.capacity()) as u64)
            .sum::<u64>();
    assert_eq!(selected.charged_bytes(), expected_charge);

    let selected_without_derived_identity = session
        .materialize_verified_selected_cached(
            &bound,
            &symbols,
            &mut context,
            routed,
            &requested,
            false,
        )
        .expect("selectively materialize without an unused range identity");
    assert!(!selected_without_derived_identity.labels_complete());
    assert_eq!(
        selected_without_derived_identity.metric_name_dropped_series_id(),
        None
    );
    assert_eq!(
        selected_without_derived_identity.labels(),
        selected.labels()
    );

    let full = session
        .materialize_verified(&bound, &symbols, routed)
        .expect("full wrapper must retain established behavior");
    assert!(full.labels_complete());
    assert_eq!(full.metric_name_dropped_series_id(), None);
    assert_eq!(full.labels().len(), 2);
    assert!(selected.charged_bytes() < full.charged_bytes());
}

#[test]
fn selective_materialization_cannot_hide_omitted_identity_mismatch() {
    let mut fixture = fixture_with_runtime(
        "schema7-selective-identity-mismatch",
        FixtureOptions {
            identity_mismatch: true,
            multi_label: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open selective identity session");
    let roots = session.load_roots().expect("load selective identity roots");
    let bound = session.bind(roots).expect("bind selective identity roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan selectively omitted labels");
    let routed = planned.get(0).expect("planned mismatched series");

    let before_symbols = symbols.logical_stats();
    let error = session
        .materialize_verified_selected(&bound, &symbols, routed, &[], false)
        .expect_err("omitting every owned label must not bypass identity verification");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_symbols = symbols.logical_stats();
    assert_eq!(
        after_symbols.returned_values - before_symbols.returned_values,
        4,
        "omitted labels must still resolve before the mismatch is reported"
    );
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);

    session
        .materialize_verified_selected(&bound, &symbols, routed, &[], false)
        .expect_err("sticky omitted-label identity corruption must gate retry");
}

#[test]
fn selective_materialization_cannot_hide_an_omitted_symbol_bounds_error() {
    let mut fixture = fixture_with_runtime(
        "schema7-selective-omitted-symbol-bounds",
        FixtureOptions {
            multi_label: true,
            symbol_count_limit: Some(5),
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open omitted-symbol-bounds session");
    let roots = session
        .load_roots()
        .expect("load omitted-symbol-bounds roots");
    let bound = session
        .bind(roots)
        .expect("bind omitted-symbol-bounds roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan row with omitted out-of-bounds value symbol");
    let requested = vec![String::from("s01")];

    let error = session
        .materialize_verified_selected(
            &bound,
            &symbols,
            planned.get(0).expect("planned malformed row"),
            &requested,
            false,
        )
        .expect_err("an invalid omitted label symbol must fail complete row validation");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn lazy_materialization_reuses_decoded_cold_metadata_between_visited_rows() {
    let runtime = runtime_with_budgets(0, 1024 * 1024);
    let mut fixture = fixture_with_runtime(
        "schema7-lazy-materialization-reuse",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open lazy materialization session");
    let roots = session
        .load_roots()
        .expect("load lazy materialization roots");
    let bound = session
        .bind(roots)
        .expect("bind lazy materialization roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0, 1])
        .expect("plan shared-keyset rows");
    let mut context = session
        .materialization_context(&bound, planned.len())
        .expect("create lazy materialization context");

    let first = session
        .materialize_verified_cached(
            &bound,
            &symbols,
            &mut context,
            planned.get(0).expect("first shared-keyset row"),
        )
        .expect("materialize first shared-keyset row");
    assert_eq!(first.series_id(), fixture.entries[0].series_id);
    let before_second = class_reads(&runtime, MetadataCacheClass::SeriesColdPage);
    let second = session
        .materialize_verified_cached(
            &bound,
            &symbols,
            &mut context,
            planned.get(1).expect("second shared-keyset row"),
        )
        .expect("materialize second shared-keyset row");
    assert_eq!(second.series_id(), fixture.entries[1].series_id);
    assert_eq!(
        read_delta(
            class_reads(&runtime, MetadataCacheClass::SeriesColdPage),
            before_second,
        )
        .calls,
        1,
        "only the newly visited row should reload a cold page with zero retention"
    );
}

#[test]
fn materialization_context_budget_refusal_falls_back_to_scalar_decode() {
    let runtime = runtime_with_budgets(1024 * 1024, 128 * 1024);
    let mut fixture = fixture_with_runtime(
        "schema7-lazy-materialization-budget-fallback",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open budget fallback session");
    let roots = session.load_roots().expect("load budget fallback roots");
    let bound = session.bind(roots).expect("bind budget fallback roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan budget fallback row");
    let usage = runtime.snapshot().governor;
    let competing_bytes = usage
        .in_flight_max_bytes
        .checked_sub(usage.in_flight_bytes)
        .and_then(|bytes| bytes.checked_sub(1))
        .expect("leave one in-flight byte for context reservation");
    let blocker = runtime
        .governor()
        .reserve_in_flight_for_usage(competing_bytes, MetadataUsageClass::Scratch)
        .expect("reserve competing context scratch");
    let mut context = session
        .materialization_context(&bound, planned.len())
        .expect("context reservation refusal is an optimization fallback");
    assert!(context.cache.is_none());
    drop(blocker);

    let verified = session
        .materialize_verified_cached(
            &bound,
            &symbols,
            &mut context,
            planned.get(0).expect("budget fallback row"),
        )
        .expect("scalar fallback must materialize the row");
    assert_eq!(verified.series_id(), fixture.entries[0].series_id);
}

#[test]
fn materialization_context_propagates_planning_overflow() {
    let runtime = runtime();
    let mut fixture = fixture_with_runtime(
        "schema7-materialization-context-overflow",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let reader = open_fixture(&mut fixture);
    let session = reader
        .query_session()
        .expect("open planning-overflow session");
    let roots = session.load_roots().expect("load planning-overflow roots");
    let bound = session.bind(roots).expect("bind planning-overflow roots");
    let before = runtime.snapshot();

    let error = session
        .materialization_context(&bound, usize::MAX)
        .err()
        .expect("planning overflow must not be swallowed as an optional cache miss");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Planning(ref error)
            if error.kind() == io::ErrorKind::InvalidInput
    ));
    let after = runtime.snapshot();
    assert_eq!(after.reads, before.reads);
    assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
    assert_eq!(
        after.governor.in_flight_bytes,
        before.governor.in_flight_bytes
    );
}

#[test]
fn materialize_verified_rejects_identity_mismatch_and_makes_it_sticky() {
    let mut fixture = fixture_with_runtime(
        "schema7-identity-mismatch",
        FixtureOptions {
            identity_mismatch: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader.query_session().expect("open identity session");
    let roots = session.load_roots().expect("load identity roots");
    let bound = session.bind(roots).expect("bind identity roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan mismatched identity series");
    let routed = planned.get(0).expect("planned mismatched series");

    let error = session
        .materialize_verified(&bound, &symbols, routed)
        .expect_err("mismatched stored identity must fail");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = fixture.runtime.snapshot();
    assert_eq!(after.cache.sticky_artifacts, 1);

    session
        .materialize_verified(&bound, &symbols, routed)
        .expect_err("sticky identity corruption must gate retry");
    assert_eq!(fixture.runtime.snapshot().reads, after.reads);
}

#[test]
fn materialize_verified_rejects_warm_row_substitution_without_more_io() {
    let mut fixture = fixture_with_runtime(
        "schema7-row-substitution",
        FixtureOptions {
            substitute_row: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader.query_session().expect("open substitution session");
    let roots = session.load_roots().expect("load substitution roots");
    let bound = session.bind(roots).expect("bind substitution roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0, 1])
        .expect("plan substituted and canonical rows");
    let substituted = planned.get(0).expect("substituted row plan");
    let canonical = planned.get(1).expect("canonical row plan");

    let warmed = session
        .materialize_verified(&bound, &symbols, canonical)
        .expect("warm the shared row, dictionary, and symbol pages");
    assert_eq!(warmed.series_id(), fixture.entries[1].series_id);
    let before = fixture.runtime.snapshot();
    let error = session
        .materialize_verified(&bound, &symbols, substituted)
        .expect_err("valid row substitution must fail identity verification");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = fixture.runtime.snapshot();
    assert_eq!(after.reads, before.reads);
    assert_eq!(
        after.cache.sticky_artifacts,
        before.cache.sticky_artifacts + 1
    );

    session
        .materialize_verified(&bound, &symbols, substituted)
        .expect_err("sticky substitution must gate retry");
    assert_eq!(fixture.runtime.snapshot().reads, after.reads);
}

#[test]
fn cross_page_keyset_materializes_only_after_both_pages_authenticate() {
    let mut fixture = fixture_with_runtime(
        "schema7-cross-page-keyset",
        FixtureOptions {
            cross_page_keyset: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    assert!(
        fixture.cold_bytes.len() > super::super::SERIES_COLD_PAGE_LEN_V1 as usize,
        "fixture must span at least two physical cold pages"
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader.query_session().expect("open cross-page session");
    let roots = session.load_roots().expect("load cross-page roots");
    let bound = session.bind(roots).expect("bind cross-page roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[1])
        .expect("plan cross-page keyset series");
    let before = class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage);
    let verified = session
        .materialize_verified(
            &bound,
            &symbols,
            planned.get(0).expect("cross-page series plan"),
        )
        .expect("materialize cross-page keyset");
    assert_eq!(verified.series_id(), fixture.entries[1].series_id);
    assert_eq!(
        verified.labels(),
        &[(String::from("s4087"), String::from("s4088"))]
    );
    let delta = read_delta(
        class_reads(&fixture.runtime, MetadataCacheClass::SeriesColdPage),
        before,
    );
    assert!(delta.calls >= 2, "both intersected pages must be issued");
}

#[test]
fn cross_page_keyset_corruption_is_sticky_before_any_row_is_returned() {
    let mut fixture = fixture_with_runtime(
        "schema7-cross-page-corruption",
        FixtureOptions {
            cross_page_keyset: true,
            corrupt_second_cold_page: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open corrupt cross-page session");
    let roots = session.load_roots().expect("load corrupt cross-page roots");
    let bound = session.bind(roots).expect("bind corrupt cross-page roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[1])
        .expect("plan corrupt cross-page series");
    let routed = planned.get(0).expect("corrupt cross-page series plan");

    let error = session
        .materialize_verified(&bound, &symbols, routed)
        .expect_err("second intersected page CRC must reject the complete keyset");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = fixture.runtime.snapshot();
    assert_eq!(after.cache.sticky_artifacts, 1);
    session
        .materialize_verified(&bound, &symbols, routed)
        .expect_err("sticky cross-page corruption must gate retry");
    assert_eq!(fixture.runtime.snapshot().reads, after.reads);
}

#[test]
fn selective_materialization_integrity_checks_an_omitted_symbol_page_crc() {
    let mut fixture = fixture_with_runtime(
        "schema7-selective-omitted-symbol-page-corruption",
        FixtureOptions {
            cross_page_keyset: true,
            corrupt_last_symbol_page: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open omitted symbol-page corruption session");
    let roots = session
        .load_roots()
        .expect("load omitted symbol-page corruption roots");
    let bound = session
        .bind(roots)
        .expect("bind omitted symbol-page corruption roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[1])
        .expect("plan row whose omitted labels use the corrupt symbol page");
    let routed = planned.get(0).expect("corrupt symbol-page series plan");

    let error = session
        .materialize_verified_selected(&bound, &symbols, routed, &[], false)
        .expect_err("an omitted label's symbol-page CRC must remain authoritative");
    assert!(
        matches!(
            error,
            Schema7MetadataReaderError::Symbols(GovernedSymbolReaderError::Cache(
                MetadataCacheError::Structural(_)
            ))
        ),
        "unexpected omitted symbol-page corruption error: {error:?}"
    );
    let after = fixture.runtime.snapshot();
    assert_eq!(after.cache.sticky_artifacts, 1);
    session
        .materialize_verified_selected(&bound, &symbols, routed, &[], false)
        .expect_err("sticky omitted symbol-page corruption must gate retry");
    assert_eq!(fixture.runtime.snapshot().reads, after.reads);
}

#[test]
fn materialization_budget_refusal_precedes_cold_io_and_is_retryable() {
    let runtime = runtime_with_budgets(1024 * 1024, 64 * 1024);
    let mut fixture = fixture_with_runtime(
        "schema7-materialize-budget",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader.query_session().expect("open budget session");
    let roots = session.load_roots().expect("load budget roots");
    let bound = session.bind(roots).expect("bind budget roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan budget series");
    let routed = planned.get(0).expect("budget series plan");
    let usage = runtime.snapshot().governor;
    let competing_bytes = usage
        .in_flight_max_bytes
        .checked_sub(usage.in_flight_bytes)
        .and_then(|bytes| bytes.checked_sub(1))
        .expect("leave one in-flight byte available");
    let blocker = runtime
        .governor()
        .reserve_in_flight_for_usage(competing_bytes, MetadataUsageClass::Scratch)
        .expect("reserve competing materialization scratch");
    let before = runtime.snapshot();
    let error = session
        .materialize_verified(&bound, &symbols, routed)
        .expect_err("cold-range scratch must be refused before I/O");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Budget(_))
    ));
    let after = runtime.snapshot();
    assert_eq!(after.reads, before.reads);
    assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
    drop(blocker);

    let verified = session
        .materialize_verified(&bound, &symbols, routed)
        .expect("budget refusal must be retryable");
    assert_eq!(verified.series_id(), fixture.entries[0].series_id);
}

#[test]
fn foreign_symbol_generation_is_rejected_before_cold_io_or_poisoning() {
    let runtime = runtime();
    let mut first = fixture_with_runtime(
        "schema7-materialize-generation-a",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let second = fixture_with_runtime(
        "schema7-materialize-generation-b",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let reader = open_fixture(&mut first);
    let foreign_symbols = open_symbol_session(&second);
    let session = reader.query_session().expect("open generation session");
    let roots = session.load_roots().expect("load generation roots");
    let bound = session.bind(roots).expect("bind generation roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan generation series");
    let before = runtime.snapshot();

    let error = session
        .materialize_verified(
            &bound,
            &foreign_symbols,
            planned.get(0).expect("generation series plan"),
        )
        .expect_err("foreign symbol session must not materialize labels");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Symbols(GovernedSymbolReaderError::ForeignSegmentGeneration)
    ));
    let after = runtime.snapshot();
    assert_eq!(after.reads, before.reads);
    assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
}

#[test]
fn out_of_range_key_symbol_is_sticky_series_corruption_not_symbol_corruption() {
    let mut fixture = fixture_with_runtime(
        "schema7-key-symbol-bound",
        FixtureOptions {
            symbol_count_limit: Some(1),
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader.query_session().expect("open key-bound session");
    let roots = session.load_roots().expect("load key-bound roots");
    let bound = session.bind(roots).expect("bind key-bound roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan key-bound series");
    let routed = planned.get(0).expect("key-bound series plan");

    let error = session
        .materialize_verified(&bound, &symbols, routed)
        .expect_err("out-of-range key symbol must fail as series corruption");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = fixture.runtime.snapshot();
    assert_eq!(after.cache.sticky_artifacts, 1);

    let owner = fixture
        .registered
        .as_ref()
        .expect("fixture owner available");
    let mut byte = [0u8; 1];
    owner
        .reader(SegmentFile::Series)
        .expect("series attribution reader")
        .read_exact_at_for_class(0, &mut byte, MetadataCacheClass::SeriesRoot)
        .expect_err("series artifact must carry the sticky key-symbol error");
    symbols
        .visit_required_resolved(0, |_| Ok(()))
        .expect("symbols artifact must remain healthy");
    let before_retry = fixture.runtime.snapshot();
    session
        .materialize_verified(&bound, &symbols, routed)
        .expect_err("sticky series corruption must gate retry");
    assert_eq!(fixture.runtime.snapshot().reads, before_retry.reads);
}

#[test]
fn zero_retention_materialization_resolves_each_symbol_with_one_page_read() {
    let runtime = runtime_with_budgets(0, 1024 * 1024);
    let mut fixture = fixture_with_runtime(
        "schema7-zero-retention-materialize",
        FixtureOptions::default(),
        runtime.clone(),
    );
    let reader = open_fixture(&mut fixture);
    let symbols = open_symbol_session(&fixture);
    let session = reader
        .query_session()
        .expect("open zero-retention materialization session");
    let roots = session
        .load_roots()
        .expect("load zero-retention materialization roots");
    let bound = session
        .bind(roots)
        .expect("bind zero-retention materialization roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0])
        .expect("plan zero-retention materialization series");
    let routed = planned
        .get(0)
        .expect("zero-retention materialization series plan");
    let before_reads = class_reads(&runtime, MetadataCacheClass::SymbolPage);
    let before_symbols = symbols.logical_stats();

    let verified = session
        .materialize_verified(&bound, &symbols, routed)
        .expect("materialize with zero retention");
    assert_eq!(verified.series_id(), fixture.entries[0].series_id);
    let symbol_reads = read_delta(
        class_reads(&runtime, MetadataCacheClass::SymbolPage),
        before_reads,
    );
    assert_eq!(symbol_reads.calls, 2);
    assert!(symbol_reads.bytes > 0);
    let after_symbols = symbols.logical_stats();
    assert_eq!(
        after_symbols.returned_values - before_symbols.returned_values,
        2
    );
    assert_eq!(
        after_symbols.returned_utf8_bytes - before_symbols.returned_utf8_bytes,
        6
    );
}

#[test]
fn duplicate_hot_blob_range_is_sticky_on_a_cache_hit_without_more_io() {
    let mut fixture = fixture_with_runtime(
        "schema7-duplicate-blob-range",
        FixtureOptions {
            duplicate_blob_locator: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let session = reader
        .query_session()
        .expect("open duplicate-range session");
    let roots = session.load_roots().expect("load duplicate-range roots");
    let bound = session.bind(roots).expect("bind duplicate-range roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[0, 1])
        .expect("plan both aliased hot records");
    let matching = planned.get(1).expect("blob identity matching series");
    session
        .load_overflow_blob(&bound, matching)
        .expect("admit intrinsically valid physical blob");

    let before_alias = fixture.runtime.snapshot();
    let alias = planned.get(0).expect("aliased hot record");
    let error = session
        .load_overflow_blob(&bound, alias)
        .expect_err("incompatible hot-record alias must fail on cache hit");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_alias = fixture.runtime.snapshot();
    assert_eq!(after_alias.reads, before_alias.reads);
    assert_eq!(after_alias.cache.hits - before_alias.cache.hits, 1);
    assert_eq!(after_alias.cache.sticky_artifacts, 2);

    let before_retry = fixture.runtime.snapshot();
    session
        .load_overflow_blob(&bound, alias)
        .expect_err("cross-artifact corruption must gate retry");
    let after_retry = fixture.runtime.snapshot();
    assert_eq!(after_retry.reads, before_retry.reads);
    assert_eq!(after_retry.cache.sticky_artifacts, 2);
}

#[test]
fn cold_page_corruption_is_sticky_without_admission_or_resource_leaks() {
    let mut fixture = fixture_with_runtime(
        "schema7-bad-cold-page",
        FixtureOptions {
            corrupt_cold: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let reader = open_fixture(&mut fixture);
    let session = reader.query_session().expect("open cold-page session");
    let roots = session.load_roots().expect("load cold-page roots");
    let bound = session.bind(roots).expect("bind cold-page roots");
    let before = fixture.runtime.snapshot();

    let error = session
        .load_cold_page(&bound, 0)
        .expect_err("cold-page CRC corruption must fail");
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = fixture.runtime.snapshot();
    assert_eq!(
        after.reads.delta_since(before.reads).classes
            [MetadataCacheClass::SeriesColdPage.stable_index()]
        .issued,
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 1,
            bytes: fixture.cold_bytes.len() as u64,
        }
    );
    assert_eq!(after.cache.successful_loads, before.cache.successful_loads);
    assert_eq!(after.cache.active_loads, 0);
    assert_eq!(after.cache.sticky_artifacts, 1);
    assert_eq!(
        after
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        before
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes
    );
    assert_eq!(after.files.open_files, 0);

    let before_retry = fixture.runtime.snapshot();
    session
        .load_cold_page(&bound, 0)
        .expect_err("sticky cold-page corruption gates retry");
    assert_eq!(fixture.runtime.snapshot().reads, before_retry.reads);
}

#[test]
fn bootstrap_decode_corruption_is_sticky_before_root_admission() {
    let fixture = fixture("schema7-bad-header", true, false, false);
    let owner = fixture.registered.as_ref().expect("fixture owner");
    let error = match Schema7MetadataReader::open(owner, fixture.context) {
        Ok(_) => panic!("corrupt fixed header must fail open"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = fixture.runtime.snapshot();
    assert_eq!(after.reads.issued.calls, 1);
    assert_eq!(after.reads.issued.bytes, SERIES_HEADER_LEN_V3 as u64);
    assert_eq!(after.cache.successful_loads, 0);
    assert_eq!(after.cache.resident_entries, 0);
    assert_eq!(after.cache.sticky_artifacts, 1);

    let series = owner.reader(SegmentFile::Series).expect("series reader");
    let before_retry = fixture.runtime.snapshot();
    let mut byte = [0u8; 1];
    series
        .read_exact_at_for_class(0, &mut byte, MetadataCacheClass::SeriesRoot)
        .expect_err("sticky header corruption gates retry");
    assert_eq!(
        fixture
            .runtime
            .snapshot()
            .reads
            .delta_since(before_retry.reads)
            .issued
            .calls,
        0
    );
}

#[test]
fn root_suffix_and_overflow_root_corruption_are_sticky_with_exact_reads() {
    let series_fixture = fixture_with_runtime(
        "schema7-bad-series-root-suffix",
        FixtureOptions {
            corrupt_series_root_suffix: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let series_owner = series_fixture
        .registered
        .as_ref()
        .expect("series-root fixture owner");
    let before_series = series_fixture.runtime.snapshot();
    let error = match Schema7MetadataReader::open(series_owner, series_fixture.context) {
        Ok(_) => panic!("corrupt series-root suffix must fail open"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_series = series_fixture.runtime.snapshot();
    let series_delta = after_series.reads.delta_since(before_series.reads);
    assert_eq!(
        series_delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 2,
            bytes: series_fixture.root_len,
        }
    );
    assert_eq!(after_series.cache.successful_loads, 0);
    assert_eq!(after_series.cache.active_loads, 0);
    assert_eq!(after_series.cache.sticky_artifacts, 1);
    assert_eq!(after_series.files.open_files, 0);
    assert_eq!(
        after_series
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        0
    );
    let mut retry = [0u8; 1];
    series_owner
        .reader(SegmentFile::Series)
        .expect("series-root retry reader")
        .read_exact_at_for_class(0, &mut retry, MetadataCacheClass::SeriesRoot)
        .expect_err("sticky series-root corruption gates retry");
    assert_eq!(series_fixture.runtime.snapshot().reads, after_series.reads);

    let overflow_fixture = fixture_with_runtime(
        "schema7-bad-overflow-root",
        FixtureOptions {
            corrupt_overflow_root: true,
            ..FixtureOptions::default()
        },
        runtime(),
    );
    let overflow_owner = overflow_fixture
        .registered
        .as_ref()
        .expect("overflow-root fixture owner");
    let before_overflow = overflow_fixture.runtime.snapshot();
    let error = match Schema7MetadataReader::open(overflow_owner, overflow_fixture.context) {
        Ok(_) => panic!("corrupt overflow root must fail open"),
        Err(error) => error,
    };
    assert!(matches!(
        error,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after_overflow = overflow_fixture.runtime.snapshot();
    let overflow_delta = after_overflow.reads.delta_since(before_overflow.reads);
    assert_eq!(
        overflow_delta.classes[MetadataCacheClass::SeriesRoot.stable_index()].issued,
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 2,
            bytes: overflow_fixture.root_len,
        }
    );
    assert_eq!(
        overflow_delta.classes[MetadataCacheClass::OverflowRoot.stable_index()].issued,
        crate::storage::metadata_runtime::MetadataIssuedReadCount {
            calls: 1,
            bytes: CHUNK_OVERFLOW_ROOT_V2_LEN as u64,
        }
    );
    assert_eq!(after_overflow.cache.successful_loads, 1);
    assert_eq!(after_overflow.cache.active_loads, 0);
    assert_eq!(after_overflow.cache.sticky_artifacts, 1);
    assert_eq!(after_overflow.files.open_files, 0);
    assert_eq!(
        after_overflow
            .governor
            .usage(MetadataUsageClass::Scratch)
            .in_flight_bytes,
        0
    );
    overflow_owner
        .reader(SegmentFile::ChunkIndex)
        .expect("overflow-root retry reader")
        .read_exact_at_for_class(0, &mut retry, MetadataCacheClass::OverflowRoot)
        .expect_err("sticky overflow-root corruption gates retry");
    assert_eq!(
        overflow_fixture.runtime.snapshot().reads,
        after_overflow.reads
    );
}

#[test]
fn touched_page_and_blob_corruption_are_sticky_and_never_admitted() {
    let mut bad_page = fixture("schema7-bad-page", false, true, false);
    let page_reader = open_fixture(&mut bad_page);
    let session = page_reader.query_session().expect("page query session");
    let roots = session.load_roots().expect("load page roots");
    let bound = session.bind(roots).expect("bind page roots");
    let before = bad_page.runtime.snapshot();
    let first = session
        .load_hot_page(&bound, 0)
        .expect_err("hot-page CRC corruption must fail");
    assert!(matches!(
        first,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = bad_page.runtime.snapshot();
    assert_eq!(after.cache.resident_entries, before.cache.resident_entries);
    assert_eq!(after.cache.successful_loads, before.cache.successful_loads);
    assert_eq!(after.cache.sticky_artifacts, 1);
    let before_retry = bad_page.runtime.snapshot();
    session
        .load_hot_page(&bound, 0)
        .expect_err("sticky hot-page corruption gates retry");
    assert_eq!(
        bad_page
            .runtime
            .snapshot()
            .reads
            .delta_since(before_retry.reads)
            .issued
            .calls,
        0
    );

    let mut bad_blob = fixture("schema7-bad-blob", false, false, true);
    let blob_reader = open_fixture(&mut bad_blob);
    let session = blob_reader.query_session().expect("blob query session");
    let roots = session.load_roots().expect("load blob roots");
    let bound = session.bind(roots).expect("bind blob roots");
    let planned = session
        .plan_hot_page(&bound, 0, &[1])
        .expect("plan overflow series");
    let overflow = planned.get(0).expect("overflow planned series");
    let before = bad_blob.runtime.snapshot();
    let first = session
        .load_overflow_blob(&bound, overflow)
        .expect_err("overflow-blob CRC corruption must fail");
    assert!(matches!(
        first,
        Schema7MetadataReaderError::Cache(MetadataCacheError::Structural(_))
    ));
    let after = bad_blob.runtime.snapshot();
    assert_eq!(after.cache.resident_entries, before.cache.resident_entries);
    assert_eq!(after.cache.successful_loads, before.cache.successful_loads);
    assert_eq!(after.cache.sticky_artifacts, 1);
    let before_retry = bad_blob.runtime.snapshot();
    session
        .load_overflow_blob(&bound, overflow)
        .expect_err("sticky overflow corruption gates retry");
    assert_eq!(
        bad_blob
            .runtime
            .snapshot()
            .reads
            .delta_since(before_retry.reads)
            .issued
            .calls,
        0
    );
}

#[test]
fn session_and_external_pin_retire_without_an_ownership_cycle() {
    let mut fixture = fixture("schema7-lifecycle", false, false, false);
    let owner = fixture.registered.take().expect("fixture owner available");
    let reader = Schema7MetadataReader::open(&owner, fixture.context)
        .expect("open lifecycle schema-7 reader");
    let session = reader.query_session().expect("open lifecycle session");
    let roots = session.load_roots().expect("load lifecycle roots");
    let bound = session.bind(roots).expect("bind lifecycle roots");
    let page = session
        .load_hot_page(&bound, 0)
        .expect("pin lifecycle hot page");

    drop(bound);
    drop(reader);
    drop(owner);
    assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 7);
    drop(session);
    assert_eq!(fixture.runtime.snapshot().cache.registered_artifacts, 1);
    assert_eq!(fixture.runtime.snapshot().files.open_files, 0);
    drop(page);
    let final_snapshot = fixture.runtime.snapshot();
    assert_eq!(final_snapshot.cache.registered_artifacts, 0);
    assert_eq!(final_snapshot.cache.resident_entries, 0);
    assert_eq!(final_snapshot.cache.live_allocations, 0);
    assert_eq!(final_snapshot.files.open_files, 0);
    assert_eq!(final_snapshot.governor.in_flight_bytes, 0);
    assert_eq!(final_snapshot.governor.retained_bytes, 0);
}
