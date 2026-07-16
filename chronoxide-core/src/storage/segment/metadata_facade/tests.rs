use std::fs;
use std::io::Cursor;

use crc32c::crc32c;
use tempfile::TempDir;

use crate::labels::METRIC_NAME_LABEL;
use crate::storage::chunk::{
    ChunkEncoding, ChunkIndexEntry, ChunkKind, IndexedChunkAuthentication, chunk_index_ranges,
    write_chunk_index,
};
use crate::storage::index::{
    ExactPostingsIndex, LabelValueFstIndex, LabelValueTimeRangeIndex, MetricSeriesRangeIndex,
    SegmentIndexes, SegmentRoutingIndex, corrupt_v8_exact_postings_payload_for_test,
    write_segment_indexes_for_roots, write_segment_indexes_v8_for_roots_for_test,
    write_segment_indexes_v8_unbound_for_test,
};
use crate::storage::metadata_governor::{MetadataGovernorConfig, MetadataUsageClass};
use crate::storage::metadata_runtime::{
    RegisteredSegment, SegmentArtifactRegistration, StoreMetadataRuntime,
};
use crate::storage::segment::{
    CompiledLabelMatcher, SEGMENT_FOOTER_TRACKED_FILES, SegmentFile, compile_promql_regex,
};
use crate::storage::series::v3::{
    Schema7SeriesAssemblyInput, write_schema7_series_and_chunk_index,
};
use crate::storage::series::{
    SERIES_KIND_FLOAT, SERIES_KIND_HISTOGRAM, SegmentSymbols, SeriesEntry, write_series_bin,
    write_symbols_bin,
};
use crate::util::XxHash64;

use super::*;

const SEGMENT_START_MS: u64 = 0;
const SEGMENT_END_MS: u64 = 200;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixtureLayout {
    Schema6,
    Schema7,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixtureIndexMutation {
    None,
    CorruptAlphaPostings,
    UnpairedLabelFst,
    MismatchedPairedLabelRanges,
    SecondSeriesIdentityMismatch,
    SecondSeriesHistogramKind,
    SecondSeriesHistogramIdentityMismatch,
    SecondSeriesMixedFloatHistogramKind,
}

#[derive(Debug, Clone, Copy)]
struct FixtureSymbols {
    metric_name: u32,
    alpha: u32,
    beta: u32,
    label: u32,
    metric: u32,
}

struct Fixture {
    _directory: TempDir,
    runtime: StoreMetadataRuntime,
    registered: RegisteredSegment,
    layout: SegmentMetadataLayout,
    symbols: FixtureSymbols,
}

fn runtime() -> StoreMetadataRuntime {
    StoreMetadataRuntime::new(MetadataGovernorConfig {
        retained_max_bytes: 8 * 1024 * 1024,
        in_flight_max_bytes: 8 * 1024 * 1024,
        max_open_files: 8,
        max_cached_open_files: 0,
    })
    .expect("valid metadata-facade runtime")
}

fn fixture(identity: &str, layout: FixtureLayout, mutation: FixtureIndexMutation) -> Fixture {
    let directory = TempDir::new().expect("create metadata-facade fixture");
    let (symbols, ids) = fixture_symbols();
    let second_series_is_histogram = matches!(
        mutation,
        FixtureIndexMutation::SecondSeriesHistogramKind
            | FixtureIndexMutation::SecondSeriesHistogramIdentityMismatch
            | FixtureIndexMutation::SecondSeriesMixedFloatHistogramKind
    );
    let second_series_is_mixed =
        mutation == FixtureIndexMutation::SecondSeriesMixedFloatHistogramKind;
    let (mut series, chunk_entries, chunks) = fixture_series(
        &symbols,
        ids,
        second_series_is_histogram,
        second_series_is_mixed,
    );

    let chunk_ranges = chunk_index_ranges(&chunk_entries).expect("derive schema-6 chunk ranges");
    for (entry, range) in series.iter_mut().zip(chunk_ranges) {
        entry.chunk_index = range;
    }

    let indexes = match mutation {
        FixtureIndexMutation::UnpairedLabelFst => {
            fixture_indexes_with_unpaired_label_fst(&symbols, &series, &chunk_entries, ids)
        }
        FixtureIndexMutation::MismatchedPairedLabelRanges => {
            fixture_indexes_with_mismatched_paired_ranges(&symbols, &series, &chunk_entries, ids)
        }
        FixtureIndexMutation::None
        | FixtureIndexMutation::CorruptAlphaPostings
        | FixtureIndexMutation::SecondSeriesIdentityMismatch
        | FixtureIndexMutation::SecondSeriesHistogramKind
        | FixtureIndexMutation::SecondSeriesHistogramIdentityMismatch
        | FixtureIndexMutation::SecondSeriesMixedFloatHistogramKind => {
            fixture_indexes(&symbols, &series, &chunk_entries)
        }
    };
    let mut symbol_bytes = Vec::new();
    write_symbols_bin(&mut symbol_bytes, &symbols).expect("encode fixture symbols");

    let mut series_bytes = Vec::new();
    let mut chunk_index_bytes = Vec::new();
    let mut index_bytes = Vec::new();
    let metadata_layout = match layout {
        FixtureLayout::Schema6 => {
            write_series_bin(&mut series_bytes, &series).expect("encode schema-6 series");
            write_chunk_index(&mut chunk_index_bytes, &chunk_entries)
                .expect("encode schema-6 chunk index");
            write_segment_indexes_for_roots(
                &mut index_bytes,
                &indexes,
                series.len() as u32,
                &symbols,
            )
            .expect("encode schema-6 v7 indexes");
            assert_eq!(mutation, FixtureIndexMutation::None);
            SegmentMetadataLayout::Schema6 {
                series_count: series.len() as u32,
            }
        }
        FixtureLayout::Schema7 => {
            if matches!(
                mutation,
                FixtureIndexMutation::SecondSeriesIdentityMismatch
                    | FixtureIndexMutation::SecondSeriesHistogramIdentityMismatch
            ) {
                series[1].series_id ^= 1;
            }
            let chunks_source = Cursor::new(chunks.clone());
            let ooo_source = Cursor::new(Vec::<u8>::new());
            let mut series_output = Cursor::new(Vec::new());
            let mut chunk_index_output = Cursor::new(Vec::new());
            let result = write_schema7_series_and_chunk_index(
                &mut series_output,
                &mut chunk_index_output,
                Schema7SeriesAssemblyInput {
                    series_entries: &series,
                    chunk_entries: &chunk_entries,
                    segment_start_ms: SEGMENT_START_MS,
                    segment_end_ms: SEGMENT_END_MS,
                    chunk_file_lens: [chunks.len() as u64, 0],
                    chunk_sources: [&chunks_source, &ooo_source],
                },
            )
            .expect("encode schema-7 series and chunk-index roots");
            series_bytes = series_output.into_inner();
            chunk_index_bytes = chunk_index_output.into_inner();
            if matches!(
                mutation,
                FixtureIndexMutation::UnpairedLabelFst
                    | FixtureIndexMutation::MismatchedPairedLabelRanges
            ) {
                write_segment_indexes_v8_unbound_for_test(
                    Cursor::new(&mut index_bytes),
                    &indexes,
                    series.len() as u32,
                    symbols.len() as u32,
                )
                .expect("encode deliberately unpaired schema-7 v8 fixture");
            } else {
                write_segment_indexes_v8_for_roots_for_test(
                    Cursor::new(&mut index_bytes),
                    &indexes,
                    series.len() as u32,
                    &symbols,
                    &series,
                )
                .expect("encode schema-7 v8 indexes");
            }
            if mutation == FixtureIndexMutation::CorruptAlphaPostings {
                corrupt_v8_exact_postings_payload_for_test(&mut index_bytes, ids.label, ids.alpha)
                    .expect("corrupt selected v8 exact-postings payload");
            }
            SegmentMetadataLayout::Schema7(Schema7MetadataOpenContext {
                series_file_len: result.stats.series_file_len,
                chunk_index_file_len: result.stats.chunk_index_file_len,
                segment_start_ms: SEGMENT_START_MS,
                segment_end_ms: SEGMENT_END_MS,
                series_count: result.stats.series_count,
            })
        }
    };

    let runtime = runtime();
    let artifacts = SEGMENT_FOOTER_TRACKED_FILES.map(|file| {
        let contents: &[u8] = match file {
            SegmentFile::MetaJson => b"{}",
            SegmentFile::Symbols => &symbol_bytes,
            SegmentFile::Series => &series_bytes,
            SegmentFile::Chunks => &chunks,
            SegmentFile::OooChunks => &[],
            SegmentFile::ChunkIndex => &chunk_index_bytes,
            SegmentFile::Indexes => &index_bytes,
            SegmentFile::Footer => unreachable!("footer is not runtime-inventoried"),
        };
        let path = directory.path().join(file.filename());
        fs::write(&path, contents).expect("write metadata-facade fixture artifact");
        SegmentArtifactRegistration::new(file, path, contents.len() as u64)
    });
    let registered = runtime
        .register_segment(identity, &artifacts)
        .expect("register metadata-facade fixture");
    Fixture {
        _directory: directory,
        runtime,
        registered,
        layout: metadata_layout,
        symbols: ids,
    }
}

fn fixture_symbols() -> (SegmentSymbols, FixtureSymbols) {
    let mut unsorted = SegmentSymbols::default();
    for value in [METRIC_NAME_LABEL, "alpha", "beta", "label", "metric"] {
        unsorted.intern(value);
    }
    let (symbols, _) = unsorted.sorted_remap().expect("sort fixture symbols");
    let ids = FixtureSymbols {
        metric_name: symbols
            .lookup(METRIC_NAME_LABEL)
            .expect("metric name symbol"),
        alpha: symbols.lookup("alpha").expect("alpha symbol"),
        beta: symbols.lookup("beta").expect("beta symbol"),
        label: symbols.lookup("label").expect("label symbol"),
        metric: symbols.lookup("metric").expect("metric symbol"),
    };
    (symbols, ids)
}

fn fixture_series(
    symbols: &SegmentSymbols,
    ids: FixtureSymbols,
    second_series_is_histogram: bool,
    second_series_is_mixed: bool,
) -> (Vec<SeriesEntry>, Vec<Vec<ChunkIndexEntry>>, Vec<u8>) {
    let mut chunks = Vec::new();
    let values_and_ranges = [
        (ids.alpha, (10, 19)),
        (ids.beta, (100, 109)),
        (ids.alpha, (20, 29)),
    ];
    let mut series = Vec::new();
    let mut chunk_entries = Vec::new();
    for (series_ref, (label_value, (min_time_ms, max_time_ms))) in
        values_and_ranges.into_iter().enumerate()
    {
        let mut labels = vec![(ids.metric_name, ids.metric), (ids.label, label_value)];
        labels.sort_unstable_by_key(|(name, _)| *name);
        let histogram = second_series_is_histogram && series_ref == 1;
        series.push(SeriesEntry {
            series_id: hash_series_id(symbols, &labels),
            kind_mask: if second_series_is_mixed && series_ref == 1 {
                SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM
            } else if histogram {
                SERIES_KIND_HISTOGRAM
            } else {
                SERIES_KIND_FLOAT
            },
            chunk_index: Default::default(),
            labels,
        });
        let entries = if second_series_is_mixed && series_ref == 1 {
            vec![
                append_float_chunk(&mut chunks, series_ref as u32, min_time_ms, max_time_ms),
                append_histogram_chunk(&mut chunks, series_ref as u32, min_time_ms, max_time_ms),
            ]
        } else if histogram {
            vec![append_histogram_chunk(
                &mut chunks,
                series_ref as u32,
                min_time_ms,
                max_time_ms,
            )]
        } else {
            vec![append_float_chunk(
                &mut chunks,
                series_ref as u32,
                min_time_ms,
                max_time_ms,
            )]
        };
        chunk_entries.push(entries);
    }
    (series, chunk_entries, chunks)
}

fn fixture_indexes(
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
    chunks: &[Vec<ChunkIndexEntry>],
) -> SegmentIndexes {
    let mut exact_postings = ExactPostingsIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    for (series_ref, (entry, chunks)) in series.iter().zip(chunks).enumerate() {
        let series_ref = series_ref as u32;
        for &(name, value) in &entry.labels {
            exact_postings.insert_monotonic(name, value, series_ref);
            for chunk in chunks {
                label_value_time_ranges.insert(name, value, chunk.min_time_ms, chunk.max_time_ms);
            }
        }
    }
    let label_values =
        LabelValueFstIndex::from_series(series, symbols).expect("build fixture FSTs");
    let metric_series_ranges =
        MetricSeriesRangeIndex::from_series(series, symbols, &label_value_time_ranges)
            .expect("build fixture metric ranges");
    let routing_index = Some(
        SegmentRoutingIndex::from_indexes(symbols, &exact_postings, &label_value_time_ranges)
            .expect("build fixture routing index"),
    );
    SegmentIndexes {
        exact_postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges,
        routing_index,
    }
}

fn fixture_indexes_with_unpaired_label_fst(
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
    chunks: &[Vec<ChunkIndexEntry>],
    ids: FixtureSymbols,
) -> SegmentIndexes {
    let mut exact_postings = ExactPostingsIndex::default();
    let mut label_value_time_ranges = LabelValueTimeRangeIndex::default();
    for (series_ref, chunks) in chunks.iter().enumerate() {
        let series_ref = series_ref as u32;
        exact_postings.insert_monotonic(ids.metric_name, ids.metric, series_ref);
        for chunk in chunks {
            label_value_time_ranges.insert(
                ids.metric_name,
                ids.metric,
                chunk.min_time_ms,
                chunk.max_time_ms,
            );
        }
    }
    let label_values =
        LabelValueFstIndex::from_series(series, symbols).expect("build unpaired fixture FSTs");
    let metric_series_ranges =
        MetricSeriesRangeIndex::from_series(series, symbols, &label_value_time_ranges)
            .expect("build unpaired fixture metric ranges");
    let routing_index = Some(
        SegmentRoutingIndex::from_indexes(symbols, &exact_postings, &label_value_time_ranges)
            .expect("build unpaired fixture routing index"),
    );
    SegmentIndexes {
        exact_postings,
        label_values,
        label_value_time_ranges,
        metric_series_ranges,
        routing_index,
    }
}

fn fixture_indexes_with_mismatched_paired_ranges(
    symbols: &SegmentSymbols,
    series: &[SeriesEntry],
    chunks: &[Vec<ChunkIndexEntry>],
    ids: FixtureSymbols,
) -> SegmentIndexes {
    let mut indexes = fixture_indexes_with_unpaired_label_fst(symbols, series, chunks, ids);
    indexes
        .label_value_time_ranges
        .insert(ids.label, ids.alpha, 10, 29);
    // Keep the authenticated paired record count equal to the FST count while
    // substituting a valid but non-emitted symbol for beta. Directory summary
    // checks therefore pass and the streaming visitor must detect the missing
    // emitted beta value itself.
    indexes
        .label_value_time_ranges
        .insert(ids.label, ids.metric, 100, 109);
    indexes
}

fn append_float_chunk(
    file: &mut Vec<u8>,
    series_ref: u32,
    min_time_ms: u64,
    max_time_ms: u64,
) -> ChunkIndexEntry {
    const HEADER_LEN: u32 = 40;
    let payload = f64::from(series_ref).to_le_bytes();
    let offset = file.len() as u64;
    let mut chunk = vec![0u8; HEADER_LEN as usize + payload.len()];
    chunk[0] = ChunkKind::Float as u8;
    chunk[1] = ChunkEncoding::RawF64 as u8;
    put_u16(&mut chunk, 2, 0);
    put_u32(&mut chunk, 4, series_ref);
    put_u64(&mut chunk, 8, min_time_ms);
    put_u64(&mut chunk, 16, max_time_ms);
    put_u32(&mut chunk, 24, 1);
    put_u32(&mut chunk, 28, HEADER_LEN);
    put_u32(&mut chunk, 32, payload.len() as u32);
    put_u32(&mut chunk, 36, crc32c(&payload));
    chunk[HEADER_LEN as usize..].copy_from_slice(&payload);
    file.extend_from_slice(&chunk);
    ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms,
        max_time_ms,
        offset,
        length: chunk.len() as u32,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    }
}

fn append_histogram_chunk(
    file: &mut Vec<u8>,
    series_ref: u32,
    min_time_ms: u64,
    max_time_ms: u64,
) -> ChunkIndexEntry {
    const HEADER_LEN: u32 = 40;
    // Metadata routing integrity-checks only the indexed fixed header in this
    // fixture; the payload is never semantically decoded by these tests.
    let payload = [0u8];
    let offset = file.len() as u64;
    let mut chunk = vec![0u8; HEADER_LEN as usize + payload.len()];
    chunk[0] = ChunkKind::Histogram as u8;
    chunk[1] = ChunkEncoding::SchemaVarLen as u8;
    put_u16(&mut chunk, 2, 0);
    put_u32(&mut chunk, 4, series_ref);
    put_u64(&mut chunk, 8, min_time_ms);
    put_u64(&mut chunk, 16, max_time_ms);
    put_u32(&mut chunk, 24, 1);
    put_u32(&mut chunk, 28, HEADER_LEN);
    put_u32(&mut chunk, 32, payload.len() as u32);
    put_u32(&mut chunk, 36, crc32c(&payload));
    chunk[HEADER_LEN as usize..].copy_from_slice(&payload);
    file.extend_from_slice(&chunk);
    ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Histogram,
        flags: 0,
        min_time_ms,
        max_time_ms,
        offset,
        length: chunk.len() as u32,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    }
}

fn hash_series_id(symbols: &SegmentSymbols, labels: &[(u32, u32)]) -> u64 {
    let mut hash = XxHash64::default();
    for &(name, value) in labels {
        hash.update(
            symbols
                .resolve(name)
                .expect("resolve label name")
                .as_bytes(),
        );
        hash.update(&[0]);
        hash.update(
            symbols
                .resolve(value)
                .expect("resolve label value")
                .as_bytes(),
        );
        hash.update(&[0xff]);
    }
    hash.finish()
}

fn put_u16(bytes: &mut [u8], offset: usize, value: u16) {
    bytes[offset..offset + 2].copy_from_slice(&value.to_le_bytes());
}

fn put_u32(bytes: &mut [u8], offset: usize, value: u32) {
    bytes[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
}

fn put_u64(bytes: &mut [u8], offset: usize, value: u64) {
    bytes[offset..offset + 8].copy_from_slice(&value.to_le_bytes());
}

fn open_fixture(
    fixture: &Fixture,
) -> (
    SegmentMetadataReader,
    SegmentMetadataSession,
    SegmentMetadataRoot,
) {
    let reader = SegmentMetadataReader::open(&fixture.registered, fixture.layout)
        .expect("open metadata facade");
    let session = reader.query_session().expect("open facade query session");
    let root = session.bind_roots().expect("bind facade roots");
    (reader, session, root)
}

fn collect_postings(
    session: &SegmentMetadataSession,
    root: &SegmentMetadataRoot,
    postings: &SegmentExactPostings,
) -> Vec<u32> {
    let mut refs = Vec::new();
    assert!(
        session
            .visit_exact_postings_refs(root, postings, |series_ref| {
                refs.push(series_ref);
                true
            })
            .expect("visit facade exact postings")
    );
    refs
}

fn collect_set(
    session: &SegmentMetadataSession,
    root: &SegmentMetadataRoot,
    set: &GovernedSeriesRefSet,
) -> Vec<u32> {
    let mut refs = Vec::new();
    assert!(
        session
            .visit_series_ref_set(root, set, |series_ref| {
                refs.push(series_ref);
                true
            })
            .expect("visit facade ref set")
    );
    refs
}

#[test]
fn schema6_and_schema7_facades_match_except_authenticated_time_pruning() {
    let schema6 = fixture(
        "facade-parity-schema6",
        FixtureLayout::Schema6,
        FixtureIndexMutation::None,
    );
    let schema7 = fixture(
        "facade-parity-schema7",
        FixtureLayout::Schema7,
        FixtureIndexMutation::None,
    );

    for fixture in [&schema6, &schema7] {
        let (reader, session, root) = open_fixture(fixture);
        assert_eq!(
            reader.segment_identity(),
            fixture.registered.segment_identity()
        );
        assert_eq!(root.series_count(), 3);
        assert_eq!(
            session.lookup_symbol(&root, "alpha").expect("lookup alpha"),
            Some(fixture.symbols.alpha)
        );
        let mut resolved = None;
        assert!(
            session
                .visit_resolved_symbol(&root, fixture.symbols.alpha, |symbol_id, value| {
                    resolved = Some((symbol_id, value.to_owned()));
                })
                .expect("resolve alpha")
        );
        assert_eq!(resolved, Some((fixture.symbols.alpha, "alpha".to_owned())));

        let selection = session
            .select_exact_postings(&root, fixture.symbols.label, fixture.symbols.alpha)
            .expect("select alpha postings")
            .expect("alpha postings exist");
        assert_eq!(
            session
                .exact_postings_encoded_len(&root, &selection)
                .unwrap(),
            12
        );
        assert_eq!(
            session
                .exact_postings_cardinality_key(&root, &selection)
                .unwrap(),
            12
        );
        let expected_overlap = matches!(fixture.layout, SegmentMetadataLayout::Schema6 { .. });
        assert_eq!(
            session
                .exact_postings_overlaps(&root, &selection, 105, 106)
                .expect("evaluate exact overlap"),
            expected_overlap
        );
        let postings = session
            .read_exact_postings(&root, &selection)
            .expect("read alpha postings");
        assert_eq!(collect_postings(&session, &root, &postings), [0, 2]);

        let mut names = Vec::new();
        assert!(
            session
                .visit_label_names(&root, |symbol_id, value| {
                    names.push((symbol_id, value.to_owned()));
                    true
                })
                .expect("visit label names")
        );
        assert_eq!(
            names,
            [
                (fixture.symbols.metric_name, METRIC_NAME_LABEL.to_owned()),
                (fixture.symbols.label, "label".to_owned()),
            ]
        );

        let mut values = Vec::new();
        assert!(
            session
                .visit_label_values(
                    &root,
                    fixture.symbols.label,
                    None,
                    None,
                    |symbol_id, value| {
                        values.push((symbol_id, value.to_owned()));
                        true
                    },
                )
                .expect("visit label values")
        );
        assert_eq!(
            values,
            [
                (fixture.symbols.alpha, "alpha".to_owned()),
                (fixture.symbols.beta, "beta".to_owned()),
            ]
        );

        let mut timed_values = Vec::new();
        assert!(
            session
                .visit_label_values(
                    &root,
                    fixture.symbols.label,
                    None,
                    Some((105, 106)),
                    |symbol_id, value| {
                        timed_values.push((symbol_id, value.to_owned()));
                        true
                    },
                )
                .expect("visit time-filtered label values")
        );
        let expected = if matches!(fixture.layout, SegmentMetadataLayout::Schema6 { .. }) {
            vec![
                (fixture.symbols.alpha, "alpha".to_owned()),
                (fixture.symbols.beta, "beta".to_owned()),
            ]
        } else {
            vec![(fixture.symbols.beta, "beta".to_owned())]
        };
        assert_eq!(timed_values, expected);
    }
}

#[test]
fn schema6_and_schema7_route_the_same_verified_series_and_exact_chunks() {
    #[derive(Debug, PartialEq, Eq)]
    struct RoutedChunk {
        series_ref: u32,
        file_id: u8,
        kind: ChunkKind,
        flags: u16,
        min_time_ms: u64,
        max_time_ms: u64,
        file_offset: u64,
        chunk_len: u32,
        scalar_lane_offset: u32,
        scalar_lane_len: u32,
        indexed_prefix_len: usize,
    }

    #[derive(Debug, PartialEq, Eq)]
    struct RoutedSeries {
        series_ref: u32,
        series_id: u64,
        kind_mask: u8,
        labels: Vec<(String, String)>,
        chunks: Vec<RoutedChunk>,
    }

    let schema6 = fixture(
        "facade-routing-schema6",
        FixtureLayout::Schema6,
        FixtureIndexMutation::None,
    );
    let schema7 = fixture(
        "facade-routing-schema7",
        FixtureLayout::Schema7,
        FixtureIndexMutation::None,
    );
    let mut routed_by_layout = Vec::new();

    for fixture in [&schema6, &schema7] {
        let (_reader, session, root) = open_fixture(fixture);
        let candidates = session
            .series_ref_set(&root, &[2, 0, 2])
            .expect("build governed routing candidates");
        let mut routed = Vec::new();
        let mut authentications = Vec::new();
        let outcome = session
            .visit_verified_series(&root, &candidates, |series| {
                assert!(series.labels_complete());
                assert_eq!(series.chunks().len(), 1);
                assert!(!series.chunks().is_empty());
                let mut chunks = Vec::new();
                let chunk_outcome = series
                    .chunks()
                    .visit(|chunk| {
                        authentications.push(chunk.authentication());
                        chunks.push(RoutedChunk {
                            series_ref: chunk.series_ref(),
                            file_id: chunk.file_id(),
                            kind: chunk.kind(),
                            flags: chunk.flags(),
                            min_time_ms: chunk.min_time_ms(),
                            max_time_ms: chunk.max_time_ms(),
                            file_offset: chunk.file_offset(),
                            chunk_len: chunk.chunk_len(),
                            scalar_lane_offset: chunk.scalar_lane_offset(),
                            scalar_lane_len: chunk.scalar_lane_len(),
                            indexed_prefix_len: chunk.indexed_prefix_len(),
                        });
                        Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
                    })
                    .expect("visit exact routed chunks");
                assert_eq!(chunk_outcome, SegmentMetadataVisitOutcome::Complete);
                routed.push(RoutedSeries {
                    series_ref: series.series_ref(),
                    series_id: series.series_id(),
                    kind_mask: series.kind_mask(),
                    labels: series.labels().to_vec(),
                    chunks,
                });
                Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
            })
            .expect("route verified series");

        assert_eq!(outcome, SegmentMetadataVisitOutcome::Complete);
        assert_eq!(
            routed
                .iter()
                .map(|series| series.series_ref)
                .collect::<Vec<_>>(),
            [0, 2]
        );
        assert_eq!(
            routed
                .iter()
                .map(|series| {
                    let chunk = &series.chunks[0];
                    (chunk.series_ref, chunk.min_time_ms, chunk.max_time_ms)
                })
                .collect::<Vec<_>>(),
            [(0, 10, 19), (2, 20, 29)]
        );
        assert!(routed.iter().all(|series| {
            series.kind_mask == SERIES_KIND_FLOAT
                && series.labels
                    == [
                        (METRIC_NAME_LABEL.to_owned(), "metric".to_owned()),
                        ("label".to_owned(), "alpha".to_owned()),
                    ]
                && series.chunks[0].kind == ChunkKind::Float
                && series.chunks[0].indexed_prefix_len == 40
        }));
        if matches!(fixture.layout, SegmentMetadataLayout::Schema6 { .. }) {
            assert!(
                authentications
                    .iter()
                    .all(|authentication| *authentication
                        == SegmentChunkAuthentication::Schema6Legacy)
            );
        } else {
            assert!(authentications.iter().all(|authentication| matches!(
                authentication,
                SegmentChunkAuthentication::Schema7IndexedPrefix { .. }
            )));
        }
        routed_by_layout.push(routed);
    }

    assert_eq!(routed_by_layout[0], routed_by_layout[1]);
}

#[test]
fn selective_routing_is_schema7_only_and_marks_partial_labels() {
    let schema6 = fixture(
        "facade-selective-routing-schema6",
        FixtureLayout::Schema6,
        FixtureIndexMutation::None,
    );
    let schema7 = fixture(
        "facade-selective-routing-schema7",
        FixtureLayout::Schema7,
        FixtureIndexMutation::None,
    );
    let selected_names = vec![String::from("label")];
    let mut identities_by_layout = Vec::new();

    for fixture in [&schema6, &schema7] {
        let (_reader, session, root) = open_fixture(fixture);
        let candidates = session
            .series_ref_set(&root, &[0, 2])
            .expect("build selective routing candidates");
        let mut routed = Vec::new();
        let outcome = session
            .visit_verified_series_selected(
                &root,
                &candidates,
                &selected_names,
                SERIES_KIND_FLOAT,
                true,
                |series| {
                    routed.push((
                        series.series_ref(),
                        series.series_id(),
                        series.metric_name_dropped_series_id(),
                        series.labels_complete(),
                        series.labels().to_vec(),
                    ));
                    Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
                },
            )
            .expect("route selectively materialized series");
        assert_eq!(outcome, SegmentMetadataVisitOutcome::Complete);
        if matches!(fixture.layout, SegmentMetadataLayout::Schema6 { .. }) {
            assert!(routed.iter().all(|(_, _, derived, complete, labels)| {
                *complete
                    && derived.is_none()
                    && labels
                        == &[
                            (METRIC_NAME_LABEL.to_owned(), String::from("metric")),
                            (String::from("label"), String::from("alpha")),
                        ]
            }));
        } else {
            let expected_derived = crate::storage::segment::segment_series_id(&[(
                String::from("label"),
                String::from("alpha"),
            )]);
            assert!(routed.iter().all(|(_, _, derived, complete, labels)| {
                !*complete
                    && *derived == Some(expected_derived)
                    && labels == &[(String::from("label"), String::from("alpha"))]
            }));
        }
        identities_by_layout.push(
            routed
                .iter()
                .map(|(series_ref, series_id, _, _, _)| (*series_ref, *series_id))
                .collect::<Vec<_>>(),
        );
    }

    assert_eq!(identities_by_layout[0], identities_by_layout[1]);
}

#[test]
fn schema7_selective_routing_keeps_typed_rows_complete_in_a_mixed_batch() {
    let fixture = fixture(
        "facade-selective-routing-mixed-kinds",
        FixtureLayout::Schema7,
        FixtureIndexMutation::SecondSeriesHistogramKind,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let candidates = session
        .series_ref_set(&root, &[0, 1])
        .expect("build mixed-kind selective candidates");
    let selected_names = vec![String::from("label")];
    let mut routed = Vec::new();

    session
        .visit_verified_series_selected(
            &root,
            &candidates,
            &selected_names,
            SERIES_KIND_FLOAT,
            true,
            |series| {
                routed.push((
                    series.series_ref(),
                    series.kind_mask(),
                    series.labels_complete(),
                    series.labels().to_vec(),
                ));
                Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
            },
        )
        .expect("route mixed scalar and typed rows");

    assert_eq!(
        routed,
        [
            (
                0,
                SERIES_KIND_FLOAT,
                false,
                vec![(String::from("label"), String::from("alpha"))],
            ),
            (
                1,
                SERIES_KIND_HISTOGRAM,
                true,
                vec![
                    (METRIC_NAME_LABEL.to_owned(), String::from("metric")),
                    (String::from("label"), String::from("beta")),
                ],
            ),
        ]
    );
}

#[test]
fn schema7_selective_routing_owns_only_requested_labels_for_pure_histograms() {
    let fixture = fixture(
        "facade-selective-routing-native-histogram",
        FixtureLayout::Schema7,
        FixtureIndexMutation::SecondSeriesHistogramKind,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let candidates = session
        .series_ref_set(&root, &[1])
        .expect("build native histogram candidate");
    let selected_names = vec![String::from("label")];
    let mut routed = Vec::new();

    session
        .visit_verified_series_selected(
            &root,
            &candidates,
            &selected_names,
            SERIES_KIND_HISTOGRAM,
            true,
            |series| {
                routed.push((
                    series.series_id(),
                    series.metric_name_dropped_series_id(),
                    series.labels_complete(),
                    series.labels().to_vec(),
                ));
                Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
            },
        )
        .expect("route pure native histogram selectively");

    let expected_dropped = crate::storage::segment::segment_series_id(&[(
        String::from("label"),
        String::from("beta"),
    )]);
    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0].1, Some(expected_dropped));
    assert!(!routed[0].2);
    assert_eq!(
        routed[0].3,
        vec![(String::from("label"), String::from("beta"))]
    );
}

#[test]
fn schema7_selective_routing_keeps_mixed_kind_histogram_rows_complete() {
    let fixture = fixture(
        "facade-selective-routing-native-mixed-row",
        FixtureLayout::Schema7,
        FixtureIndexMutation::SecondSeriesMixedFloatHistogramKind,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let candidates = session
        .series_ref_set(&root, &[1])
        .expect("build mixed-kind native histogram candidate");
    let selected_names = vec![String::from("label")];
    let mut routed = Vec::new();

    session
        .visit_verified_series_selected(
            &root,
            &candidates,
            &selected_names,
            SERIES_KIND_HISTOGRAM,
            true,
            |series| {
                routed.push((
                    series.kind_mask(),
                    series.metric_name_dropped_series_id(),
                    series.labels_complete(),
                    series.labels().to_vec(),
                ));
                Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
            },
        )
        .expect("route mixed-kind histogram with complete labels");

    assert_eq!(routed.len(), 1);
    assert_eq!(routed[0].0, SERIES_KIND_FLOAT | SERIES_KIND_HISTOGRAM);
    assert_eq!(routed[0].1, None);
    assert!(routed[0].2);
    assert_eq!(
        routed[0].3,
        vec![
            (METRIC_NAME_LABEL.to_owned(), String::from("metric")),
            (String::from("label"), String::from("beta")),
        ]
    );
}

#[test]
fn native_selective_and_full_routing_report_the_same_identity_corruption() {
    let run = |identity: &str, selective: bool| {
        let fixture = fixture(
            identity,
            FixtureLayout::Schema7,
            FixtureIndexMutation::SecondSeriesHistogramIdentityMismatch,
        );
        let (_reader, session, root) = open_fixture(&fixture);
        let candidates = session
            .series_ref_set(&root, &[1])
            .expect("build corrupt native histogram candidate");
        let result = if selective {
            session.visit_verified_series_selected(
                &root,
                &candidates,
                &[],
                SERIES_KIND_HISTOGRAM,
                true,
                |_| Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue),
            )
        } else {
            session.visit_verified_series(&root, &candidates, |_| {
                Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
            })
        };
        let error = result.expect_err("the touched identity mismatch must remain corruption");
        assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
        error.to_string()
    };

    assert_eq!(
        run("facade-native-full-corruption", false),
        run("facade-native-selective-corruption", true)
    );
}

#[test]
fn schema7_selective_routing_cannot_hide_omitted_identity_corruption() {
    let fixture = fixture(
        "facade-selective-routing-corruption",
        FixtureLayout::Schema7,
        FixtureIndexMutation::SecondSeriesIdentityMismatch,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let candidates = session
        .series_ref_set(&root, &[1])
        .expect("build selectively omitted corrupt candidate");

    session
        .visit_verified_series_selected(&root, &candidates, &[], SERIES_KIND_FLOAT, true, |_| {
            Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
        })
        .expect_err("omitted labels must still reproduce the integrity-checked identity");
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 1);
}

#[test]
fn schema7_stop_does_not_materialize_or_poison_a_later_corrupt_series() {
    let fixture = fixture(
        "facade-schema7-stop-before-later-corruption",
        FixtureLayout::Schema7,
        FixtureIndexMutation::SecondSeriesIdentityMismatch,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let candidates = session
        .series_ref_set(&root, &[0, 1])
        .expect("build stop-precedence candidates");
    let before = fixture.runtime.snapshot();
    let mut visited = Vec::new();
    let outcome = session
        .visit_verified_series(&root, &candidates, |series| {
            visited.push(series.series_ref());
            Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Stop)
        })
        .expect("stopping after the first valid series must not touch the later identity mismatch");
    assert_eq!(outcome, SegmentMetadataVisitOutcome::Stopped);
    assert_eq!(visited, [0]);
    assert_eq!(
        fixture.runtime.snapshot().cache.sticky_artifacts,
        before.cache.sticky_artifacts,
        "the unvisited later series must not poison the series artifact"
    );

    session
        .visit_verified_series(&root, &candidates, |_| {
            Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
        })
        .expect_err("continuing to the malformed later identity must report corruption");
    assert_eq!(
        fixture.runtime.snapshot().cache.sticky_artifacts,
        before.cache.sticky_artifacts + 1
    );
}

#[test]
fn foreign_generation_handles_fail_before_io_or_sticky_corruption() {
    let left = fixture(
        "facade-provenance-left",
        FixtureLayout::Schema7,
        FixtureIndexMutation::None,
    );
    let right = fixture(
        "facade-provenance-right",
        FixtureLayout::Schema7,
        FixtureIndexMutation::None,
    );
    let (_left_reader, left_session, left_root) = open_fixture(&left);
    let (_right_reader, right_session, right_root) = open_fixture(&right);
    let left_selection = left_session
        .select_exact_postings(&left_root, left.symbols.label, left.symbols.alpha)
        .unwrap()
        .unwrap();
    let left_postings = left_session
        .read_exact_postings(&left_root, &left_selection)
        .unwrap();
    let left_set = left_session
        .exact_postings_ref_set(&left_root, &left_postings)
        .unwrap();

    let before = right.runtime.snapshot();
    assert!(matches!(
        right_session.lookup_symbol(&left_root, "alpha"),
        Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
    ));
    assert!(matches!(
        right_session.exact_postings_encoded_len(&right_root, &left_selection),
        Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
    ));
    assert!(matches!(
        right_session.exact_postings_cardinality_key(&right_root, &left_selection),
        Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
    ));
    assert!(matches!(
        right_session.visit_exact_postings_refs(&right_root, &left_postings, |_| true),
        Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
    ));
    assert!(matches!(
        right_session.visit_series_ref_set(&right_root, &left_set, |_| true),
        Err(SegmentMetadataFacadeError::ForeignSegmentGeneration)
    ));
    let after = right.runtime.snapshot();
    assert_eq!(after.reads, before.reads);
    assert_eq!(after.cache.sticky_artifacts, before.cache.sticky_artifacts);
}

#[test]
fn touched_schema7_postings_corruption_is_sticky_and_retry_issues_no_io() {
    let fixture = fixture(
        "facade-corrupt-v8",
        FixtureLayout::Schema7,
        FixtureIndexMutation::CorruptAlphaPostings,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let selection = session
        .select_exact_postings(&root, fixture.symbols.label, fixture.symbols.alpha)
        .expect("select corrupted postings")
        .expect("corrupted postings remain discoverable");

    let before = fixture.runtime.snapshot();
    let error = match session.read_exact_postings(&root, &selection) {
        Err(error) => error,
        Ok(_) => panic!("touched payload corruption must propagate"),
    };
    assert!(error.to_string().contains("CRC mismatch"));
    let after_first = fixture.runtime.snapshot();
    assert_eq!(after_first.cache.sticky_artifacts, 1);
    assert_eq!(after_first.reads.delta_since(before.reads).issued.calls, 1);

    assert!(session.read_exact_postings(&root, &selection).is_err());
    let after_retry = fixture.runtime.snapshot();
    assert_eq!(after_retry.reads, after_first.reads);
    assert_eq!(after_retry.cache.sticky_artifacts, 1);
}

#[test]
fn timed_schema7_fst_without_paired_ranges_is_conservatively_emitted() {
    let fixture = fixture(
        "facade-unpaired-v8-ranges",
        FixtureLayout::Schema7,
        FixtureIndexMutation::UnpairedLabelFst,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let before = fixture.runtime.snapshot();
    let mut visited = Vec::new();
    assert!(
        session
            .visit_label_values(
                &root,
                fixture.symbols.label,
                None,
                Some((105, 106)),
                |symbol_id, value| {
                    visited.push((symbol_id, value.to_owned()));
                    true
                },
            )
            .expect("an FST-only v8 inventory is canonically unconstrained")
    );
    assert_eq!(
        visited,
        [
            (fixture.symbols.alpha, "alpha".to_owned()),
            (fixture.symbols.beta, "beta".to_owned()),
        ]
    );
    let after_first = fixture.runtime.snapshot();
    assert_eq!(after_first.cache.sticky_artifacts, 0);
    assert!(after_first.reads.issued.calls > before.reads.issued.calls);

    let mut repeated = 0usize;
    assert!(
        session
            .visit_label_values(
                &root,
                fixture.symbols.label,
                None,
                Some((105, 106)),
                |_, _| {
                    repeated += 1;
                    true
                },
            )
            .expect("repeat unconstrained traversal remains valid")
    );
    assert_eq!(repeated, 2);
    assert_eq!(fixture.runtime.snapshot().cache.sticky_artifacts, 0);
}

#[test]
fn timed_schema7_paired_ranges_missing_an_emitted_value_are_sticky() {
    let fixture = fixture(
        "facade-mismatched-v8-ranges",
        FixtureLayout::Schema7,
        FixtureIndexMutation::MismatchedPairedLabelRanges,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let before = fixture.runtime.snapshot();
    let mut visited = false;
    let error = session
        .visit_label_values(
            &root,
            fixture.symbols.label,
            None,
            Some((105, 106)),
            |_, _| {
                visited = true;
                true
            },
        )
        .expect_err("missing value in an existing paired range record must be corruption");
    assert!(!visited);
    assert!(
        error
            .to_string()
            .contains("no paired authenticated time range")
    );
    let after_first = fixture.runtime.snapshot();
    assert_eq!(after_first.cache.sticky_artifacts, 1);
    assert!(after_first.reads.issued.calls > before.reads.issued.calls);

    session
        .visit_label_values(
            &root,
            fixture.symbols.label,
            None,
            Some((105, 106)),
            |_, _| true,
        )
        .expect_err("sticky paired-range corruption must gate retry");
    assert_eq!(fixture.runtime.snapshot().reads, after_first.reads);
}

#[test]
fn ref_set_budget_refusal_is_transient_and_does_not_issue_io() {
    let fixture = fixture(
        "facade-ref-budget",
        FixtureLayout::Schema7,
        FixtureIndexMutation::None,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let governor_before = fixture.runtime.snapshot().governor;
    let blocker = fixture
        .runtime
        .governor()
        .reserve_in_flight_for_usage(
            governor_before
                .in_flight_max_bytes
                .checked_sub(governor_before.in_flight_bytes)
                .and_then(|remaining| remaining.checked_sub(1))
                .expect("fixture leaves one reservable byte"),
            MetadataUsageClass::Scratch,
        )
        .expect("reserve all but one in-flight byte");
    let before = fixture.runtime.snapshot();
    assert!(matches!(
        session.series_ref_set(&root, &[0, 1, 2]),
        Err(SegmentMetadataFacadeError::Budget(_))
    ));
    let refused = fixture.runtime.snapshot();
    assert_eq!(refused.reads, before.reads);
    assert_eq!(
        refused.cache.sticky_artifacts,
        before.cache.sticky_artifacts
    );
    assert_eq!(
        refused.governor.in_flight_refusals,
        before.governor.in_flight_refusals + 1
    );

    drop(blocker);
    let set = session
        .series_ref_set(&root, &[2, 0, 2])
        .expect("retry after transient budget release");
    assert_eq!(collect_set(&session, &root, &set), [0, 2]);
}

#[test]
fn governed_ref_sets_are_sorted_unique_charged_and_support_algebra() {
    let fixture = fixture(
        "facade-ref-algebra",
        FixtureLayout::Schema6,
        FixtureIndexMutation::None,
    );
    let (_reader, session, root) = open_fixture(&fixture);
    let selection = session
        .select_exact_postings(&root, fixture.symbols.label, fixture.symbols.alpha)
        .unwrap()
        .unwrap();
    let postings = session.read_exact_postings(&root, &selection).unwrap();
    let baseline = fixture.runtime.snapshot().governor.in_flight_bytes;

    {
        let left = session.series_ref_set(&root, &[2, 0, 2]).unwrap();
        let right = session.series_ref_set(&root, &[1, 2]).unwrap();
        let all = session.all_series_ref_set(&root).unwrap();
        let from_postings = session.exact_postings_ref_set(&root, &postings).unwrap();
        let union = session.union_series_ref_sets(&root, &left, &right).unwrap();
        let intersection = session
            .intersect_series_ref_sets(&root, &left, &right)
            .unwrap();
        let difference = session
            .difference_series_ref_sets(&root, &left, &right)
            .unwrap();

        assert_eq!(left.len(), 2);
        assert!(!left.is_empty());
        for set in [
            &left,
            &right,
            &all,
            &from_postings,
            &union,
            &intersection,
            &difference,
        ] {
            assert_eq!(
                set.charged_bytes(),
                u64::try_from(set.capacity_for_test() * std::mem::size_of::<u32>())
                    .expect("fixture capacity charge fits u64")
            );
        }
        let live_set_charge = [
            &left,
            &right,
            &all,
            &from_postings,
            &union,
            &intersection,
            &difference,
        ]
        .into_iter()
        .map(GovernedSeriesRefSet::charged_bytes)
        .sum::<u64>();
        assert_eq!(
            fixture
                .runtime
                .snapshot()
                .governor
                .in_flight_bytes
                .checked_sub(baseline)
                .expect("live ref-set charge cannot reduce baseline"),
            live_set_charge
        );
        assert_eq!(collect_set(&session, &root, &left), [0, 2]);
        assert_eq!(collect_set(&session, &root, &right), [1, 2]);
        assert_eq!(collect_set(&session, &root, &all), [0, 1, 2]);
        assert_eq!(collect_set(&session, &root, &from_postings), [0, 2]);
        assert_eq!(collect_set(&session, &root, &union), [0, 1, 2]);
        assert_eq!(collect_set(&session, &root, &intersection), [2]);
        assert_eq!(collect_set(&session, &root, &difference), [0]);

        let mut first = None;
        assert!(
            !session
                .visit_series_ref_set(&root, &union, |series_ref| {
                    first = Some(series_ref);
                    false
                })
                .unwrap()
        );
        assert_eq!(first, Some(0));
    }
    assert_eq!(
        fixture.runtime.snapshot().governor.in_flight_bytes,
        baseline
    );
    assert!(matches!(
        session.series_ref_set(&root, &[3]),
        Err(SegmentMetadataFacadeError::InvalidSeriesRef {
            series_ref: 3,
            series_count: 3,
        })
    ));
}

#[test]
fn schema6_and_schema7_matcher_selection_uses_governed_postings_and_fsts() {
    for (identity, layout) in [
        ("facade-selector-schema6", FixtureLayout::Schema6),
        ("facade-selector-schema7", FixtureLayout::Schema7),
    ] {
        let fixture = fixture(identity, layout, FixtureIndexMutation::None);
        let (_reader, session, root) = open_fixture(&fixture);

        for (matchers, expected) in [
            (vec![matcher_eq("label", "alpha")], vec![0, 2]),
            (vec![matcher_regex("label", "a.*")], vec![0, 2]),
            (vec![matcher_not_eq("label", "alpha")], vec![1]),
            (vec![matcher_not_regex("label", "a.*")], vec![1]),
            (
                vec![
                    matcher_eq(METRIC_NAME_LABEL, "metric"),
                    matcher_regex("label", "a.*"),
                ],
                vec![0, 2],
            ),
            (vec![matcher_eq("missing", "value")], vec![]),
            (vec![matcher_regex("missing", ".+")], vec![]),
        ] {
            let candidates = session
                .select_matcher_candidates(&root, &matchers, None)
                .expect("select schema-neutral matcher candidates");
            assert_eq!(collect_set(&session, &root, &candidates), expected);
            assert_eq!(collect_matching_refs(&session, &root, &matchers), expected);
        }
    }
}

#[test]
fn missing_label_matchers_remain_conservative_until_canonical_materialization() {
    for (identity, layout) in [
        ("facade-missing-schema6", FixtureLayout::Schema6),
        ("facade-missing-schema7", FixtureLayout::Schema7),
    ] {
        let fixture = fixture(identity, layout, FixtureIndexMutation::None);
        let (_reader, session, root) = open_fixture(&fixture);

        for (matcher, expected) in [
            (matcher_eq("missing", ""), vec![0, 1, 2]),
            (matcher_regex("missing", ".*"), vec![0, 1, 2]),
            (matcher_not_eq("missing", ""), vec![]),
            (matcher_not_regex("missing", ".*"), vec![]),
        ] {
            let matchers = [matcher];
            let candidates = session
                .select_matcher_candidates(&root, &matchers, None)
                .expect("missing-label matcher candidate selection");
            assert_eq!(collect_set(&session, &root, &candidates), [0, 1, 2]);
            assert_eq!(collect_matching_refs(&session, &root, &matchers), expected);
        }
    }
}

#[test]
fn matcher_selection_defers_labels_and_preserves_locator_authentication() {
    for (identity, layout) in [
        ("facade-deferred-schema6", FixtureLayout::Schema6),
        ("facade-deferred-schema7", FixtureLayout::Schema7),
    ] {
        let fixture = fixture(identity, layout, FixtureIndexMutation::None);
        let (_reader, session, root) = open_fixture(&fixture);
        let matchers = [matcher_eq("label", "alpha")];
        let before = fixture.runtime.snapshot();
        let candidates = session
            .select_matcher_candidates(&root, &matchers, None)
            .expect("select alpha candidates without series materialization");
        assert_eq!(collect_set(&session, &root, &candidates), [0, 2]);
        let after_selection = fixture.runtime.snapshot();
        let before_series_reads = before
            .reads
            .files
            .iter()
            .find(|stats| stats.file == SegmentFile::Series)
            .expect("series file has a read counter")
            .issued;
        let after_series_reads = after_selection
            .reads
            .files
            .iter()
            .find(|stats| stats.file == SegmentFile::Series)
            .expect("series file has a read counter")
            .issued;
        assert_eq!(
            after_series_reads, before_series_reads,
            "postings/FST selection must not materialize canonical series labels"
        );

        let mut refs = Vec::new();
        let mut authentications = Vec::new();
        let mut owned_locators = Vec::new();
        let outcome = session
            .visit_matching_verified_series(&root, &matchers, None, |series| {
                refs.push(series.series_ref());
                series.chunks().visit(|locator| {
                    authentications.push(locator.authentication());
                    owned_locators.push(locator.to_owned_indexed_locator());
                    Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
                })?;
                Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
            })
            .expect("visit canonically verified selector results");
        assert_eq!(outcome, SegmentMetadataVisitOutcome::Complete);
        assert_eq!(refs, [0, 2]);
        assert_eq!(authentications.len(), 2);
        assert_eq!(owned_locators.len(), 2);
        assert_eq!(
            owned_locators
                .iter()
                .map(|locator| locator.series_ref())
                .collect::<Vec<_>>(),
            [0, 2]
        );
        match layout {
            FixtureLayout::Schema6 => {
                assert!(
                    authentications
                        .iter()
                        .all(|value| *value == SegmentChunkAuthentication::Schema6Legacy)
                );
                assert!(owned_locators.iter().all(|locator| matches!(
                    locator.authentication(),
                    IndexedChunkAuthentication::Schema6V1Legacy
                )));
            }
            FixtureLayout::Schema7 => {
                assert!(authentications.iter().all(|value| matches!(
                    value,
                    SegmentChunkAuthentication::Schema7IndexedPrefix { .. }
                )));
                assert!(owned_locators.iter().all(|locator| matches!(
                    locator.authentication(),
                    IndexedChunkAuthentication::Schema7 { .. }
                )));
            }
        }
    }
}

fn matcher_eq(name: &str, value: &str) -> CompiledLabelMatcher {
    CompiledLabelMatcher::Eq {
        name: name.to_owned(),
        value: value.to_owned(),
    }
}

fn matcher_not_eq(name: &str, value: &str) -> CompiledLabelMatcher {
    CompiledLabelMatcher::NotEq {
        name: name.to_owned(),
        value: value.to_owned(),
    }
}

fn matcher_regex(name: &str, pattern: &str) -> CompiledLabelMatcher {
    CompiledLabelMatcher::Regex {
        name: name.to_owned(),
        pattern: compile_promql_regex(pattern).expect("compile selector regex"),
    }
}

fn matcher_not_regex(name: &str, pattern: &str) -> CompiledLabelMatcher {
    CompiledLabelMatcher::NotRegex {
        name: name.to_owned(),
        pattern: compile_promql_regex(pattern).expect("compile negative selector regex"),
    }
}

fn collect_matching_refs(
    session: &SegmentMetadataSession,
    root: &SegmentMetadataRoot,
    matchers: &[CompiledLabelMatcher],
) -> Vec<u32> {
    let mut refs = Vec::new();
    let outcome = session
        .visit_matching_verified_series(root, matchers, None, |series| {
            refs.push(series.series_ref());
            Ok::<_, std::convert::Infallible>(SegmentMetadataVisitControl::Continue)
        })
        .expect("visit matcher results");
    assert_eq!(outcome, SegmentMetadataVisitOutcome::Complete);
    refs
}
