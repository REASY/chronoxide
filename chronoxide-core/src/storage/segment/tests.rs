use super::*;
use crate::storage::chunk::{ChunkEncoding, ChunkKind, ChunkReader, ChunkSamples};
use crate::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue, SummaryQuantileValue,
    SummaryValue, TypedSampleMetadata,
};
use crate::storage::index::LabelValueTimeRange;
use crate::storage::series::{
    SERIES_KIND_EXPONENTIAL_HISTOGRAM, SERIES_KIND_HISTOGRAM, SERIES_KIND_SUMMARY,
};
use std::io::{Cursor, ErrorKind, Read, Seek, SeekFrom};

const FRAME_HEADER_LEN: u64 = 14;

#[test]
fn query_budget_counts_unique_matched_series_once() {
    let mut budget = QueryBudget::new(QueryLimits {
        max_matched_series: Some(1),
        ..QueryLimits::unlimited()
    });

    budget.observe_matched_series(10).unwrap();
    budget.observe_matched_series(10).unwrap();
    assert_eq!(budget.stats().matched_series, 1);

    let err = budget.observe_matched_series(11).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::QuotaExceeded);
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::MatchedSeries);
    assert_eq!(limit.max, 1);
}

#[test]
fn query_budget_counts_unique_projected_series_once() {
    let mut budget = QueryBudget::new(QueryLimits {
        max_projected_series: Some(1),
        ..QueryLimits::unlimited()
    });

    budget.observe_projected_series(10).unwrap();
    budget.observe_projected_series(10).unwrap();
    assert_eq!(budget.stats().projected_series, 1);

    let err = budget.observe_projected_series(11).unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::QuotaExceeded);
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::ProjectedSeries);
    assert_eq!(limit.max, 1);
}

#[test]
fn query_limits_production_default_matches_storage_spec_guardrails() {
    let limits = QueryLimits::production_default();

    assert_eq!(limits.max_matched_series, Some(1_000_000));
    assert_eq!(limits.max_projected_series, Some(2_000_000));
    assert_eq!(limits.max_chunk_reads, Some(5_000_000));
    assert_eq!(limits.max_bytes_read, Some(2 * 1024 * 1024 * 1024));
    assert_eq!(limits.max_samples_decoded, Some(50_000_000));
    assert_eq!(limits.max_regex_values_examined, Some(100_000));
}

#[test]
fn query_budget_rejects_chunk_byte_sample_and_regex_limits() {
    let mut budget = QueryBudget::new(QueryLimits {
        max_chunk_reads: Some(0),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_chunk_read(1).unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::ChunkReads);
    assert_eq!(limit.max, 0);

    let mut budget = QueryBudget::new(QueryLimits {
        max_bytes_read: Some(4),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_chunk_read(5).unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::BytesRead);
    assert_eq!(limit.max, 4);

    let mut budget = QueryBudget::new(QueryLimits {
        max_samples_decoded: Some(1),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_samples_decoded(2).unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::SamplesDecoded);
    assert_eq!(limit.max, 1);

    let mut budget = QueryBudget::new(QueryLimits {
        max_regex_values_examined: Some(0),
        ..QueryLimits::unlimited()
    });
    let err = budget.observe_regex_value().unwrap_err();
    let limit = query_limit_exceeded_from_io(&err).unwrap();
    assert_eq!(limit.limit, QueryLimit::RegexValuesExamined);
    assert_eq!(limit.max, 0);
}

#[test]
fn query_profile_reports_chunk_payload_locality() {
    let mut profile = SegmentStoreQueryProfile::default();

    profile.observe_chunk_payload_read(100, 10);
    profile.observe_chunk_payload_read(110, 5);
    profile.observe_chunk_payload_read(200, 10);
    profile.observe_chunk_payload_read(260, 5);
    profile.observe_chunk_payload_read(240, 5);

    let locality = profile.chunk_payload_locality;
    assert_eq!(locality.reads, 5);
    assert_eq!(locality.backward_jumps, 1);
    assert_eq!(locality.forward_gaps, 2);
    assert_eq!(locality.forward_gap_bytes, 135);
    assert_eq!(locality.contiguous_runs, 4);
    assert_eq!(locality.contiguous_span_bytes, 35);
    assert_eq!(locality.coalesced_4k_runs, 2);
    assert_eq!(locality.coalesced_4k_span_bytes, 170);
    assert_eq!(locality.coalesced_64k_runs, 2);
    assert_eq!(locality.coalesced_64k_span_bytes, 170);
}

#[test]
fn query_profile_reports_sorted_chunk_payload_coalescing_potential() {
    let mut profile = SegmentStoreQueryProfile::default();
    let mut ranges = [(200, 10), (100, 10), (110, 5), (260, 5), (240, 5)];

    profile.observe_sorted_chunk_payload_ranges(&mut ranges);

    let locality = profile.chunk_payload_locality;
    assert_eq!(locality.reads, 0);
    assert_eq!(locality.sorted_contiguous_runs, 4);
    assert_eq!(locality.sorted_contiguous_span_bytes, 35);
    assert_eq!(locality.sorted_coalesced_4k_runs, 1);
    assert_eq!(locality.sorted_coalesced_4k_span_bytes, 165);
    assert_eq!(locality.sorted_coalesced_64k_runs, 1);
    assert_eq!(locality.sorted_coalesced_64k_span_bytes, 165);
}

#[test]
fn regex_literal_prefix_extracts_only_safe_prefixes() {
    assert_eq!(
        regex_literal_prefix("go_gc_duration_seconds.*"),
        Some("go_gc_duration_seconds".to_string())
    );
    assert_eq!(
        regex_literal_prefix("^rpc_duration.*_count"),
        Some("rpc_duration".to_string())
    );
    assert_eq!(
        regex_literal_prefix(r"http\.request\..*"),
        Some("http.request.".to_string())
    );
    assert_eq!(regex_literal_prefix(".*_count"), None);
    assert_eq!(regex_literal_prefix("[a-z].*"), None);
    assert_eq!(regex_literal_prefix(r"\d+"), None);
    assert_eq!(
        regex_literal_prefixes("rpc_duration_count", true),
        vec!["rpc_duration".to_string(), "rpc_duration_count".to_string()]
    );
    assert_eq!(
        regex_literal_prefixes("rpc_duration_count", false),
        vec!["rpc_duration_count".to_string()]
    );
}

#[test]
fn metadata_accumulator_sorts_dedupes_and_tracks_metric_names() {
    let mut metadata = MetadataAccumulator::default();
    metadata.add_labelset(&[
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod_name".to_string(), "backend-2".to_string()),
    ]);
    metadata.add_labelset(&[
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod_name".to_string(), "backend-1".to_string()),
        ("namespace".to_string(), "default".to_string()),
    ]);

    assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
    assert_eq!(
        metadata.label_names(),
        vec![
            METRIC_NAME_LABEL.to_string(),
            "namespace".to_string(),
            "pod_name".to_string()
        ]
    );
    assert_eq!(
        metadata.label_values("pod_name"),
        vec!["backend-1".to_string(), "backend-2".to_string()]
    );
}

#[test]
fn metric_name_index_collection_reads_only_metric_name_values() {
    let mut symbols = SegmentSymbols::default();
    let metric = symbols.intern(METRIC_NAME_LABEL);
    let cpu = symbols.intern("cpu_usage");
    let pod = symbols.intern("pod_name");
    let backend = symbols.intern("backend-1");
    let series = vec![SeriesEntry {
        series_id: 1,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(metric, cpu), (pod, backend)],
    }];
    let mut label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    label_values.insert_fst(pod, b"not an fst".to_vec());
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values,
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };
    let mut index_reader = index_reader_for(&indexes);
    let mut metadata = MetadataAccumulator::default();

    collect_metric_names_from_index(&symbols, &mut index_reader, 0, 10_000, &mut metadata).unwrap();

    assert_eq!(metadata.metric_names(), vec!["cpu_usage".to_string()]);
}

#[test]
fn label_value_index_collection_reads_only_requested_label_values() {
    let mut symbols = SegmentSymbols::default();
    let metric = symbols.intern(METRIC_NAME_LABEL);
    let cpu = symbols.intern("cpu_usage");
    let pod = symbols.intern("pod_name");
    let backend = symbols.intern("backend-1");
    let series = vec![SeriesEntry {
        series_id: 1,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(metric, cpu), (pod, backend)],
    }];
    let mut label_values = LabelValueFstIndex::from_series(&series, &symbols).unwrap();
    label_values.insert_fst(metric, b"not an fst".to_vec());
    let indexes = SegmentIndexes {
        exact_postings: ExactPostingsIndex::default(),
        label_values,
        label_value_time_ranges: LabelValueTimeRangeIndex::default(),
        metric_series_ranges: MetricSeriesRangeIndex::default(),
        routing_index: None,
    };
    let mut index_reader = index_reader_for(&indexes);
    let mut metadata = MetadataAccumulator::default();

    collect_label_values_from_index(
        &symbols,
        &mut index_reader,
        "pod_name",
        0,
        10_000,
        &mut metadata,
    )
    .unwrap();

    assert_eq!(
        metadata.label_values("pod_name"),
        vec!["backend-1".to_string()]
    );
}

fn index_reader_for(indexes: &SegmentIndexes) -> SegmentIndexReader<Cursor<Vec<u8>>> {
    let mut bytes = Vec::new();
    write_segment_indexes(&mut bytes, indexes).unwrap();
    SegmentIndexReader::open(Cursor::new(bytes)).unwrap()
}

fn read_chunk_encoding(file: &mut File) -> u8 {
    file.seek(SeekFrom::Start(FRAME_HEADER_LEN + 1))
        .expect("seek to encoding");
    let mut buf = [0u8; 1];
    file.read_exact(&mut buf).expect("read encoding");
    buf[0]
}

fn resolved_entry_labels(symbols: &SegmentSymbols, entry: &SeriesEntry) -> Vec<(String, String)> {
    entry
        .labels
        .iter()
        .map(|(key, value)| {
            (
                symbols.resolve(*key).unwrap().to_string(),
                symbols.resolve(*value).unwrap().to_string(),
            )
        })
        .collect()
}

#[test]
fn segment_id_dir_name_roundtrip() {
    let ulid = Ulid::new();
    let id = SegmentId::with_ulid(10, 20, ulid).unwrap();
    let parsed = SegmentId::parse_dir_name(&id.dir_name()).unwrap();
    assert_eq!(parsed.start_ms(), 10);
    assert_eq!(parsed.end_ms(), 20);
    assert_eq!(parsed.ulid(), ulid);
}

#[test]
fn segment_id_rejects_invalid_range() {
    let err = SegmentId::with_ulid(10, 10, Ulid::new()).unwrap_err();
    assert!(matches!(
        err,
        SegmentIdError::InvalidRange {
            start_ms: 10,
            end_ms: 10
        }
    ));
}

#[test]
fn segment_id_rejects_invalid_dir_name() {
    let err = SegmentId::parse_dir_name("seg-10-20").unwrap_err();
    assert!(matches!(err, SegmentIdError::InvalidFormat(_)));
}

#[test]
fn segment_file_names_are_stable() {
    assert_eq!(SegmentFile::MetaJson.filename(), "meta.json");
    assert_eq!(SegmentFile::Symbols.filename(), "symbols.bin");
    assert_eq!(SegmentFile::Series.filename(), "series.bin");
    assert_eq!(SegmentFile::Chunks.filename(), "chunks.bin");
    assert_eq!(SegmentFile::OooChunks.filename(), "ooo_chunks.bin");
    assert_eq!(SegmentFile::ChunkIndex.filename(), "chunk_index.bin");
    assert_eq!(SegmentFile::Indexes.filename(), "indexes.puffin");
    assert_eq!(SegmentFile::Footer.filename(), "footer.bin");
}

#[test]
fn segment_paths_are_consistent() {
    let id = SegmentId::with_ulid(1, 2, Ulid::new()).unwrap();
    let paths = SegmentPaths::new("/tmp/segments", id);
    let dir = paths.dir();
    assert!(dir.ends_with(id.dir_name()));
    let tmp = paths.temp_dir();
    assert!(tmp.ends_with(format!(".tmp/{}", id.dir_name())));
    let chunk_path = paths.file_path(SegmentFile::Chunks);
    assert!(chunk_path.ends_with("chunks.bin"));
}

#[test]
fn segment_footer_roundtrips_file_metadata() {
    let footer = SegmentFooter {
        schema_version: SEGMENT_SCHEMA_VERSION,
        files: vec![
            SegmentFooterFile {
                file: SegmentFile::MetaJson,
                size: 128,
                checksum_xxh64: 0x1122_3344_5566_7788,
            },
            SegmentFooterFile {
                file: SegmentFile::Chunks,
                size: 4096,
                checksum_xxh64: 0x8877_6655_4433_2211,
            },
        ],
    };

    let bytes = encode_segment_footer(&footer).unwrap();
    let decoded = decode_segment_footer(&bytes).unwrap();

    assert_eq!(decoded, footer);
}

#[test]
fn segment_footer_rejects_bad_crc32c() {
    let footer = SegmentFooter {
        schema_version: SEGMENT_SCHEMA_VERSION,
        files: vec![SegmentFooterFile {
            file: SegmentFile::MetaJson,
            size: 128,
            checksum_xxh64: 0x1122_3344_5566_7788,
        }],
    };
    let mut bytes = encode_segment_footer(&footer).unwrap();
    bytes[SEGMENT_FOOTER_HEADER_LEN] ^= 0xff;

    let err = decode_segment_footer(&bytes).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn segment_footer_validation_rejects_tracked_file_corruption() {
    let tempdir = tempfile::tempdir().unwrap();
    write_footer_test_files(tempdir.path());
    write_segment_footer(tempdir.path()).unwrap();
    validate_segment_footer(tempdir.path()).unwrap();

    let symbols_path = tempdir.path().join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    symbols[0] ^= 0xff;
    fs::write(symbols_path, symbols).unwrap();
    let err = validate_segment_footer(tempdir.path()).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

fn write_footer_test_files(dir: &Path) {
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        fs::write(
            dir.join(file.filename()),
            format!("content:{}", file.filename()),
        )
        .unwrap();
    }
}

#[test]
fn manifest_segment_meta_accepts_matching_meta() {
    let id = SegmentId::with_ulid(100, 200, Ulid::new()).unwrap();
    let manifest_segment =
        crate::storage::manifest::ManifestSegment::new(id.dir_name(), 100, 200, Some(42)).unwrap();
    let meta = SegmentMeta {
        segment_id: id.dir_name(),
        start_ms: 100,
        end_ms: 200,
        datapoints: 3,
        series: 1,
        chunk_summary: None,
    };

    validate_manifest_segment_meta(&manifest_segment, &meta).unwrap();
}

#[test]
fn manifest_segment_meta_rejects_mismatched_meta_json() {
    let id = SegmentId::with_ulid(100, 200, Ulid::new()).unwrap();
    let manifest_segment =
        crate::storage::manifest::ManifestSegment::new(id.dir_name(), 100, 200, Some(42)).unwrap();
    let meta = SegmentMeta {
        segment_id: id.dir_name(),
        start_ms: 100,
        end_ms: 201,
        datapoints: 3,
        series: 1,
        chunk_summary: None,
    };

    let err = validate_manifest_segment_meta(&manifest_segment, &meta).unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

#[test]
fn segment_writer_creates_segment_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer.flush().unwrap();

    let entries: Vec<_> = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .collect();
    assert_eq!(entries.len(), 1);

    let seg_dir = entries[0].path();
    assert!(seg_dir.join("meta.json").exists());
    assert!(seg_dir.join("chunks.bin").exists());
    assert!(seg_dir.join("series.bin").exists());
    assert!(seg_dir.join("symbols.bin").exists());
    assert!(seg_dir.join("chunk_index.bin").exists());
    assert!(seg_dir.join("indexes.puffin").exists());
    assert!(!seg_dir.join("routing_index.bin").exists());
    assert!(seg_dir.join("footer.bin").exists());
    let chunk_len = fs::metadata(seg_dir.join("chunks.bin")).unwrap().len();
    assert!(chunk_len > 0);
    let index_len = fs::metadata(seg_dir.join("chunk_index.bin")).unwrap().len();
    assert!(index_len > 0);
}

#[test]
fn segment_writer_remaps_sealed_indexes_to_sorted_symbol_ids() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("z.label".to_string(), "last".to_string()),
        ("a.label".to_string(), "first".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(1), &labels, &[(1_000, 1.5)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(&seg_dir).unwrap();
    let symbols =
        read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols)).unwrap()).unwrap();
    let symbol_values: Vec<_> = (0..symbols.len())
        .map(|idx| symbols.resolve(idx as u32).unwrap().to_string())
        .collect();
    let mut sorted_symbol_values = symbol_values.clone();
    sorted_symbol_values.sort();
    assert_eq!(symbol_values, sorted_symbol_values);

    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).unwrap()).unwrap();
    assert_eq!(
        resolved_entry_labels(&symbols, &series[0]),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.usage")
            ),
            (normalize_label_name("a.label"), "first".to_string()),
            (normalize_label_name("z.label"), "last".to_string()),
        ]
    );

    let results = reader
        .query_exact(
            &[
                (
                    METRIC_NAME_LABEL,
                    normalize_metric_name("cpu.usage").as_str(),
                ),
                (normalize_label_name("a.label").as_str(), "first"),
            ],
            0,
            2_000,
        )
        .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples.len(), 1);
    assert_eq!(results[0].samples[0].1, 1.5);
}

#[test]
fn segment_writer_orders_sealed_series_by_metric_name_and_preserves_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    let z_first = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z-first".to_string()),
    ];
    let a_first = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a-first".to_string()),
    ];
    let z_second = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z-second".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(10), &z_first, &[(1_000, 10.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_first, &[(1_000, 20.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(12), &z_second, &[(1_000, 30.0)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(&seg_dir).unwrap();
    let symbols =
        read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols)).unwrap()).unwrap();
    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).unwrap()).unwrap();

    let ordered_labels: Vec<_> = series
        .iter()
        .map(|entry| resolved_entry_labels(&symbols, entry))
        .collect();
    let label_value = |labels: &[(String, String)], name: &str| {
        labels
            .iter()
            .find_map(|(key, value)| (key == name).then_some(value.as_str()))
            .unwrap()
            .to_string()
    };
    assert_eq!(
        ordered_labels
            .iter()
            .map(|labels| (
                label_value(labels, METRIC_NAME_LABEL),
                label_value(labels, normalize_label_name("pod.name").as_str())
            ))
            .collect::<Vec<_>>(),
        vec![
            (normalize_metric_name("a.metric"), "a-first".to_string()),
            (normalize_metric_name("z.metric"), "z-first".to_string()),
            (normalize_metric_name("z.metric"), "z-second".to_string()),
        ]
    );

    let chunk_entries = reader.read_chunk_index().unwrap();
    assert_eq!(chunk_entries.len(), 3);
    let chunk_offsets = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            entries[0].offset
        })
        .collect::<Vec<_>>();
    assert_eq!(
        chunk_offsets,
        {
            let mut sorted = chunk_offsets.clone();
            sorted.sort_unstable();
            sorted
        },
        "chunks.bin offsets should follow final metric-query series order"
    );
    let mut chunks = reader.open_chunks().unwrap();
    let decoded: Vec<_> = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            read_chunk_record_at(&mut chunks, entries[0].offset, entries[0].length).unwrap()
        })
        .collect();
    assert_eq!(
        decoded
            .iter()
            .map(|record| record.series_ref)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert_eq!(
        decoded
            .iter()
            .map(|record| match &record.samples {
                ChunkSamples::Float(samples) => samples[0].1,
                other => panic!("unexpected chunk samples: {other:?}"),
            })
            .collect::<Vec<_>>(),
        vec![20.0, 10.0, 30.0]
    );

    let a_results = reader
        .query_exact(
            &[(
                METRIC_NAME_LABEL,
                normalize_metric_name("a.metric").as_str(),
            )],
            0,
            2_000,
        )
        .unwrap();
    assert_eq!(a_results.len(), 1);
    assert_eq!(a_results[0].samples, vec![(1_000, 20.0)]);
}

#[test]
fn segment_writer_orders_chunk_payloads_by_series_ref_then_time() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    let z_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z".to_string()),
    ];
    let a_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(3_000, 30.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(10), &z_labels, &[(1_000, 10.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(1_000, 20.0)])
        .unwrap();
    writer.flush().unwrap();
    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.chunk_rewrite_frames(), 3);
    assert!(profile.chunk_rewrite_payload_bytes() > 0);

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(&seg_dir).unwrap();
    let chunk_entries = reader.read_chunk_index().unwrap();
    assert_eq!(chunk_entries.len(), 2);
    assert_eq!(
        chunk_entries[0]
            .iter()
            .map(|entry| entry.min_time_ms)
            .collect::<Vec<_>>(),
        vec![1_000, 3_000]
    );

    let chunk_offsets = chunk_entries
        .iter()
        .flat_map(|entries| entries.iter().map(|entry| entry.offset))
        .collect::<Vec<_>>();
    assert_eq!(
        chunk_offsets,
        {
            let mut sorted = chunk_offsets.clone();
            sorted.sort_unstable();
            sorted
        },
        "chunks.bin offsets should be series-major and time-ordered within each series"
    );
}

#[test]
fn segment_writer_publishes_manifest_records_for_flushed_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let manifest_dir = tempdir.path().join("manifest");
    let inventory = crate::storage::manifest::read_manifest_inventory(&manifest_dir)
        .unwrap()
        .expect("manifest inventory");
    let segment_dirs = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .collect::<Vec<_>>();

    assert_eq!(segment_dirs.len(), 1);
    assert_eq!(inventory.segments.len(), 1);
    assert_eq!(
        inventory.segments[0].segment_id,
        segment_dirs[0].file_name().to_string_lossy()
    );
}

#[test]
fn manifest_published_open_skips_footer_validation_by_default() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_with_labels(
            SeriesRef::new(1),
            &[(METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string())],
            &[(5_000, 1.0)],
        )
        .unwrap();
    writer.flush().unwrap();

    let manifest_dir = tempdir.path().join("manifest");
    let inventory = crate::storage::manifest::read_manifest_inventory(&manifest_dir)
        .unwrap()
        .expect("manifest inventory");
    assert_eq!(inventory.segments.len(), 1);
    let segment_dir = tempdir.path().join(&inventory.segments[0].segment_id);
    let symbols_path = segment_dir.join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    symbols[0] ^= 0xff;
    fs::write(symbols_path, symbols).unwrap();

    let store = SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir)
        .expect("default manifest open should skip heavy footer validation");
    assert_eq!(store.segments.len(), 1);
    let err = match SegmentStoreReader::open_manifest_published_with_options(
        tempdir.path(),
        &manifest_dir,
        SegmentStoreOpenOptions {
            validate_segment_footers: true,
        },
    ) {
        Ok(_) => panic!("validated manifest open should catch footer mismatch"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn segment_writer_persists_chunk_summary_in_meta() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(
                2_000,
                HistogramValue {
                    count: 2,
                    sum: Some(3.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let meta: SegmentMeta =
        serde_json::from_slice(&fs::read(seg_dir.join("meta.json")).unwrap()).unwrap();
    let summary = meta.chunk_summary.expect("chunk summary");

    assert_eq!(summary.chunks, 2);
    assert_eq!(summary.by_kind.float.chunks, 1);
    assert_eq!(summary.by_kind.histogram.chunks, 1);
    assert!(summary.chunk_bytes > 0);
    assert!(summary.by_kind.float.chunk_bytes > 0);
    assert!(summary.by_kind.histogram.chunk_bytes > 0);
}

#[test]
fn smoke_verify_uses_chunk_summary_for_totals_without_chunk_scan_when_not_sampling() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    fs::remove_file(seg_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    fs::remove_file(seg_dir.join(SegmentFile::Chunks.filename())).unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let report = store.smoke_verify(0, 10_000, 0).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.chunks, 1);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert!(report.sample_series.is_empty());
    assert!(report.queries.is_empty());
}

#[test]
fn deterministic_segment_id_provider_replays_same_sequence() {
    let first = DeterministicSegmentIdProvider::new(7);
    let first_id = first.next_segment_id(0, 10_000).unwrap();
    let second_id = first.next_segment_id(10_000, 20_000).unwrap();
    assert_ne!(first_id, second_id);

    let replay = DeterministicSegmentIdProvider::new(7);
    assert_eq!(replay.next_segment_id(0, 10_000).unwrap(), first_id);
    assert_eq!(replay.next_segment_id(10_000, 20_000).unwrap(), second_id);
}

#[test]
fn segment_writer_with_deterministic_ids_replays_same_directory_names() {
    fn write_segments(path: &Path) -> Vec<String> {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42);
        let mut writer = SegmentWriter::new(config).unwrap();

        writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
        writer
            .record_sample(SeriesRef::new(1), 11_000, 2.5)
            .unwrap();
        writer.flush().unwrap();

        let mut names: Vec<_> = fs::read_dir(path)
            .unwrap()
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.file_name().to_string_lossy().to_string())
            .filter(|name| name.starts_with("seg-"))
            .collect();
        names.sort();
        names
    }

    let first = tempfile::tempdir().unwrap();
    let replay = tempfile::tempdir().unwrap();

    let first_names = write_segments(first.path());
    let replay_names = write_segments(replay.path());

    assert_eq!(first_names.len(), 2);
    assert_eq!(first_names, replay_names);
}

#[test]
fn segment_writer_records_flush_profile_stages() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ];

    assert!(writer.last_flush_profile().is_none());

    writer
        .record_samples_with_labels(SeriesRef::new(7), &labels, &[(1_000, 1.5), (2_000, 2.5)])
        .unwrap();
    writer.flush().unwrap();

    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.datapoints, 2);
    assert_eq!(profile.series, 1);
    assert_eq!(
        profile.stage_kinds(),
        &[
            SegmentFlushStageKind::MetaJson,
            SegmentFlushStageKind::ChunksFlush,
            SegmentFlushStageKind::ChunkIndex,
            SegmentFlushStageKind::SegmentMetadata,
            SegmentFlushStageKind::LabelValues,
            SegmentFlushStageKind::LabelValueTimeRanges,
            SegmentFlushStageKind::RoutingIndexBuild,
            SegmentFlushStageKind::Symbols,
            SegmentFlushStageKind::Series,
            SegmentFlushStageKind::Indexes,
            SegmentFlushStageKind::OooChunks,
            SegmentFlushStageKind::Footer,
            SegmentFlushStageKind::Publish,
        ]
    );
    assert!(
        profile
            .stage_elapsed(SegmentFlushStageKind::SegmentMetadata)
            .is_some()
    );
    assert!(
        profile.total
            >= profile
                .stage_elapsed(SegmentFlushStageKind::Publish)
                .unwrap()
    );
}

#[test]
fn segment_writer_records_flush_profile_file_sizes() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
        ("pod".to_string(), "backend-1".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(7), &labels, &[(1_000, 1.5), (2_000, 2.5)])
        .unwrap();
    writer.flush().unwrap();

    let profile = writer.last_flush_profile().unwrap();
    for file in [
        SegmentFile::MetaJson,
        SegmentFile::Symbols,
        SegmentFile::Series,
        SegmentFile::Chunks,
        SegmentFile::OooChunks,
        SegmentFile::ChunkIndex,
        SegmentFile::Indexes,
        SegmentFile::Footer,
    ] {
        assert!(
            profile.file_size_bytes(file).is_some(),
            "missing file size for {}",
            file.filename()
        );
    }
    assert!(profile.file_size_bytes(SegmentFile::Chunks).unwrap() > 0);
    assert!(profile.total_file_bytes() >= profile.file_size_bytes(SegmentFile::Chunks).unwrap());
    assert_eq!(
        profile.total_file_bytes(),
        profile.data_file_bytes()
            + profile.metadata_file_bytes()
            + profile.index_file_bytes()
            + profile.footer_file_bytes()
    );
}

#[test]
fn segment_writer_skips_chunk_rewrite_when_input_is_metric_query_ordered() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let a_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a".to_string()),
    ];
    let z_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
        ("pod.name".to_string(), "z".to_string()),
    ];

    writer
        .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(1_000, 20.0)])
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(10), &z_labels, &[(1_000, 10.0)])
        .unwrap();
    writer.flush().unwrap();

    let profile = writer.last_flush_profile().unwrap();
    assert_eq!(profile.chunk_rewrite_frames(), 0);
    assert_eq!(profile.chunk_rewrite_payload_bytes(), 0);
}

#[test]
fn segment_writer_reserves_active_window_series_structures() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.reserve_window_series(0, 10_000, 4_096).unwrap();

    let active = writer.active.as_ref().unwrap();
    assert!(active.series_map.capacity() >= 4_096);
    assert!(active.metadata_present.capacity() >= 4_096);
    assert!(active.series_entries.capacity() >= 4_096);
    assert!(active.chunk_entries.capacity() >= 4_096);
}

#[test]
fn segment_writer_records_record_path_profile() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let before = writer.record_profile();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(7),
            &[(1_000, 1.5), (2_000, 2.5)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu_usage");
                visit("pod", "backend-1");
            },
        )
        .unwrap();

    let delta = writer.record_profile().saturating_sub(before);
    assert_eq!(delta.chunks, 1);
    assert_eq!(delta.samples, 2);
    assert!(delta.total_elapsed() <= delta.wall_elapsed);
}

#[test]
fn segment_series_metadata_builder_matches_raw_label_canonicalization() {
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.usage".to_string()),
        ("namespace".to_string(), "default".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];

    let canonical = crate::promql::canonicalize_labelset(
        "cpu.usage",
        &[("namespace", "default"), ("pod.name", "backend-1")],
    );
    let expected_series_id = crate::promql::series_id(&canonical);
    let expected_labels: Vec<_> = canonical
        .labels()
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();

    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in &raw_labels {
        builder.push_label(key, value);
    }
    let metadata = builder.finish();

    assert_eq!(metadata.series_id, expected_series_id);
    assert_eq!(metadata.labels, expected_labels);
}

#[test]
fn segment_series_metadata_builder_keeps_first_metric_name() {
    let raw_labels = vec![
        (METRIC_NAME_LABEL.to_string(), "cpu.first".to_string()),
        (METRIC_NAME_LABEL.to_string(), "cpu.second".to_string()),
        ("pod.name".to_string(), "backend-1".to_string()),
    ];

    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in &raw_labels {
        builder.push_label(key, value);
    }
    let metadata = builder.finish();

    assert!(metadata.labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.first")
    }));
    assert!(!metadata.labels.iter().any(|(key, value)| {
        key == METRIC_NAME_LABEL && value == &normalize_metric_name("cpu.second")
    }));
}

#[test]
fn label_visitor_encoder_matches_metadata_builder_canonicalization() {
    let raw_labels = [
        (METRIC_NAME_LABEL, "cpu.usage"),
        ("pod.name", "backend-1"),
        ("namespace", "default"),
    ];
    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in raw_labels {
        builder.push_label(key, value);
    }
    let expected = builder.finish();

    let mut symbols = SegmentSymbols::default();
    let mut postings = ExactPostingsIndex::default();
    let entry = encode_label_visitor_metadata(&mut symbols, &mut postings, 0, |visit| {
        for (key, value) in raw_labels {
            visit(key, value);
        }
    });

    let labels = resolved_entry_labels(&symbols, &entry);
    assert_eq!(entry.series_id, expected.series_id);
    assert_eq!(labels, expected.labels);
}

#[test]
fn borrowed_label_encoder_matches_owned_canonical_encoding() {
    let canonical = vec![
        (
            METRIC_NAME_LABEL.to_string(),
            normalize_metric_name("cpu.usage"),
        ),
        (normalize_label_name("namespace"), "default".to_string()),
        (normalize_label_name("pod.name"), "backend-1".to_string()),
    ];

    let mut owned_symbols = SegmentSymbols::default();
    let mut owned_postings = ExactPostingsIndex::default();
    let owned = encode_canonical_segment_labels(
        canonical
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        &mut owned_symbols,
        &mut owned_postings,
        0,
    );

    let mut borrowed_symbols = SegmentSymbols::default();
    let mut borrowed_postings = ExactPostingsIndex::default();
    let borrowed = encode_borrowed_canonical_segment_labels(
        canonical
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        &mut borrowed_symbols,
        &mut borrowed_postings,
        0,
    );

    assert_eq!(borrowed.series_id, owned.series_id);
    assert_eq!(
        resolved_entry_labels(&borrowed_symbols, &borrowed),
        resolved_entry_labels(&owned_symbols, &owned)
    );
}

#[test]
fn flat_interned_label_encoder_matches_visitor_encoding() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("namespace", "default")),
        crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut visitor_symbols = SegmentSymbols::default();
    let mut visitor_postings = ExactPostingsIndex::default();
    let visitor =
        encode_label_visitor_metadata(&mut visitor_symbols, &mut visitor_postings, 0, |visit| {
            crate::labels::LabelSetStore::visit_labelset(&store, series, |key, value| {
                visit(key, value)
            })
        });

    let mut flat_symbols = SegmentSymbols::default();
    let mut flat_postings = ExactPostingsIndex::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::new();
    let mut label_scratch = Vec::new();
    let flat = encode_flat_interned_label_metadata(
        &mut flat_symbols,
        &mut flat_postings,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        0,
        &store,
        series,
    );

    assert_eq!(flat.series_id, visitor.series_id);
    assert_eq!(
        resolved_entry_labels(&flat_symbols, &flat),
        resolved_entry_labels(&visitor_symbols, &visitor)
    );
}

#[test]
fn flat_interned_label_encoder_reuses_scratch_buffers() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("namespace", "default")),
        crate::labels::KeyValueRef::from(("pod.name", "backend-1")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut symbols = SegmentSymbols::default();
    let mut postings = ExactPostingsIndex::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::with_capacity(256);
    let initial_capacity = hash_scratch.capacity();
    let mut label_scratch = Vec::with_capacity(32);
    let initial_label_capacity = label_scratch.capacity();

    let first = encode_flat_interned_label_metadata(
        &mut symbols,
        &mut postings,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        0,
        &store,
        series,
    );
    assert_eq!(hash_scratch.len(), 0);
    assert_eq!(hash_scratch.capacity(), initial_capacity);
    assert_eq!(label_scratch.len(), 0);
    assert_eq!(label_scratch.capacity(), initial_label_capacity);

    let second = encode_flat_interned_label_metadata(
        &mut symbols,
        &mut postings,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        1,
        &store,
        series,
    );
    assert_eq!(hash_scratch.len(), 0);
    assert_eq!(hash_scratch.capacity(), initial_capacity);
    assert_eq!(label_scratch.len(), 0);
    assert_eq!(label_scratch.capacity(), initial_label_capacity);
    assert_eq!(second.series_id, first.series_id);
    assert_eq!(
        resolved_entry_labels(&symbols, &second),
        resolved_entry_labels(&symbols, &first)
    );
}

#[test]
fn flat_interned_label_encoder_matches_disambiguated_label_canonicalization() {
    let labels = [
        crate::labels::KeyValueRef::from((METRIC_NAME_LABEL, "cpu.usage")),
        crate::labels::KeyValueRef::from(("pod.name", "dotted")),
        crate::labels::KeyValueRef::from(("pod_name", "underscore")),
    ];
    let mut store: crate::labels::FlatInternedLabelSetStore = Default::default();
    let series = crate::labels::LabelSetStore::intern(&mut store, &labels).unwrap();

    let mut visitor_symbols = SegmentSymbols::default();
    let mut visitor_postings = ExactPostingsIndex::default();
    let visitor =
        encode_label_visitor_metadata(&mut visitor_symbols, &mut visitor_postings, 0, |visit| {
            crate::labels::LabelSetStore::visit_labelset(&store, series, |key, value| {
                visit(key, value)
            })
        });

    let mut flat_symbols = SegmentSymbols::default();
    let mut flat_postings = ExactPostingsIndex::default();
    let mut normalized_names = NormalizedNameCache::default();
    let mut hash_scratch = Vec::new();
    let mut label_scratch = Vec::new();
    let flat = encode_flat_interned_label_metadata(
        &mut flat_symbols,
        &mut flat_postings,
        &mut normalized_names,
        &mut hash_scratch,
        &mut label_scratch,
        0,
        &store,
        series,
    );

    let flat_labels = resolved_entry_labels(&flat_symbols, &flat);
    assert_eq!(flat.series_id, visitor.series_id);
    assert_eq!(
        flat_labels,
        resolved_entry_labels(&visitor_symbols, &visitor)
    );
    assert_eq!(
        flat_labels,
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.usage")
            ),
            (normalize_label_name("pod_name"), "underscore".to_string()),
            (normalize_label_name("pod.name"), "dotted".to_string()),
        ]
    );
}

#[test]
fn flat_interned_sorted_label_encoder_keeps_last_duplicate_key() {
    let duplicate_key: Arc<str> = Arc::from("pod_name");
    let labels = vec![
        (
            Arc::from(METRIC_NAME_LABEL),
            SourceLabelValue::Owned(Arc::from("cpu_usage")),
        ),
        (
            Arc::clone(&duplicate_key),
            SourceLabelValue::Owned(Arc::from("first")),
        ),
        (duplicate_key, SourceLabelValue::Owned(Arc::from("second"))),
    ];
    let source_symbols = crate::labels::DefaultSymbolTable::default();
    let mut symbols = SegmentSymbols::default();
    let mut postings = ExactPostingsIndex::default();
    let mut hash_scratch = Vec::new();

    let entry = encode_flat_interned_sorted_labels(
        &labels,
        &source_symbols,
        &mut symbols,
        &mut postings,
        &mut hash_scratch,
        0,
    );

    assert_eq!(
        resolved_entry_labels(&symbols, &entry),
        vec![
            (METRIC_NAME_LABEL.to_string(), "cpu_usage".to_string()),
            (normalize_label_name("pod_name"), "second".to_string()),
        ]
    );
}

#[test]
fn normalized_name_cache_reuses_label_and_metric_names_by_source_symbol_id() {
    let mut cache = NormalizedNameCache::default();
    let mut label_normalizations = 0usize;
    let mut metric_normalizations = 0usize;
    let mut source_symbols = crate::labels::DefaultSymbolTable::default();
    let label_id = source_symbols.intern("pod.name").unwrap();
    let metric_id = source_symbols.intern("cpu.usage").unwrap();

    let first_label = cache.label_name(label_id, "pod.name", |name| {
        label_normalizations += 1;
        normalize_label_name(name)
    });
    let second_label = cache.label_name(label_id, "pod.name", |name| {
        label_normalizations += 1;
        normalize_label_name(name)
    });
    let first_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
        metric_normalizations += 1;
        normalize_metric_name(name)
    });
    let second_metric = cache.metric_name(metric_id, "cpu.usage", |name| {
        metric_normalizations += 1;
        normalize_metric_name(name)
    });

    assert_eq!(first_label.as_ref(), normalize_label_name("pod.name"));
    assert_eq!(second_label, first_label);
    assert_eq!(first_metric.as_ref(), normalize_metric_name("cpu.usage"));
    assert_eq!(second_metric, first_metric);
    assert_eq!(label_normalizations, 1);
    assert_eq!(metric_normalizations, 1);
}

#[test]
fn normalized_name_cache_falls_back_to_uncached_normalization_after_cap() {
    let mut cache = NormalizedNameCache::with_max_entries(1);
    let mut source_symbols = crate::labels::DefaultSymbolTable::default();
    let first_id = source_symbols.intern("pod.name").unwrap();
    let second_id = source_symbols.intern("container.name").unwrap();
    let mut normalizations = 0usize;

    cache.label_name(first_id, "pod.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(first_id, "pod.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(second_id, "container.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });
    cache.label_name(second_id, "container.name", |name| {
        normalizations += 1;
        normalize_label_name(name)
    });

    assert_eq!(normalizations, 3);
}

#[test]
fn label_visitor_encoder_keeps_first_metric_name_and_sorts_labels() {
    let mut symbols = SegmentSymbols::default();
    let mut postings = ExactPostingsIndex::default();

    let entry = encode_label_visitor_metadata(&mut symbols, &mut postings, 7, |visit| {
        visit("z.label", "last");
        visit(METRIC_NAME_LABEL, "cpu.first");
        visit("a.label", "first");
        visit(METRIC_NAME_LABEL, "cpu.second");
    });

    assert_eq!(
        resolved_entry_labels(&symbols, &entry),
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.first")
            ),
            (normalize_label_name("a.label"), "first".to_string()),
            (normalize_label_name("z.label"), "last".to_string()),
        ]
    );
    assert!(
        postings
            .get(
                symbols.lookup(&normalize_label_name("a.label")).unwrap(),
                symbols.lookup("first").unwrap()
            )
            .is_some_and(|refs| refs == [7])
    );
}

#[test]
fn label_value_time_ranges_update_from_encoded_series_entry() {
    let mut index = LabelValueTimeRangeIndex::default();
    let entry = SeriesEntry {
        series_id: 7,
        kind_mask: SERIES_KIND_FLOAT,
        chunk_index: Default::default(),
        labels: vec![(1, 10), (2, 20)],
    };
    let first_chunk = ChunkIndexEntry {
        file_id: 0,
        kind: ChunkKind::Float,
        flags: 0,
        min_time_ms: 1_000,
        max_time_ms: 2_000,
        offset: 0,
        length: 1,
        scalar_lane_offset: 0,
        scalar_lane_len: 0,
    };
    let second_chunk = ChunkIndexEntry {
        min_time_ms: 500,
        max_time_ms: 4_000,
        ..first_chunk.clone()
    };

    update_label_value_time_ranges(&mut index, &entry, &first_chunk);
    update_label_value_time_ranges(&mut index, &entry, &second_chunk);

    assert_eq!(
        index.get(1, 10),
        Some(LabelValueTimeRange {
            min_time_ms: 500,
            max_time_ms: 4_000,
        })
    );
    assert_eq!(
        index.get(2, 20),
        Some(LabelValueTimeRange {
            min_time_ms: 500,
            max_time_ms: 4_000,
        })
    );
}

#[test]
fn segment_writer_rotates_on_new_window() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(1), 1_000, 1.5).unwrap();
    writer
        .record_sample(SeriesRef::new(2), 25_000, 2.5)
        .unwrap();
    writer.flush().unwrap();

    let segments: Vec<_> = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .collect();
    assert_eq!(segments.len(), 2);
}

#[test]
fn segment_writer_batches_samples_per_series() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples(
            SeriesRef::new(5),
            &[(1_000, 1.0), (2_000, 2.0), (1_500, 1.5)],
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 3);
    assert_eq!(reader.meta().series, 1);
    let entries = reader.read_chunk_index().unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 1);
}

#[test]
fn segment_writer_records_ordered_samples_with_label_visitor() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(5),
            &[(1_000, 1.0), (1_500, 1.5), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu.usage");
                visit("pod.name", "backend-1");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 3);
    assert_eq!(reader.meta().series, 1);

    let mut chunk_reader = ChunkReader::new(reader.open_chunks().unwrap());
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(
        record.samples,
        ChunkSamples::Float(vec![(1_000, 1.0), (1_500, 1.5), (2_000, 2.0)])
    );
}

#[test]
fn segment_writer_ordered_samples_reject_unsorted_input() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    let err = writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(5),
            &[(2_000, 2.0), (1_000, 1.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu.usage");
                visit("pod.name", "backend-1");
            },
        )
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn segment_writer_writes_int_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_i64(SeriesRef::new(11), &[(1_000, 5), (2_000, -1)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    let chunk_file = reader.open_chunks().unwrap();
    let mut chunk_reader = ChunkReader::new(chunk_file);
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(record.kind, ChunkKind::Int64);
    assert_eq!(
        record.samples,
        ChunkSamples::Int64(vec![(1_000, 5), (2_000, -1)])
    );
}

#[test]
fn segment_writer_writes_typed_otlp_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    let histogram = HistogramValue {
        count: 4,
        sum: Some(10.0),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 5.0],
        bucket_counts: vec![1, 2, 1],
    };
    let exphist = ExponentialHistogramValue {
        count: 6,
        sum: Some(15.0),
        min: Some(1.0),
        max: Some(8.0),
        scale: 2,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![0],
        },
    };
    let summary = SummaryValue {
        count: 10,
        sum: 50.0,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![
            SummaryQuantileValue {
                quantile: 0.5,
                value: 4.0,
            },
            SummaryQuantileValue {
                quantile: 0.9,
                value: 8.0,
            },
        ],
    };

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(21),
            &[(1_000, histogram.clone())],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(22),
            &[(2_000, exphist.clone())],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.size");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(23),
            &[(3_000, summary.clone())],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.latency");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 3);
    assert_eq!(reader.meta().series, 3);

    let series =
        read_series_bin(File::open(reader.file_path(SegmentFile::Series)).expect("open series"))
            .unwrap();
    assert_eq!(
        series[0].kind_mask & SERIES_KIND_HISTOGRAM,
        SERIES_KIND_HISTOGRAM
    );
    assert_eq!(
        series[1].kind_mask & SERIES_KIND_SUMMARY,
        SERIES_KIND_SUMMARY
    );
    assert_eq!(
        series[2].kind_mask & SERIES_KIND_EXPONENTIAL_HISTOGRAM,
        SERIES_KIND_EXPONENTIAL_HISTOGRAM
    );

    let chunk_entries = reader.read_chunk_index().unwrap();
    assert_eq!(chunk_entries[0][0].kind, ChunkKind::Histogram);
    assert_eq!(chunk_entries[1][0].kind, ChunkKind::Summary);
    assert_eq!(chunk_entries[2][0].kind, ChunkKind::ExponentialHistogram);

    let mut chunks = reader.open_chunks().unwrap();
    let indexed_chunks: Vec<_> = chunk_entries
        .iter()
        .map(|entries| {
            assert_eq!(entries.len(), 1);
            read_chunk_record_at(&mut chunks, entries[0].offset, entries[0].length).unwrap()
        })
        .collect();
    assert_eq!(
        indexed_chunks[0].samples,
        ChunkSamples::Histogram(vec![(1_000, histogram)])
    );
    assert_eq!(indexed_chunks[0].series_ref, 0);
    assert_eq!(
        indexed_chunks[1].samples,
        ChunkSamples::Summary(vec![(3_000, summary)])
    );
    assert_eq!(indexed_chunks[1].series_ref, 1);
    assert_eq!(
        indexed_chunks[2].samples,
        ChunkSamples::ExponentialHistogram(vec![(2_000, exphist)])
    );
    assert_eq!(indexed_chunks[2].series_ref, 2);
}

#[test]
fn promql_count_projection_materializes_labels_only_for_matching_kinds() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    for idx in 0..10u32 {
        let series_label = format!("float-{idx}");
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(idx + 1),
                &[(1_000, f64::from(idx))],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed.metric");
                    visit("series", &series_label);
                },
            )
            .unwrap();
    }

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(100),
            &[(
                1_000,
                HistogramValue {
                    count: 2,
                    sum: Some(3.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "mixed.metric");
                visit("series", "histogram");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("mixed.metric"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(1_000, 2.0)]);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(
        profile.series_entries_read, 1,
        "native count projection should not fully materialize scalar series labels"
    );
}

#[test]
fn promql_scalar_projection_materializes_labels_only_for_scalar_kinds() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 42.0)], |visit| {
            visit(METRIC_NAME_LABEL, "mixed_scalar_count");
            visit("series", "float");
        })
        .unwrap();

    for idx in 0..10u32 {
        let series_label = format!("histogram-{idx}");
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(100 + idx),
                &[(
                    1_000,
                    HistogramValue {
                        count: 2,
                        sum: Some(3.0),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "mixed_scalar_count");
                    visit("series", &series_label);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!(
        "{{{}=\"{}\"}}",
        METRIC_NAME_LABEL,
        normalize_metric_name("mixed_scalar_count")
    );
    let execution = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(execution.results[0].samples, vec![(1_000, 42.0)]);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(
        profile.series_entries_read, 1,
        "scalar projection should not fully materialize non-scalar series labels"
    );
}

#[test]
fn promql_projection_reuses_labels_for_same_series_across_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(7),
            &[(
                1_000,
                HistogramValue {
                    count: 2,
                    sum: Some(3.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_duration");
                visit("route", "/shared");
            },
        )
        .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(7),
            &[(
                11_000,
                HistogramValue {
                    count: 3,
                    sum: Some(4.0),
                    min: None,
                    max: None,
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 2],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request_duration");
                visit("route", "/shared");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 1);
    assert_eq!(
        execution.results[0].samples,
        vec![(1_000, 2.0), (11_000, 3.0)]
    );
    let profile = query_session.profile().delta_since(before);
    assert_eq!(
        profile.series_entries_read, 1,
        "labels for the same logical series_id should be materialized once per query session"
    );
}

#[test]
fn promql_projection_batches_label_materialization_for_segment_misses() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    for (series_ref, route, count) in [
        (SeriesRef::new(7), "/alpha", 2),
        (SeriesRef::new(8), "/beta", 3),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[(
                    1_000,
                    HistogramValue {
                        count,
                        sum: Some(count as f64),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, count - 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request_duration");
                    visit("route", route);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 2);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(profile.series_entries_read, 2);
    assert_eq!(
        profile.series_entry_read_batches, 1,
        "projection label cache misses in one segment should be materialized in one series.bin batch"
    );
}

#[test]
fn promql_projection_reuses_series_table_locators_for_label_materialization() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    for (series_ref, route, count) in [
        (SeriesRef::new(7), "/alpha", 2),
        (SeriesRef::new(8), "/beta", 3),
    ] {
        writer
            .record_histogram_samples_ordered_with_label_visitor(
                series_ref,
                &[(
                    1_000,
                    HistogramValue {
                        count,
                        sum: Some(count as f64),
                        min: None,
                        max: None,
                        metadata: TypedSampleMetadata::default(),
                        explicit_bounds: vec![1.0],
                        bucket_counts: vec![1, count - 1],
                    },
                )],
                |visit| {
                    visit(METRIC_NAME_LABEL, "request_duration");
                    visit("route", route);
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = SegmentReader::open(seg_dir).unwrap();
    let mut series_reader =
        SeriesReader::open(File::open(reader.file_path(SegmentFile::Series)).unwrap()).unwrap();
    let refs = [0, 1];
    let (locators, locator_bytes) = series_reader.read_entry_locators_with_bytes(&refs).unwrap();
    let (_, materialized_bytes) = series_reader
        .read_entries_from_locators_with_bytes(&locators)
        .unwrap();
    let expected_series_entry_bytes = locator_bytes + materialized_bytes;

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 2);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(profile.series_entry_bytes, expected_series_entry_bytes);
}

#[test]
fn segment_writer_writes_raw_float_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_raw(SeriesRef::new(12), &[(1_000, 1.0), (2_000, 2.0)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    let mut chunk_file = reader.open_chunks().unwrap();
    let encoding = read_chunk_encoding(&mut chunk_file);
    assert_eq!(encoding, ChunkEncoding::RawF64 as u8);
    chunk_file.seek(SeekFrom::Start(0)).unwrap();

    let mut chunk_reader = ChunkReader::new(chunk_file);
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(record.kind, ChunkKind::Float);
    assert_eq!(
        record.samples,
        ChunkSamples::Float(vec![(1_000, 1.0), (2_000, 2.0)])
    );
}

#[test]
fn segment_writer_writes_raw_int_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_i64_raw(SeriesRef::new(13), &[(1_000, 5), (2_000, -1)])
        .unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    let mut chunk_file = reader.open_chunks().unwrap();
    let encoding = read_chunk_encoding(&mut chunk_file);
    assert_eq!(encoding, ChunkEncoding::RawI64 as u8);
    chunk_file.seek(SeekFrom::Start(0)).unwrap();

    let mut chunk_reader = ChunkReader::new(chunk_file);
    let record = chunk_reader.read_next().unwrap().unwrap();
    assert_eq!(record.kind, ChunkKind::Int64);
    assert_eq!(
        record.samples,
        ChunkSamples::Int64(vec![(1_000, 5), (2_000, -1)])
    );
}

#[test]
fn segment_reader_loads_meta() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer.record_sample(SeriesRef::new(7), 5_000, 7.5).unwrap();
    writer.flush().unwrap();

    let seg_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(|entry| entry.ok())
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();

    let reader = SegmentReader::open(seg_dir).unwrap();
    assert_eq!(reader.meta().datapoints, 1);
    assert_eq!(reader.meta().series, 1);
}

#[test]
fn segment_store_smoke_verifier_counts_kinds_and_runs_promql_readbacks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.0), (2_000, 2.0)],
            |visit| {
                visit(METRIC_NAME_LABEL, "cpu.usage");
                visit("instance", "host-a");
            },
        )
        .unwrap();

    let histogram = HistogramValue {
        count: 4,
        sum: Some(10.0),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 5.0],
        bucket_counts: vec![1, 2, 1],
    };
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(1_000, histogram)],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();

    let exphist = ExponentialHistogramValue {
        count: 6,
        sum: Some(15.0),
        min: Some(1.0),
        max: Some(8.0),
        scale: 2,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![0],
        },
    };
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(3),
            &[(2_000, exphist)],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.size");
                visit("route", "/typed");
            },
        )
        .unwrap();

    let summary = SummaryValue {
        count: 10,
        sum: 50.0,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![SummaryQuantileValue {
            quantile: 0.9,
            value: 8.0,
        }],
    };
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(4),
            &[(3_000, summary)],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.latency");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let report = store.smoke_verify(0, 10_000, 1).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.datapoints, 5);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert_eq!(report.totals.by_kind.histogram.chunks, 1);
    assert_eq!(report.totals.by_kind.exponential_histogram.chunks, 1);
    assert_eq!(report.totals.by_kind.summary.chunks, 1);

    assert!(report.sample_series.iter().any(|series| {
        series.kind == ChunkKind::Float
            && series
                .labels
                .iter()
                .any(|(key, value)| key == "instance" && value == "host-a")
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Float && query.result_samples > 0 && query.samples_decoded > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Histogram
            && query.query.contains("_count")
            && query.result_series > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Histogram
            && query.query.contains("_bucket")
            && query.query.contains(r#"le="1""#)
            && query.result_series > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::ExponentialHistogram
            && query.query.contains("_bucket")
            && query.query.contains(r#"le="+Inf""#)
            && query.result_series > 0
    }));
    assert!(report.queries.iter().any(|query| {
        query.kind == ChunkKind::Summary
            && query.query.contains(r#"quantile="0.9""#)
            && query.result_series > 0
    }));
}
