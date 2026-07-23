use super::*;

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
fn schema7_writer_publishes_v3_v2_v8_roots() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.5)], |visit| {
            visit(METRIC_NAME_LABEL, "schema7_metric");
            visit("service", "api");
        })
        .unwrap();
    writer.flush().unwrap();

    let stages = writer.last_flush_profile().unwrap().stage_kinds();
    let ooo_stage = stages
        .iter()
        .position(|stage| *stage == SegmentFlushStageKind::OooChunks)
        .unwrap();
    let series_stage = stages
        .iter()
        .position(|stage| *stage == SegmentFlushStageKind::Series)
        .unwrap();
    assert!(ooo_stage < series_stage);

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    validate_segment_footer_for_schema7(&segment_dir).unwrap();

    let series = fs::read(segment_dir.join(SegmentFile::Series.filename())).unwrap();
    let series_header = crate::storage::series::v3::SeriesHeaderV3::decode(
        &series[..crate::storage::series::v3::SERIES_HEADER_LEN_V3],
    )
    .unwrap();
    crate::storage::series::v3::decode_series_root_v3(
        &series[..usize::try_from(series_header.hot_pages_offset).unwrap()],
    )
    .unwrap();

    let chunk_index = fs::read(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    let overflow_root = crate::storage::chunk::decode_chunk_overflow_root_v2(
        &chunk_index[..crate::storage::chunk::CHUNK_OVERFLOW_ROOT_V2_LEN],
        chunk_index.len() as u64,
    )
    .unwrap();
    assert_eq!(overflow_root.series_count, 1);
    assert_eq!(overflow_root.blob_count, 0);

    let indexes = fs::read(segment_dir.join(SegmentFile::Indexes.filename())).unwrap();
    assert_eq!(u16::from_le_bytes(indexes[4..6].try_into().unwrap()), 8);
    assert_eq!(
        read_segment_footer_for_schema7(&segment_dir)
            .unwrap()
            .schema_version,
        SEGMENT_SCHEMA_VERSION_V7
    );
}

#[test]
fn default_schema8_writer_publishes_v3_v2_v9_roots() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.5)], |visit| {
            visit(METRIC_NAME_LABEL, "schema8_metric");
            visit("service", "api");
        })
        .unwrap();
    writer.flush().unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    validate_segment_footer_for_schema8(&segment_dir).unwrap();

    let series = fs::read(segment_dir.join(SegmentFile::Series.filename())).unwrap();
    let series_header = crate::storage::series::v3::SeriesHeaderV3::decode(
        &series[..crate::storage::series::v3::SERIES_HEADER_LEN_V3],
    )
    .unwrap();
    crate::storage::series::v3::decode_series_root_v3(
        &series[..usize::try_from(series_header.hot_pages_offset).unwrap()],
    )
    .unwrap();

    let chunk_index = fs::read(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    let overflow_root = crate::storage::chunk::decode_chunk_overflow_root_v2(
        &chunk_index[..crate::storage::chunk::CHUNK_OVERFLOW_ROOT_V2_LEN],
        chunk_index.len() as u64,
    )
    .unwrap();
    assert_eq!(overflow_root.series_count, 1);
    assert_eq!(overflow_root.blob_count, 0);

    let indexes = fs::read(segment_dir.join(SegmentFile::Indexes.filename())).unwrap();
    assert_eq!(u16::from_le_bytes(indexes[4..6].try_into().unwrap()), 9);
    assert_eq!(
        read_segment_footer_for_schema8(&segment_dir)
            .unwrap()
            .schema_version,
        SEGMENT_SCHEMA_VERSION_V8
    );
}

#[test]
fn inline_one_spill_survives_reorder_and_readback_in_schema7_and_schema8() {
    for (schema, policy) in [
        (
            SegmentStorageSchema::Schema7,
            SegmentStoreSchemaPolicy::StrictSchema7,
        ),
        (
            SegmentStorageSchema::Schema8,
            SegmentStoreSchemaPolicy::StrictSchema8,
        ),
    ] {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(schema);
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
            .record_samples_with_labels(SeriesRef::new(10), &z_labels, &[(3_000, 30.0)])
            .unwrap();
        writer
            .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(2_000, 20.0)])
            .unwrap();
        writer
            .record_samples_with_labels(SeriesRef::new(10), &z_labels, &[(1_000, 10.0)])
            .unwrap();
        assert!(writer.active.as_ref().unwrap().chunk_entries.rows()[0].spilled());

        writer.flush().unwrap();
        assert_eq!(
            writer.last_flush_profile().unwrap().chunk_rewrite_frames(),
            3
        );

        let segment_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        match schema {
            SegmentStorageSchema::Schema7 => {
                validate_segment_footer_for_schema7(&segment_dir).unwrap()
            }
            SegmentStorageSchema::Schema8 => {
                validate_segment_footer_for_schema8(&segment_dir).unwrap()
            }
            SegmentStorageSchema::Schema6 => unreachable!(),
        }

        let chunk_index = fs::read(segment_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
        let decoded_chunk_index =
            crate::storage::chunk::decode_chunk_index_v2(&chunk_index).unwrap();
        assert_eq!(decoded_chunk_index.root.series_count, 2);
        assert_eq!(decoded_chunk_index.root.blob_count, 1);
        assert_eq!(decoded_chunk_index.blobs.len(), 1);
        assert_eq!(decoded_chunk_index.blobs[0].series_ref, 1);
        assert_eq!(
            decoded_chunk_index.blobs[0]
                .entries
                .iter()
                .map(|entry| entry.min_time_ms)
                .collect::<Vec<_>>(),
            vec![1_000, 3_000]
        );
        assert!(
            decoded_chunk_index.blobs[0].entries[0].offset
                < decoded_chunk_index.blobs[0].entries[1].offset
        );

        let store = SegmentStoreReader::open_with_options(
            tempdir.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: policy,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        let a_metric = normalize_metric_name("a.metric");
        let z_metric = normalize_metric_name("z.metric");
        let a_results = store
            .query_exact(&[(METRIC_NAME_LABEL, a_metric.as_str())], 0, 4_000)
            .unwrap();
        let z_results = store
            .query_exact(&[(METRIC_NAME_LABEL, z_metric.as_str())], 0, 4_000)
            .unwrap();
        assert_eq!(a_results.len(), 1);
        assert_eq!(a_results[0].samples, vec![(2_000, 20.0)]);
        assert_eq!(z_results.len(), 1);
        assert_eq!(z_results[0].samples, vec![(1_000, 10.0), (3_000, 30.0)]);
    }
}

#[test]
fn explicit_schema6_selection_is_deterministic() {
    fn write(path: &Path) -> BTreeMap<String, Vec<u8>> {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42)
            .with_storage_schema(SegmentStorageSchema::Schema6);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(1_000, 1.5)],
                |visit| {
                    visit(METRIC_NAME_LABEL, "schema6_metric");
                    visit("service", "api");
                },
            )
            .unwrap();
        writer.flush().unwrap();

        let segment = fs::read_dir(path)
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        SEGMENT_FLUSH_SIZE_FILES
            .iter()
            .map(|file| {
                (
                    file.filename().to_string(),
                    fs::read(segment.join(file.filename())).unwrap(),
                )
            })
            .collect()
    }

    let first = tempfile::tempdir().unwrap();
    let second = tempfile::tempdir().unwrap();
    assert_eq!(write(first.path()), write(second.path()));
}

#[test]
fn segment_writer_remaps_sealed_indexes_to_sorted_symbol_ids() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
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
    let reader = open_schema6_segment_for_test(&seg_dir).unwrap();
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
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
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
    let reader = open_schema6_segment_for_test(&seg_dir).unwrap();
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
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
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
    let reader = open_schema6_segment_for_test(&seg_dir).unwrap();
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
            SegmentFlushStageKind::MetricSeriesRanges,
            SegmentFlushStageKind::RoutingIndexBuild,
            SegmentFlushStageKind::Indexes,
            SegmentFlushStageKind::Symbols,
            SegmentFlushStageKind::OooChunks,
            SegmentFlushStageKind::Series,
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
fn metric_query_ordered_fast_path_is_byte_identical_to_generic_identity_order() {
    fn write(path: &Path, trusted_order: bool) -> (String, BTreeMap<String, Vec<u8>>) {
        let config = SegmentWriterConfig::new(path, Duration::from_secs(10))
            .with_deterministic_segment_ids(42);
        let mut writer = SegmentWriter::new(config).unwrap();
        let a_labels = vec![
            (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
            ("pod.name".to_string(), "a".to_string()),
        ];
        let z_labels = vec![
            (METRIC_NAME_LABEL.to_string(), "z.metric".to_string()),
            ("pod.name".to_string(), "z".to_string()),
        ];

        if trusted_order {
            writer
                .reserve_metric_query_ordered_window_series(0, 10_000, 2)
                .unwrap();
            assert!(writer.active.as_ref().unwrap().metric_query_ordered_input);
        }
        writer
            .record_samples_with_labels(SeriesRef::new(11), &a_labels, &[(1_000, 20.0)])
            .unwrap();
        writer
            .record_samples_with_labels(SeriesRef::new(10), &z_labels, &[(1_000, 10.0)])
            .unwrap();
        writer.flush().unwrap();
        assert_eq!(
            writer.last_flush_profile().unwrap().chunk_rewrite_frames(),
            0
        );

        let segment = fs::read_dir(path)
            .unwrap()
            .filter_map(Result::ok)
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap();
        let segment_name = segment.file_name().to_string_lossy().into_owned();
        let artifacts = SEGMENT_FLUSH_SIZE_FILES
            .iter()
            .map(|file| {
                (
                    file.filename().to_string(),
                    fs::read(segment.path().join(file.filename())).unwrap(),
                )
            })
            .collect();
        (segment_name, artifacts)
    }

    let generic = tempfile::tempdir().unwrap();
    let trusted = tempfile::tempdir().unwrap();
    assert_eq!(write(generic.path(), false), write(trusted.path(), true));
}

#[test]
fn segment_writer_skips_chunk_rewrite_for_trusted_metric_query_order() {
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
        .reserve_metric_query_ordered_window_series(0, 10_000, 2)
        .unwrap();
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
fn trusted_identity_series_order_still_rewrites_interleaved_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
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
        .reserve_metric_query_ordered_window_series(0, 10_000, 2)
        .unwrap();
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

    let segment = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let reader = open_schema6_segment_for_test(&segment).unwrap();
    let chunk_entries = reader.read_chunk_index().unwrap();
    assert_eq!(
        chunk_entries[0]
            .iter()
            .map(|entry| entry.min_time_ms)
            .collect::<Vec<_>>(),
        vec![1_000, 3_000]
    );
    assert_eq!(
        chunk_entries
            .iter()
            .flat_map(|entries| entries.iter().map(|entry| entry.offset))
            .collect::<Vec<_>>(),
        {
            let mut offsets = chunk_entries
                .iter()
                .flat_map(|entries| entries.iter().map(|entry| entry.offset))
                .collect::<Vec<_>>();
            offsets.sort_unstable();
            offsets
        }
    );
}

#[test]
fn trusted_identity_series_order_propagates_chunk_rewrite_errors() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a".to_string()),
    ];

    writer
        .reserve_metric_query_ordered_window_series(0, 10_000, 1)
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(11), &labels, &[(1_000, 20.0)])
        .unwrap();
    writer.active.as_mut().unwrap().chunk_entries.series_mut(0)[0].file_id = 1;

    let error = writer.flush().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(
        error
            .to_string()
            .contains("series-major chunk rewrite only supports chunks.bin entries")
    );
    assert!(
        fs::read_dir(tempdir.path())
            .unwrap()
            .filter_map(Result::ok)
            .all(|entry| !entry.file_name().to_string_lossy().starts_with("seg-"))
    );
}

#[test]
fn schema8_series_failure_after_index_write_stays_unpublished() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema8);
    let mut writer = SegmentWriter::new(config).unwrap();
    let labels = vec![
        (METRIC_NAME_LABEL.to_string(), "a.metric".to_string()),
        ("pod.name".to_string(), "a".to_string()),
    ];

    writer
        .reserve_metric_query_ordered_window_series(0, 10_000, 1)
        .unwrap();
    writer
        .record_samples_with_labels(SeriesRef::new(11), &labels, &[(1_000, 20.0)])
        .unwrap();
    let segment_name = writer.active.as_ref().unwrap().id.dir_name();
    writer.active.as_mut().unwrap().chunk_entries.series_mut(0)[0].offset = u64::MAX;

    let error = writer.flush().unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("schema-7 chunk range overflows"));

    let temporary_segment = tempdir.path().join(".tmp").join(&segment_name);
    assert!(
        temporary_segment
            .join(SegmentFile::Indexes.filename())
            .is_file()
    );
    assert!(
        !temporary_segment
            .join(SegmentFile::Footer.filename())
            .exists()
    );
    assert!(!tempdir.path().join(segment_name).exists());
    assert!(!tempdir.path().join("manifest").exists());
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
    assert_eq!(delta.label_time_range, Duration::ZERO);
    assert!(delta.total_elapsed() <= delta.wall_elapsed);
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
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
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

    let reader = open_schema6_segment_for_test(seg_dir).unwrap();
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
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
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

    let reader = open_schema6_segment_for_test(seg_dir).unwrap();
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
