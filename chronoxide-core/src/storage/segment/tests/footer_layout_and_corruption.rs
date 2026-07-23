use super::*;

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
fn schema8_writer_is_default_and_maps_to_footer_schema8() {
    let config = SegmentWriterConfig::new("/tmp/segments", Duration::from_secs(60));

    assert_eq!(config.storage_schema, SegmentStorageSchema::Schema8);
    assert_eq!(
        config.storage_schema.footer_version(),
        SEGMENT_SCHEMA_VERSION_V8
    );
}

#[test]
fn segment_footer_roundtrips_file_metadata() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);

    let bytes = encode_segment_footer(&footer).unwrap();
    let decoded = decode_segment_footer_for_schema6(&bytes).unwrap();

    assert_eq!(decoded, footer);
}

#[test]
fn schema8_footer_requires_explicit_schema8_decoder() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V8);
    let bytes = encode_segment_footer(&footer).unwrap();

    assert_eq!(decode_segment_footer_for_schema8(&bytes).unwrap(), footer);
    assert_eq!(
        decode_segment_footer_for_schema7(&bytes)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(
        decode_segment_footer_for_schema6(&bytes)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn schema8_footer_validation_integrity_checks_tracked_files() {
    let segment_dir = tempfile::tempdir().unwrap();
    for file in SEGMENT_FOOTER_TRACKED_FILES {
        fs::write(
            segment_dir.path().join(file.filename()),
            file.filename().as_bytes(),
        )
        .unwrap();
    }
    write_segment_footer_for_schema(segment_dir.path(), SEGMENT_SCHEMA_VERSION_V8).unwrap();

    validate_segment_footer_for_schema8(segment_dir.path()).unwrap();
}

#[test]
fn segment_footer_rejects_bad_crc32c() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);
    let mut bytes = encode_segment_footer(&footer).unwrap();
    bytes[SEGMENT_FOOTER_HEADER_LEN] ^= 0xff;

    let err = decode_segment_footer_for_schema6(&bytes).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn layout_ab_footer_decoder_accepts_only_checksum_valid_schema5() {
    let footer = footer_test_fixture(LEGACY_SEGMENT_SCHEMA_VERSION_FOR_LAYOUT_AB);
    let bytes = encode_segment_footer(&footer).unwrap();

    assert_eq!(
        decode_segment_footer_for_schema6(&bytes)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
    assert_eq!(decode_segment_footer_for_layout_ab(&bytes).unwrap(), footer);

    let mut corrupt = bytes;
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xff;
    assert_eq!(
        decode_segment_footer_for_layout_ab(&corrupt)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn segment_footer_rejects_noncanonical_inventory() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);

    let mut missing = footer.clone();
    missing.files.pop();
    assert_eq!(
        decode_segment_footer_for_schema6(&encode_segment_footer(&missing).unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut duplicate = footer.clone();
    duplicate.files[1] = duplicate.files[0].clone();
    assert_eq!(
        decode_segment_footer_for_schema6(&encode_segment_footer(&duplicate).unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut reordered = footer;
    reordered.files.swap(0, 1);
    assert_eq!(
        decode_segment_footer_for_schema6(&encode_segment_footer(&reordered).unwrap())
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn segment_footer_rejects_nonzero_reserved_fields() {
    let footer = footer_test_fixture(SEGMENT_SCHEMA_VERSION_V6);
    let encoded = encode_segment_footer(&footer).unwrap();

    let mut payload_reserved = encoded.clone();
    payload_reserved[SEGMENT_FOOTER_HEADER_LEN + 2] = 1;
    rewrite_footer_test_crc(&mut payload_reserved);
    assert_eq!(
        decode_segment_footer_for_schema6(&payload_reserved)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );

    let mut entry_reserved = encoded;
    entry_reserved[SEGMENT_FOOTER_HEADER_LEN + 6] = 1;
    rewrite_footer_test_crc(&mut entry_reserved);
    assert_eq!(
        decode_segment_footer_for_schema6(&entry_reserved)
            .unwrap_err()
            .kind(),
        ErrorKind::InvalidData
    );
}

#[test]
fn segment_footer_validation_rejects_tracked_file_corruption() {
    let tempdir = tempfile::tempdir().unwrap();
    write_footer_test_files(tempdir.path());
    write_segment_footer_for_schema6(tempdir.path()).unwrap();
    validate_segment_footer_for_schema6(tempdir.path()).unwrap();

    let symbols_path = tempdir.path().join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    symbols[0] ^= 0xff;
    fs::write(symbols_path, symbols).unwrap();
    let err = validate_segment_footer_for_schema6(tempdir.path()).unwrap_err();

    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn segment_footer_hashes_large_files_with_the_fixed_streaming_buffer() {
    let tempdir = tempfile::tempdir().unwrap();
    let len = SEGMENT_FOOTER_HASH_BUFFER_BYTES * 2 + 17;
    let bytes: Vec<u8> = (0..len)
        .map(|index| (index as u8).wrapping_mul(19).wrapping_add(5))
        .collect();
    fs::write(tempdir.path().join(SegmentFile::Chunks.filename()), &bytes).unwrap();

    let entry = segment_footer_file(tempdir.path(), SegmentFile::Chunks).unwrap();

    assert_eq!(SEGMENT_FOOTER_HASH_BUFFER_BYTES, 1024 * 1024);
    assert_eq!(entry.size, len as u64);
    assert_eq!(entry.checksum_xxh64, xxhash64(&bytes));
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
    let pages_offset = u64::from_le_bytes(symbols[56..64].try_into().unwrap()) as usize;
    symbols[pages_offset] ^= 0xff;
    fs::write(symbols_path, symbols).unwrap();

    let store = SegmentStoreReader::open_manifest_published(tempdir.path(), &manifest_dir)
        .expect("default manifest open should skip heavy footer validation");
    assert_eq!(store.segments.len(), 1);
    let err = match SegmentStoreReader::open_manifest_published_with_options(
        tempdir.path(),
        &manifest_dir,
        SegmentStoreOpenOptions {
            validate_segment_footers: true,
            ..SegmentStoreOpenOptions::default()
        },
    ) {
        Ok(_) => panic!("validated manifest open should catch footer mismatch"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}

#[test]
fn validated_segment_open_parses_every_symbols_page_after_footer_hashing() {
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

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let symbols_path = segment_dir.join(SegmentFile::Symbols.filename());
    let mut symbols = fs::read(&symbols_path).unwrap();
    let pages_offset = u64::from_le_bytes(symbols[56..64].try_into().unwrap()) as usize;
    symbols[pages_offset] ^= 0xff;
    fs::write(&symbols_path, symbols).unwrap();

    // Integrity-check the deliberately malformed bytes so this test exercises
    // structural page validation rather than a footer hash mismatch.
    write_segment_footer_for_schema(&segment_dir, SEGMENT_SCHEMA_VERSION_V8).unwrap();
    SegmentReader::open(&segment_dir).unwrap();
    let error = match SegmentReader::open_validated(&segment_dir) {
        Ok(_) => panic!("validated open accepted a malformed symbols page"),
        Err(error) => error,
    };

    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("symbols page CRC mismatch"));
}

#[test]
fn ordinary_segment_open_rejects_an_old_schema_without_hashing_tracked_files() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    writer.record_sample(SeriesRef::new(1), 5_000, 1.0).unwrap();
    writer.flush().unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    let footer_path = segment_dir.join(SegmentFile::Footer.filename());
    let mut footer = read_segment_footer_for_schema8(&segment_dir).unwrap();
    footer.schema_version = SEGMENT_SCHEMA_VERSION_V7;
    fs::write(footer_path, encode_segment_footer(&footer).unwrap()).unwrap();

    let error = match SegmentReader::open(&segment_dir) {
        Ok(_) => panic!("ordinary open accepted an old segment schema"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("schema version"));
}

#[test]
fn schema6_layout_ab_rejects_schema5() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    writer
        .record_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, 1.5), (2_000, 2.5)],
            |visit| {
                visit(METRIC_NAME_LABEL, "layout.ab.metric");
                visit("service", "api");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    open_schema6_store_for_test(tempdir.path()).unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .unwrap()
        .path();
    rewrite_symbols_and_footer_as_schema5_v2_for_layout_ab(&segment_dir);
    let error = match SegmentStoreReader::open(tempdir.path()) {
        Ok(_) => panic!("production store open accepted schema 5"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);

    let error = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .err()
    .expect("schema-6 layout A/B accepted retired schema 5");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("schema version"));
}

#[test]
fn explicit_layout_ab_rejects_a_mixed_schema5_schema6_store() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();
    writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();
    writer
        .record_sample(SeriesRef::new(1), 11_000, 2.0)
        .unwrap();
    writer.flush().unwrap();
    let mut segment_dirs = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    segment_dirs.sort();
    assert_eq!(segment_dirs.len(), 2);
    rewrite_symbols_and_footer_as_schema5_v2_for_layout_ab(&segment_dirs[0]);

    let error = match SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            ..SegmentStoreOpenOptions::default()
        },
    ) {
        Ok(_) => panic!("layout A/B open accepted a mixed-schema store"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("schema version"));
}
