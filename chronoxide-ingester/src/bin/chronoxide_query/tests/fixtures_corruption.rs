use super::*;

#[test]
fn schema7_independent_readback_oracle_rejects_corrupt_indexed_prefix() {
    let tempdir = schema7_segment_store_with_all_inline_kinds();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let mut chunks = fs::read(&chunks_path).unwrap();
    for byte in &mut chunks {
        *byte ^= 1;
    }
    fs::write(chunks_path, chunks).unwrap();
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_corrupt_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: vec![2.0],
        validate_segment_footers: false,
    };

    let error = collect_expected_readbacks(&config, StorageLayoutArg::Schema7, &[true; 5])
        .expect_err("corrupt authenticated prefix must fail independent readback collection");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "schema-7 oracle indexed prefix CRC mismatch"
    );
}

#[test]
fn schema7_independent_readback_oracle_rejects_authenticated_scalar_flags() {
    let tempdir = schema7_segment_store_with_inline_float();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    set_schema7_inline_chunk_flags(&segment_dir, 0, 0x0001);
    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_flagged_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    let error = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema7,
        &[true, false, false, false, false],
    )
    .expect_err("authenticated reserved scalar flags must fail independent readback collection");
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(
        error.to_string(),
        "schema-7 oracle scalar chunk flags must be zero"
    );
}

#[test]
fn schema7_independent_readback_oracle_routes_inline_ooo_payload() {
    let tempdir = schema7_segment_store_with_inline_float();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_float_chunk_ordered(0, &[(1_000, 99.0), (2_000, 100.0)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    let original_offset = replace_schema7_inline_locator(&segment_dir, 0, &replacement);
    assert_eq!(replacement.offset, original_offset);

    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_ooo_inline_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };
    let expected = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema7,
        &[true, false, false, false, false],
    )
    .unwrap();

    assert_eq!(expected.len(), 5);
    assert!(
        expected
            .iter()
            .all(|readback| readback.query.contains("schema7_float"))
    );
    assert!(
        expected
            .iter()
            .any(|readback| readback.samples == [(1_000, 99.0), (2_000, 100.0)])
    );

    let report = run_query_smoke_with_storage_layout(&config, StorageLayoutArg::Schema7).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();
    assert_eq!(report.sample_series.len(), 1);
    assert!(markdown.contains("| Checked Queries | 5 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 0 |"));
}

#[test]
fn schema8_independent_readback_oracle_routes_inline_ooo_payload() {
    let tempdir = schema8_segment_store_with_inline_float();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_float_chunk_ordered(0, &[(1_000, 99.0), (2_000, 100.0)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    let original_offset = replace_schema7_inline_locator(&segment_dir, 0, &replacement);
    assert_eq!(replacement.offset, original_offset);

    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema8_ooo_inline_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 1,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };

    let report = run_query_smoke_with_storage_layout(&config, StorageLayoutArg::Schema8).unwrap();
    let markdown = fs::read_to_string(&config.output).unwrap();
    assert_eq!(report.sample_series.len(), 1);
    assert!(markdown.contains("| Checked Queries | 5 |"));
    assert!(markdown.contains("| Mismatches | 0 |"));
    assert!(markdown.contains("| Skipped Readback Queries | 0 |"));
    assert!(markdown.contains("| Isolation Check Skips | 0 |"));
}

#[test]
fn schema7_independent_readback_oracle_routes_mixed_overflow_payload_files() {
    let tempdir = schema7_segment_store_with_float_overflow();
    let segment_dir = segment_dirs(tempdir.path()).unwrap().remove(0);
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_float_chunk_ordered(0, &[(2_000, 99.0), (2_500, 100.0)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    let first_in_order_offset = replace_schema7_overflow_locator(&segment_dir, 1, &replacement);
    assert_eq!(replacement.offset, first_in_order_offset);

    let config = QuerySmokeConfig {
        segments_dir: tempdir.path().to_path_buf(),
        output: tempdir.path().join("schema7_ooo_overflow_oracle.md"),
        start_ms: 0,
        end_ms: 10_000,
        sample_limit_per_kind: 2,
        verify_readbacks: true,
        exponential_histogram_bucket_boundaries: Vec::new(),
        validate_segment_footers: false,
    };
    let expected = collect_expected_readbacks(
        &config,
        StorageLayoutArg::Schema7,
        &[true, false, false, false, false],
    )
    .unwrap();

    assert_eq!(expected.len(), 5);
    assert!(
        expected
            .iter()
            .all(|readback| readback.query.contains("schema7_overflow"))
    );
    assert!(expected.iter().any(|readback| {
        readback.samples
            == [
                (1_000, 1_000.0),
                (1_500, 1_500.0),
                (2_000, 99.0),
                (2_500, 100.0),
            ]
    }));
}

#[test]
fn independent_readback_decoder_routes_chunk_payload_file_ids() {
    let chunks = tempfile::NamedTempFile::new().unwrap();
    let ooo_chunks = tempfile::NamedTempFile::new().unwrap();

    let mut chunks_writer = ChunkWriter::new(chunks.reopen().unwrap()).unwrap();
    let chunks_entry = chunks_writer.append_float_sample(0, 1_000, 1.0).unwrap();
    chunks_writer.flush().unwrap();
    drop(chunks_writer);

    let mut ooo_writer = ChunkWriter::new(ooo_chunks.reopen().unwrap()).unwrap();
    let ooo_entry = ooo_writer.append_float_sample(0, 1_000, 99.0).unwrap();
    ooo_writer.flush().unwrap();
    drop(ooo_writer);

    assert_eq!(ooo_entry.offset, chunks_entry.offset);
    let mut files = [chunks.reopen().unwrap(), ooo_chunks.reopen().unwrap()];
    let record =
        read_chunk_record_from_payload_files(&mut files, 1, ooo_entry.offset, ooo_entry.length)
            .unwrap();
    let ChunkSamples::Float(samples) = record.samples else {
        panic!("expected a float payload");
    };
    assert_eq!(samples, vec![(1_000, 99.0)]);

    let error =
        read_chunk_record_from_payload_files(&mut files, 2, ooo_entry.offset, ooo_entry.length)
            .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert_eq!(error.to_string(), "chunk payload file_id must be 0 or 1");
}

#[test]
fn segment_footer_validation_is_opt_in_for_query_open() {
    let defaults = Args::parse_from(["chronoxide-query"]);
    assert!(!defaults.validate_segment_footers);

    let validated = Args::parse_from(["chronoxide-query", "--validate-segment-footers"]);
    assert!(validated.validate_segment_footers);
}

#[test]
fn open_segment_store_validates_manifest_segment_footers_only_when_requested() {
    let tempdir = segment_store_with_two_windows_schema7();
    let segments = sorted_segment_metadata(tempdir.path());
    assert_eq!(segments.len(), 2);
    publish_manifest_segments(tempdir.path(), &[&segments[0]]);

    let segment_dir = tempdir.path().join(segments[0].segment_id.clone());
    let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
    let mut chunks = fs::read(&chunks_path).unwrap();
    chunks[0] ^= 0xff;
    fs::write(chunks_path, chunks).unwrap();

    let _store = open_segment_store_for_layout_ab(
        tempdir.path(),
        false,
        query_projection_config(&[]),
        StorageLayoutArg::Schema7,
    )
    .expect("default query open should skip footer checksum validation");

    let err = match open_segment_store_for_layout_ab(
        tempdir.path(),
        true,
        query_projection_config(&[]),
        StorageLayoutArg::Schema7,
    ) {
        Ok(_) => panic!("validated query open should catch footer checksum mismatch"),
        Err(err) => err,
    };
    assert_eq!(err.kind(), ErrorKind::InvalidData);
}
