use super::*;
use crate::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue, SummaryQuantileValue,
    SummaryValue, TypedSampleMetadata,
};
use crate::storage::io::{ChunkReadConfig, ChunkReadMode};

fn segment_dirs(root: &Path) -> Vec<PathBuf> {
    let mut dirs = fs::read_dir(root)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
        .map(|entry| entry.path())
        .collect::<Vec<_>>();
    dirs.sort_unstable();
    dirs
}

fn schema6_writer_config(root: &Path, segment_duration: Duration) -> SegmentWriterConfig {
    SegmentWriterConfig::new(root, segment_duration)
        .with_storage_schema(SegmentStorageSchema::Schema6)
}

fn summary(count: u64) -> SummaryValue {
    SummaryValue {
        count,
        sum: count as f64,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![SummaryQuantileValue {
            quantile: 0.5,
            value: count as f64,
        }],
    }
}

fn replace_only_chunk_with_ooo_summary(segment_dir: &Path) -> (u64, f64) {
    let index_path = segment_dir.join(SegmentFile::ChunkIndex.filename());
    let mut entries = read_chunk_index(&mut File::open(&index_path).unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 1);
    let original = entries[0][0].clone();
    assert_eq!(original.file_id, 0);
    assert_eq!(original.kind, ChunkKind::Summary);

    let expected_count = if original.min_time_ms < 10_000 {
        101
    } else {
        202
    };
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_summary_chunk_ordered(0, &[(original.min_time_ms, summary(expected_count))])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    assert_eq!(replacement.offset, original.offset);
    entries[0][0] = replacement;
    write_chunk_index(File::create(index_path).unwrap(), &entries).unwrap();
    write_segment_footer_for_schema6(segment_dir).unwrap();
    validate_segment_footer_for_schema6(segment_dir).unwrap();
    (original.min_time_ms, expected_count as f64)
}

fn replace_only_chunk_with_ooo_summary_samples(
    segment_dir: &Path,
    samples: &[(u64, u64)],
) -> Vec<(u64, f64)> {
    let index_path = segment_dir.join(SegmentFile::ChunkIndex.filename());
    let mut entries = read_chunk_index(&mut File::open(&index_path).unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 1);
    let original = entries[0][0].clone();
    assert_eq!(original.file_id, 0);
    assert_eq!(original.kind, ChunkKind::Summary);

    let encoded = samples
        .iter()
        .map(|(timestamp_ms, count)| (*timestamp_ms, summary(*count)))
        .collect::<Vec<_>>();
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer.append_summary_chunk_ordered(0, &encoded).unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    assert_eq!(replacement.offset, original.offset);
    entries[0][0] = replacement;
    write_chunk_index(File::create(index_path).unwrap(), &entries).unwrap();
    write_segment_footer_for_schema6(segment_dir).unwrap();
    validate_segment_footer_for_schema6(segment_dir).unwrap();
    samples
        .iter()
        .map(|(timestamp_ms, count)| (*timestamp_ms, *count as f64))
        .collect()
}

fn replace_second_float_chunk_with_ooo(segment_dir: &Path) -> (u64, f64) {
    let index_path = segment_dir.join(SegmentFile::ChunkIndex.filename());
    let mut entries = read_chunk_index(&mut File::open(&index_path).unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 2);
    let in_order = entries[0][0].clone();
    let replaced = entries[0][1].clone();
    assert_eq!(in_order.file_id, 0);
    assert_eq!(replaced.file_id, 0);
    assert_eq!(in_order.kind, ChunkKind::Float);
    assert_eq!(replaced.kind, ChunkKind::Float);
    assert_eq!(in_order.min_time_ms, replaced.min_time_ms);

    let expected_value = if replaced.min_time_ms < 10_000 {
        303.0
    } else {
        404.0
    };
    let ooo_path = segment_dir.join(SegmentFile::OooChunks.filename());
    let mut writer = ChunkWriter::new(File::create(&ooo_path).unwrap()).unwrap();
    let mut replacement = writer
        .append_float_sample(0, replaced.min_time_ms, expected_value)
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    assert_eq!(replacement.offset, in_order.offset);
    entries[0][1] = replacement;
    write_chunk_index(File::create(index_path).unwrap(), &entries).unwrap();
    write_segment_footer_for_schema6(segment_dir).unwrap();
    validate_segment_footer_for_schema6(segment_dir).unwrap();
    (replaced.min_time_ms, expected_value)
}

fn replace_only_chunk_with_ooo_histogram(segment_dir: &Path, value: HistogramValue) {
    let index_path = segment_dir.join(SegmentFile::ChunkIndex.filename());
    let mut entries = read_chunk_index(&mut File::open(&index_path).unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 1);
    let original = entries[0][0].clone();
    assert_eq!(original.file_id, 0);
    assert_eq!(original.kind, ChunkKind::Histogram);

    let mut writer = ChunkWriter::new(
        File::create(segment_dir.join(SegmentFile::OooChunks.filename())).unwrap(),
    )
    .unwrap();
    let mut replacement = writer
        .append_histogram_chunk_ordered(0, &[(original.min_time_ms, value)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    assert_eq!(replacement.offset, original.offset);
    entries[0][0] = replacement;
    write_chunk_index(File::create(index_path).unwrap(), &entries).unwrap();
    write_segment_footer_for_schema6(segment_dir).unwrap();
    validate_segment_footer_for_schema6(segment_dir).unwrap();
}

fn replace_only_chunk_with_ooo_exponential_histogram(
    segment_dir: &Path,
    value: ExponentialHistogramValue,
) {
    let index_path = segment_dir.join(SegmentFile::ChunkIndex.filename());
    let mut entries = read_chunk_index(&mut File::open(&index_path).unwrap()).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].len(), 1);
    let original = entries[0][0].clone();
    assert_eq!(original.file_id, 0);
    assert_eq!(original.kind, ChunkKind::ExponentialHistogram);

    let mut writer = ChunkWriter::new(
        File::create(segment_dir.join(SegmentFile::OooChunks.filename())).unwrap(),
    )
    .unwrap();
    let mut replacement = writer
        .append_exponential_histogram_chunk_ordered(0, &[(original.min_time_ms, value)])
        .unwrap();
    replacement.file_id = 1;
    writer.flush().unwrap();
    drop(writer);

    assert_eq!(replacement.offset, original.offset);
    entries[0][0] = replacement;
    write_chunk_index(File::create(index_path).unwrap(), &entries).unwrap();
    write_segment_footer_for_schema6(segment_dir).unwrap();
    validate_segment_footer_for_schema6(segment_dir).unwrap();
}

fn query_default_and_cross_segment(
    root: &Path,
    query: &str,
    max_open_files: u32,
) -> (
    QueryExecution,
    SegmentStoreQueryProfile,
    QueryExecution,
    SegmentStoreQueryProfile,
) {
    let store = SegmentStoreReader::open_with_options(
        root,
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            metadata_governor: MetadataGovernorConfig {
                max_open_files,
                max_cached_open_files: max_open_files,
                ..MetadataGovernorConfig::default()
            },
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let run = |cross_segment| {
        let mut session = store.query_session().unwrap();
        session
            .set_chunk_read_config(ChunkReadConfig {
                mode: ChunkReadMode::Pread,
                queue_depth: 8,
                payload_coalesce_max_gap_bytes:
                    crate::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
            })
            .unwrap();
        session.set_experimental_cross_segment_chunk_reads(cross_segment);
        let before = session.profile();
        let execution = session
            .query_promql_with_limits(query, 0, 20_000, QueryLimits::unlimited())
            .unwrap();
        let profile = session.profile().delta_since(before);
        (execution, profile)
    };

    let (default, default_profile) = run(false);
    let (cross, cross_profile) = run(true);
    assert_eq!(store.metadata_runtime.snapshot().files.active_leases, 0);
    (default, default_profile, cross, cross_profile)
}

fn assert_equivalent_runs(default: &QueryExecution, cross: &QueryExecution) {
    assert_eq!(cross.results, default.results);
    assert_eq!(cross.stats, default.stats);
    assert_eq!(
        cross.semantic_fingerprint_sha256(),
        default.semantic_fingerprint_sha256()
    );
    assert_eq!(
        cross.portable_semantic_fingerprint_sha256(),
        default.portable_semantic_fingerprint_sha256()
    );
}

#[test]
fn schema6_range_query_legacy_context_uses_configured_payload_coalesce_gap() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(schema6_writer_config(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    for (series_ref, selected, value) in [
        (SeriesRef::new(1), "yes", 1.0),
        (SeriesRef::new(2), "no", 99.0),
        (SeriesRef::new(3), "yes", 2.0),
    ] {
        writer
            .record_samples_ordered_with_label_visitor(series_ref, &[(5_000, value)], |visit| {
                visit(METRIC_NAME_LABEL, "legacy_payload_coalesce_gap");
                visit("selected", selected);
            })
            .unwrap();
    }
    writer.flush().unwrap();

    let run = |payload_coalesce_max_gap_bytes| {
        let store = open_schema6_store_for_test(tempdir.path()).unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_chunk_read_config(ChunkReadConfig {
                mode: ChunkReadMode::Pread,
                queue_depth: 8,
                payload_coalesce_max_gap_bytes,
            })
            .unwrap();
        session.set_range_scalar_cache_budget_bytes(0).unwrap();
        let execution = session
            .query_promql_range_with_limits(
                r#"legacy_payload_coalesce_gap{selected="yes"}"#,
                5_000,
                5_000,
                1_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        (execution, session.profile())
    };

    let (uncoalesced, uncoalesced_profile) = run(0);
    let (coalesced, coalesced_profile) = run(4096);

    assert_equivalent_runs(&uncoalesced, &coalesced);
    assert_eq!(
        coalesced_profile.chunk_payload_bytes,
        uncoalesced_profile.chunk_payload_bytes
    );
    assert_eq!(uncoalesced_profile.chunk_payload_physical_reads, 2);
    assert_eq!(coalesced_profile.chunk_payload_physical_reads, 1);
    assert!(
        coalesced_profile.chunk_payload_physical_bytes
            > uncoalesced_profile.chunk_payload_physical_bytes
    );
}

#[test]
fn ooo_only_scalar_payload_routes_by_file_in_default_and_cross_segment_queries() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(schema6_writer_config(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for timestamp_ms in [1_000, 11_000] {
        writer
            .record_summary_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[(timestamp_ms, summary(1))],
                |visit| {
                    visit(METRIC_NAME_LABEL, "payload.routing.summary");
                    visit("source", "ooo-only");
                },
            )
            .unwrap();
    }
    writer.flush().unwrap();

    let mut expected = segment_dirs(tempdir.path())
        .iter()
        .map(|segment| replace_only_chunk_with_ooo_summary(segment))
        .collect::<Vec<_>>();
    expected.sort_unstable_by_key(|sample| sample.0);
    let query = format!("{}_count", normalize_metric_name("payload.routing.summary"));
    let (default, default_profile, cross, cross_profile) =
        query_default_and_cross_segment(tempdir.path(), &query, 128);

    assert_eq!(default.results.len(), 1);
    assert_eq!(default.results[0].samples, expected);
    assert_equivalent_runs(&default, &cross);
    assert_eq!(default_profile.chunk_read_scheduler.executions, 2);
    assert_eq!(cross_profile.chunk_read_scheduler.executions, 1);
    assert_eq!(cross_profile.chunk_read_scheduler.logical_requests, 2);
}

#[test]
fn smoke_scan_routes_ooo_payloads_by_file() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(schema6_writer_config(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(10_000, summary(1))],
            |visit| {
                visit(METRIC_NAME_LABEL, "payload.routing.smoke");
                visit("source", "ooo-smoke");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let segments = segment_dirs(tempdir.path());
    assert_eq!(segments.len(), 1);
    replace_only_chunk_with_ooo_summary_samples(&segments[0], &[(10_000, 2), (20_000, 3)]);

    let store = open_schema6_store_for_test(tempdir.path()).unwrap();
    let report = store.smoke_verify(0, 30_000, 1).unwrap();
    let sample = report
        .sample_series
        .iter()
        .find(|sample| sample.kind == ChunkKind::Summary)
        .expect("OOO Summary must be sampled");
    assert_eq!(sample.samples, 2);
    assert_eq!(sample.min_time_ms, 10_000);
    assert_eq!(sample.max_time_ms, 20_000);
    assert!(
        report
            .queries
            .iter()
            .any(|query| { query.kind == ChunkKind::Summary && query.result_samples == 2 })
    );
}

#[test]
fn ooo_native_histogram_kinds_route_by_file_in_default_and_cross_segment_queries() {
    let histogram_root = tempfile::tempdir().unwrap();
    let histogram = |bucket_counts| HistogramValue {
        count: 10,
        sum: Some(15.0),
        min: None,
        max: None,
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 2.0],
        bucket_counts,
    };
    let mut writer = SegmentWriter::new(schema6_writer_config(
        histogram_root.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(10_000, histogram(vec![10, 0, 0]))],
            |visit| visit(METRIC_NAME_LABEL, "payload.routing.native.histogram"),
        )
        .unwrap();
    writer.flush().unwrap();
    let segments = segment_dirs(histogram_root.path());
    assert_eq!(segments.len(), 1);
    replace_only_chunk_with_ooo_histogram(&segments[0], histogram(vec![0, 10, 0]));

    let query = "histogram_quantile(0.5, payload.routing.native.histogram)";
    let (default, _, cross, _) = query_default_and_cross_segment(histogram_root.path(), query, 1);
    assert_equivalent_runs(&default, &cross);
    assert_eq!(default.stats.typed_full_chunks_decoded, 1);
    assert_eq!(default.results.len(), 1);
    assert_eq!(default.results[0].samples, vec![(20_000, 1.5)]);

    let exponential_root = tempfile::tempdir().unwrap();
    let exponential = |offset| ExponentialHistogramValue {
        count: 10,
        sum: Some(15.0),
        min: None,
        max: None,
        metadata: TypedSampleMetadata::default(),
        scale: 0,
        zero_count: 0,
        zero_threshold: 0.0,
        positive: ExponentialHistogramBuckets {
            offset,
            counts: vec![10],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    };
    let mut writer = SegmentWriter::new(schema6_writer_config(
        exponential_root.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(10_000, exponential(1))],
            |visit| visit(METRIC_NAME_LABEL, "payload.routing.native.exponential"),
        )
        .unwrap();
    writer.flush().unwrap();
    let segments = segment_dirs(exponential_root.path());
    assert_eq!(segments.len(), 1);
    replace_only_chunk_with_ooo_exponential_histogram(&segments[0], exponential(0));

    let query = "histogram_quantile(0.5, payload.routing.native.exponential)";
    let (default, _, cross, _) = query_default_and_cross_segment(exponential_root.path(), query, 1);
    assert_equivalent_runs(&default, &cross);
    assert_eq!(default.stats.typed_full_chunks_decoded, 1);
    assert_eq!(default.results.len(), 1);
    assert_eq!(default.results[0].samples.len(), 1);
    assert_eq!(default.results[0].samples[0].0, 20_000);
    assert!((default.results[0].samples[0].1 - 2.0_f64.sqrt()).abs() < 1e-12);
}

#[test]
fn mixed_payload_files_keep_identical_offsets_distinct_in_default_and_cross_segment_queries() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(schema6_writer_config(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    for timestamp_ms in [1_000, 11_000] {
        for decoy in [1.0, 2.0] {
            writer
                .record_samples_ordered_with_label_visitor(
                    SeriesRef::new(1),
                    &[(timestamp_ms, decoy)],
                    |visit| {
                        visit(METRIC_NAME_LABEL, "payload.routing.float");
                        visit("source", "mixed");
                    },
                )
                .unwrap();
        }
    }
    writer.flush().unwrap();

    let mut expected = segment_dirs(tempdir.path())
        .iter()
        .map(|segment| replace_second_float_chunk_with_ooo(segment))
        .collect::<Vec<_>>();
    expected.sort_unstable_by_key(|sample| sample.0);
    let query = normalize_metric_name("payload.routing.float");
    let (default, default_profile, cross, cross_profile) =
        query_default_and_cross_segment(tempdir.path(), &query, 1);

    assert_eq!(default.results.len(), 1);
    assert_eq!(default.results[0].samples, expected);
    assert_equivalent_runs(&default, &cross);
    assert_eq!(default_profile.chunk_read_scheduler.executions, 4);
    assert_eq!(default_profile.chunk_read_scheduler.pread_decisions, 4);
    assert_eq!(cross_profile.chunk_read_scheduler.executions, 4);
    assert_eq!(cross_profile.chunk_read_scheduler.pread_decisions, 4);
    assert_eq!(cross_profile.chunk_read_scheduler.logical_requests, 4);
    assert_eq!(cross_profile.chunk_read_scheduler.physical_spans, 4);
    assert_eq!(default_profile.chunk_payload_locality.contiguous_runs, 4);
    assert_eq!(default_profile.chunk_payload_locality.backward_jumps, 0);
    assert_eq!(default_profile.chunk_payload_locality.forward_gaps, 0);
    assert_eq!(
        default_profile
            .chunk_payload_locality
            .sorted_contiguous_runs,
        4
    );
    assert_eq!(
        cross_profile.chunk_payload_locality,
        default_profile.chunk_payload_locality
    );

    let (cap_two_default, cap_two_default_profile, cap_two_cross, cap_two_cross_profile) =
        query_default_and_cross_segment(tempdir.path(), &query, 2);
    assert_eq!(cap_two_default.results, default.results);
    assert_eq!(cap_two_default.stats, default.stats);
    assert_eq!(
        cap_two_default.semantic_fingerprint_sha256(),
        default.semantic_fingerprint_sha256()
    );
    assert_eq!(
        cap_two_default.portable_semantic_fingerprint_sha256(),
        default.portable_semantic_fingerprint_sha256()
    );
    assert_equivalent_runs(&cap_two_default, &cap_two_cross);
    assert_eq!(cap_two_default_profile.chunk_read_scheduler.executions, 2);
    assert_eq!(cap_two_cross_profile.chunk_read_scheduler.executions, 2);
    assert_eq!(
        cap_two_cross_profile.chunk_read_scheduler.pread_decisions,
        2
    );
}

#[test]
fn ooo_scalar_payload_is_an_explicit_range_cache_bypass() {
    const CACHE_BUDGET_BYTES: u64 = 4 * 1024 * 1024;

    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(schema6_writer_config(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[
                (10_000, summary(1)),
                (20_000, summary(1)),
                (30_000, summary(1)),
            ],
            |visit| {
                visit(METRIC_NAME_LABEL, "payload.routing.cache");
                visit("source", "ooo-cache-bypass");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let segments = segment_dirs(tempdir.path());
    assert_eq!(segments.len(), 1);
    let expected = replace_only_chunk_with_ooo_summary_samples(
        &segments[0],
        &[(10_000, 2), (20_000, 3), (30_000, 5)],
    );
    let query = format!("{}_count", normalize_metric_name("payload.routing.cache"));

    let run = |cache_budget_bytes| {
        let store = open_schema6_store_for_test(tempdir.path()).unwrap();
        let mut session = store.query_session().unwrap();
        session
            .set_chunk_read_config(ChunkReadConfig {
                mode: ChunkReadMode::Pread,
                queue_depth: 8,
                payload_coalesce_max_gap_bytes:
                    crate::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
            })
            .unwrap();
        session
            .set_range_scalar_cache_budget_bytes(cache_budget_bytes)
            .unwrap();
        let before_stats = session.stats();
        let before_profile = session.profile();
        let execution = session
            .query_promql_range_with_limits(
                &query,
                10_000,
                30_000,
                10_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        let stats = session.stats().delta_since(before_stats);
        let profile = session.profile().delta_since(before_profile);
        let summary = session.last_range_scalar_cache_summary().copied().unwrap();
        (execution, stats, profile, summary)
    };

    let (cache_off, cache_off_stats, cache_off_profile, cache_off_summary) = run(0);
    let (cache_on, cache_on_stats, cache_on_profile, cache_on_summary) = run(CACHE_BUDGET_BYTES);

    assert_eq!(cache_off.results.len(), 1);
    assert_eq!(cache_off.results[0].samples, expected);
    assert_eq!(cache_on.results, cache_off.results);
    assert_eq!(cache_on.stats, cache_off.stats);
    assert_eq!(cache_on_stats, cache_off_stats);
    assert_eq!(
        cache_on.semantic_fingerprint_sha256(),
        cache_off.semantic_fingerprint_sha256()
    );
    assert_eq!(
        cache_on.portable_semantic_fingerprint_sha256(),
        cache_off.portable_semantic_fingerprint_sha256()
    );
    assert_eq!(
        cache_on_profile.chunk_payload_physical_reads,
        cache_off_profile.chunk_payload_physical_reads
    );
    assert_eq!(
        cache_on_profile.chunk_payload_physical_bytes,
        cache_off_profile.chunk_payload_physical_bytes
    );

    for (configured_budget, profile, summary) in [
        (0, cache_off_profile, cache_off_summary),
        (CACHE_BUDGET_BYTES, cache_on_profile, cache_on_summary),
    ] {
        assert_eq!(summary.configured_budget_bytes, configured_budget);
        assert_eq!(summary.hits, 0);
        assert_eq!(summary.misses, 0);
        assert_eq!(summary.admitted_entries, 0);
        assert_eq!(summary.streaming_budget_bypasses, 0);
        assert!(summary.unsupported_bypasses > 0, "{summary:?}");
        assert_eq!(summary.logical_hit_bytes, 0);
        assert_eq!(
            summary.logical_miss_or_bypass_bytes,
            profile.chunk_payload_bytes
        );
        assert_eq!(summary.peak_retained_charge_bytes, 0);
        assert_eq!(summary.retained_charge_after_finalize, 0);
    }
}

#[test]
fn scheduled_payload_short_read_is_sticky_only_for_the_exact_artifact() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(schema6_writer_config(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.0)], |visit| {
            visit(METRIC_NAME_LABEL, "payload.routing.short-read");
        })
        .unwrap();
    writer.flush().unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            metadata_governor: MetadataGovernorConfig {
                max_open_files: 1,
                max_cached_open_files: 1,
                ..MetadataGovernorConfig::default()
            },
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    assert_eq!(store.segments.len(), 1);
    let segment = &store.segments[0];
    let chunks = segment
        .registered_metadata
        .reader(SegmentFile::Chunks)
        .unwrap();
    let ooo_chunks = segment
        .registered_metadata
        .reader(SegmentFile::OooChunks)
        .unwrap();
    let scheduler = ChunkReadScheduler::new(Arc::new(
        crate::storage::io::ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::Pread,
            queue_depth: 8,
            payload_coalesce_max_gap_bytes:
                crate::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        })
        .unwrap(),
    ));
    let make_item = || {
        let request = ChunkPayloadRead {
            file_id: 0,
            offset: 0,
            len: 1,
        };
        ChunkReadSchedulerItem {
            segment_ordinal: 0,
            file_id: 0,
            file: ChunkReadSchedulerFile::Governed(chunks.clone()),
            plan: plan_chunk_payload_batch(&[request], 0).unwrap(),
            logical_requests: 1,
        }
    };

    let span_requests = (0..16)
        .map(|index| ChunkPayloadRead {
            file_id: 0,
            offset: index * 2,
            len: 1,
        })
        .collect::<Vec<_>>();
    let before_many_spans = store.metadata_runtime.snapshot();
    let (_, many_span_stats) = scheduler
        .execute(vec![ChunkReadSchedulerItem {
            segment_ordinal: 0,
            file_id: 0,
            file: ChunkReadSchedulerFile::Governed(chunks.clone()),
            plan: plan_chunk_payload_batch(&span_requests, 0).unwrap(),
            logical_requests: span_requests.len() as u64,
        }])
        .unwrap();
    assert_eq!(many_span_stats.physical_spans, 16);
    let after_many_spans = store.metadata_runtime.snapshot();
    assert_eq!(after_many_spans.files.active_leases, 0);
    assert_eq!(
        after_many_spans.files.lease_clones,
        before_many_spans.files.lease_clones + 1,
        "physical spans must share one governed lease token"
    );

    let (_, duplicate_stats) = scheduler.execute(vec![make_item(), make_item()]).unwrap();
    assert_eq!(duplicate_stats.executions, 1);
    assert_eq!(duplicate_stats.pread_decisions, 1);
    assert_eq!(duplicate_stats.logical_requests, 2);
    let before_failure = store.metadata_runtime.snapshot();
    assert_eq!(before_failure.files.active_leases, 0);

    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(segment.dir.join(SegmentFile::Chunks.filename()))
        .unwrap();

    let first = match scheduler.execute(vec![make_item()]) {
        Ok(_) => panic!("truncated cached descriptor unexpectedly read successfully"),
        Err(error) => error,
    };
    assert_eq!(first.kind(), io::ErrorKind::UnexpectedEof);
    assert!(first.to_string().contains("failed to fill whole buffer"));
    let after_first = store.metadata_runtime.snapshot();
    assert_eq!(after_first.files.active_leases, 0);
    assert_eq!(
        after_first.cache.sticky_artifacts,
        before_failure.cache.sticky_artifacts + 1
    );
    assert_eq!(
        after_first.files.acquire_calls,
        before_failure.files.acquire_calls + 1
    );

    let retry = match scheduler.execute(vec![make_item()]) {
        Ok(_) => panic!("sticky payload corruption unexpectedly read successfully"),
        Err(error) => error,
    };
    assert_eq!(retry.kind(), io::ErrorKind::UnexpectedEof);
    assert_eq!(retry.to_string(), first.to_string());
    let after_retry = store.metadata_runtime.snapshot();
    assert_eq!(after_retry.files.active_leases, 0);
    assert_eq!(
        after_retry.files.acquire_calls,
        after_first.files.acquire_calls
    );
    assert_eq!(
        after_retry.files.descriptor_opens,
        after_first.files.descriptor_opens
    );
    assert_eq!(
        after_retry.cache.corruption_hits,
        after_first.cache.corruption_hits + 1
    );

    drop(
        GovernedArtifactReader::acquire_file_leases(std::slice::from_ref(&ooo_chunks))
            .expect("unrelated OOO payload artifact must remain healthy"),
    );
    let after_unrelated = store.metadata_runtime.snapshot();
    assert_eq!(after_unrelated.files.active_leases, 0);
    assert_eq!(
        after_unrelated.cache.sticky_artifacts,
        after_retry.cache.sticky_artifacts
    );
}

#[cfg(all(target_os = "linux", feature = "io_uring"))]
#[test]
fn io_uring_multi_artifact_failure_is_sticky_only_for_its_request_file() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(schema6_writer_config(
        tempdir.path(),
        Duration::from_secs(60),
    ))
    .unwrap();
    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &[(1_000, 1.0)], |visit| {
            visit(METRIC_NAME_LABEL, "payload.routing.uring-error");
        })
        .unwrap();
    writer.flush().unwrap();
    let segments = segment_dirs(tempdir.path());
    assert_eq!(segments.len(), 1);
    fs::write(segments[0].join(SegmentFile::OooChunks.filename()), [0xA5]).unwrap();
    write_segment_footer_for_schema6(&segments[0]).unwrap();
    validate_segment_footer_for_schema6(&segments[0]).unwrap();

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            metadata_governor: MetadataGovernorConfig {
                max_open_files: 2,
                max_cached_open_files: 2,
                ..MetadataGovernorConfig::default()
            },
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let segment = &store.segments[0];
    let chunks = segment
        .registered_metadata
        .reader(SegmentFile::Chunks)
        .unwrap();
    let ooo_chunks = segment
        .registered_metadata
        .reader(SegmentFile::OooChunks)
        .unwrap();
    drop(
        GovernedArtifactReader::acquire_file_leases(&[chunks.clone(), ooo_chunks.clone()]).unwrap(),
    );
    fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(segment.dir.join(SegmentFile::Chunks.filename()))
        .unwrap();

    let scheduler = ChunkReadScheduler::new(Arc::new(
        crate::storage::io::ChunkReader::new(ChunkReadConfig {
            mode: ChunkReadMode::IoUring,
            queue_depth: 8,
            payload_coalesce_max_gap_bytes:
                crate::storage::io::DEFAULT_CHUNK_PAYLOAD_COALESCE_MAX_GAP_BYTES,
        })
        .unwrap(),
    ));
    let item = |file_id, file: GovernedArtifactReader| {
        let request = ChunkPayloadRead {
            file_id,
            offset: 0,
            len: 1,
        };
        ChunkReadSchedulerItem {
            segment_ordinal: 0,
            file_id,
            file: ChunkReadSchedulerFile::Governed(file),
            plan: plan_chunk_payload_batch(&[request], 0).unwrap(),
            logical_requests: 1,
        }
    };

    let before = store.metadata_runtime.snapshot();
    let error = match scheduler.execute(vec![item(1, ooo_chunks.clone()), item(0, chunks.clone())])
    {
        Ok(_) => panic!("truncated chunks artifact unexpectedly read successfully"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
    assert!(error.to_string().contains("failed to fill whole buffer"));
    let after = store.metadata_runtime.snapshot();
    assert_eq!(after.files.active_leases, 0);
    assert_eq!(
        after.cache.sticky_artifacts,
        before.cache.sticky_artifacts + 1
    );

    let (_, healthy_stats) = scheduler
        .execute(vec![item(1, ooo_chunks.clone())])
        .expect("unrelated OOO payload remains readable");
    assert_eq!(healthy_stats.io_uring_decisions, 1);
    let after_ooo = store.metadata_runtime.snapshot();
    assert_eq!(
        after_ooo.cache.sticky_artifacts,
        after.cache.sticky_artifacts
    );
    assert_eq!(after_ooo.files.active_leases, 0);

    let acquire_calls = after_ooo.files.acquire_calls;
    let retry = match scheduler.execute(vec![item(0, chunks)]) {
        Ok(_) => panic!("sticky chunks corruption unexpectedly read successfully"),
        Err(error) => error,
    };
    assert_eq!(retry.to_string(), error.to_string());
    assert_eq!(
        store.metadata_runtime.snapshot().files.acquire_calls,
        acquire_calls
    );
}
