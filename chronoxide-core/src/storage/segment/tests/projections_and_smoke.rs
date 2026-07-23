use super::*;

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
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    fs::remove_file(seg_dir.join(SegmentFile::ChunkIndex.filename())).unwrap();
    fs::remove_file(seg_dir.join(SegmentFile::Chunks.filename())).unwrap();

    let report = store.smoke_verify(0, 10_000, 0).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.chunks, 1);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert!(report.sample_series.is_empty());
    assert!(report.queries.is_empty());
}

#[test]
fn promql_count_projection_verifies_v7_candidates_before_kind_materialization() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
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

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
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
        profile.series_entries_read, 0,
        "schema-7 facade verification is charged to governed metadata reads"
    );
}

#[test]
fn promql_scalar_projection_reuses_projected_labels_across_queries() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema6);
    let mut writer = SegmentWriter::new(config).unwrap();

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
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
                visit(METRIC_NAME_LABEL, "cache.metric");
                visit("series", "histogram");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_schema6_store_for_test(tempdir.path()).unwrap();
    let mut query_session = store.query_session().unwrap();
    let query = format!("{}_count", normalize_metric_name("cache.metric"));

    let first = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();
    assert_eq!(first.results.len(), 1);
    assert_eq!(first.results[0].samples, vec![(1_000, 2.0)]);
    assert_eq!(query_session.projected_label_cache.entries.len(), 1);
    assert_eq!(query_session.projected_label_cache.misses, 1);
    assert_eq!(query_session.projected_label_cache.hits, 0);

    let second = query_session
        .query_promql_with_limits(&query, 0, 2_000, QueryLimits::unlimited())
        .unwrap();
    assert_eq!(second.results, first.results);
    assert_eq!(query_session.projected_label_cache.entries.len(), 1);
    assert_eq!(query_session.projected_label_cache.misses, 1);
    assert_eq!(query_session.projected_label_cache.hits, 1);
}

#[test]
fn promql_scalar_projection_verifies_v7_candidates_before_kind_materialization() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
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

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
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
        profile.series_entries_read, 0,
        "schema-7 facade verification is charged to governed metadata reads"
    );
}

#[test]
fn promql_projection_reuses_labels_for_same_series_across_segments() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
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

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
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
        profile.series_entries_read, 0,
        "schema-7 facade verification is charged to governed metadata reads"
    );
}

#[test]
fn promql_projection_batches_label_materialization_for_segment_misses() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
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

    let store = SegmentStoreReader::open_with_options(
        tempdir.path(),
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
            ..SegmentStoreOpenOptions::default()
        },
    )
    .unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 2);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(profile.series_entries_read, 0);
    assert_eq!(profile.series_entry_read_batches, 0);
}

#[test]
fn promql_projection_reuses_verified_series_entries_for_label_materialization() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
        .with_storage_schema(SegmentStorageSchema::Schema7);
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
    let schema7_options = SegmentStoreOpenOptions {
        storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
        ..SegmentStoreOpenOptions::default()
    };
    let metadata_runtime = open_metadata_runtime(schema7_options.metadata_governor).unwrap();
    SegmentReader::open_with_options(&seg_dir, schema7_options, metadata_runtime).unwrap();

    let store = SegmentStoreReader::open_with_options(tempdir.path(), schema7_options).unwrap();
    let mut query_session = store.query_session().unwrap();
    let before = query_session.profile();

    let query = format!("{}_count", normalize_metric_name("request_duration"));
    let execution = query_session
        .query_promql_with_limits(&query, 0, 20_000, QueryLimits::unlimited())
        .unwrap();

    assert_eq!(execution.results.len(), 2);
    let profile = query_session.profile().delta_since(before);
    assert_eq!(profile.series_entries_read, 0);
    assert_eq!(profile.series_entry_read_batches, 0);
    assert_eq!(profile.series_entry_bytes, 0);
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
