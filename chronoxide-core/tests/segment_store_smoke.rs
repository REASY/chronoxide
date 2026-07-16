use std::time::Duration;

use chronoxide_core::labels::SeriesRef;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::chunk::ChunkKind;
use chronoxide_core::storage::head::{
    ExponentialHistogramBuckets, ExponentialHistogramValue, HistogramValue, SummaryQuantileValue,
    SummaryValue, TypedSampleMetadata,
};
use chronoxide_core::storage::segment::{SegmentStoreReader, SegmentWriter, SegmentWriterConfig};

fn open_default_store(segments_dir: &std::path::Path) -> SegmentStoreReader {
    SegmentStoreReader::open(segments_dir).unwrap()
}

#[test]
fn smoke_verify_reports_chunk_kinds_and_queryable_promql_projections() {
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

    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(2),
            &[(
                1_000,
                HistogramValue {
                    count: 4,
                    sum: Some(10.0),
                    min: Some(1.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata::default(),
                    explicit_bounds: vec![1.0, 5.0],
                    bucket_counts: vec![1, 2, 1],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.duration");
                visit("route", "/typed");
            },
        )
        .unwrap();

    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(3),
            &[(
                2_000,
                ExponentialHistogramValue {
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
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.size");
                visit("route", "/typed");
            },
        )
        .unwrap();

    writer
        .record_summary_samples_ordered_with_label_visitor(
            SeriesRef::new(4),
            &[(
                3_000,
                SummaryValue {
                    count: 10,
                    sum: 50.0,
                    metadata: TypedSampleMetadata::default(),
                    quantiles: vec![SummaryQuantileValue {
                        quantile: 0.9,
                        value: 8.0,
                    }],
                },
            )],
            |visit| {
                visit(METRIC_NAME_LABEL, "request.latency");
                visit("route", "/typed");
            },
        )
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let report = store.smoke_verify(0, 10_000, 1).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert_eq!(report.totals.by_kind.histogram.chunks, 1);
    assert_eq!(report.totals.by_kind.exponential_histogram.chunks, 1);
    assert_eq!(report.totals.by_kind.summary.chunks, 1);

    for kind in [
        ChunkKind::Float,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ] {
        assert!(
            report
                .sample_series
                .iter()
                .any(|sample| sample.kind == kind)
        );
        assert!(report.queries.iter().any(|query| {
            query.kind == kind && query.result_series > 0 && query.result_samples > 0
        }));
    }
}

#[test]
fn smoke_verify_queries_sampled_chunks_instead_of_full_requested_range() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(1));
    let mut writer = SegmentWriter::new(config).unwrap();
    let samples = (0..5_000)
        .map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64))
        .collect::<Vec<_>>();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &samples, |visit| {
            visit(METRIC_NAME_LABEL, "long.range.cpu");
            visit("instance", "host-a");
        })
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let report = store.smoke_verify(0, 10_000, 1).unwrap();

    assert_eq!(report.totals.segments, 5);
    assert_eq!(report.totals.by_kind.float.chunks, 5);
    assert_eq!(report.sample_series.len(), 1);
    assert_eq!(report.sample_series[0].samples, 1_000);
    assert_eq!(report.queries.len(), 1);
    assert_eq!(report.queries[0].samples_decoded, 1_000);
}

#[test]
fn smoke_verify_partial_segment_range_counts_only_overlapping_chunks() {
    let tempdir = tempfile::tempdir().unwrap();
    let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
    let mut writer = SegmentWriter::new(config).unwrap();
    let samples = (0..5_000)
        .map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64))
        .collect::<Vec<_>>();

    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(1), &samples, |visit| {
            visit(METRIC_NAME_LABEL, "partial.range.cpu");
            visit("instance", "host-a");
        })
        .unwrap();
    writer.flush().unwrap();

    let store = open_default_store(tempdir.path());
    let report = store.smoke_verify(0, 999, 0).unwrap();

    assert_eq!(report.totals.segments, 1);
    assert_eq!(report.totals.by_kind.float.chunks, 1);
    assert!(report.sample_series.is_empty());
    assert!(report.queries.is_empty());
}
