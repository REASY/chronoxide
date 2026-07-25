use std::{path::Path, sync::Arc, time::Duration};

use chronoxide_core::{
    labels::{
        KeyValueRef, LabelSetStore, METRIC_NAME_LABEL, SeriesRef,
        VersionedFlatInternedLabelSetStore,
    },
    storage::{
        head::{
            CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue,
            FloatEncoding, FrozenHeadFragment, FrozenHeadReadView, HeadBuffer, HeadConfig,
            HeadReadView, HistogramValue, IntEncoding, LiveSeriesCatalogBuilder,
            OtlpAggregationTemporality, SampleValue, TypedSampleMetadata,
        },
        segment::{
            QueryExecution, QueryLimits, RangeExecutionMode, SegmentStoreReader, SegmentWriter,
            SegmentWriterConfig,
        },
    },
};

const FLOAT_A_LABELS: &[(&str, &str)] = &[
    (METRIC_NAME_LABEL, "recursive_float"),
    ("host", "a"),
    ("job", "api-v1"),
];
const FLOAT_B_LABELS: &[(&str, &str)] = &[
    (METRIC_NAME_LABEL, "recursive_float"),
    ("host", "b"),
    ("job", "api-v1"),
];
const PEER_LABELS: &[(&str, &str)] = &[
    (METRIC_NAME_LABEL, "recursive_peer"),
    ("host", "a"),
    ("job", "api-v1"),
];
const HISTOGRAM_LABELS: &[(&str, &str)] = &[
    (METRIC_NAME_LABEL, "recursive_histogram"),
    ("host", "a"),
    ("job", "api-v1"),
];
const EXPONENTIAL_LABELS: &[(&str, &str)] = &[
    (METRIC_NAME_LABEL, "recursive_exponential"),
    ("host", "a"),
    ("job", "api-v1"),
];
const CLASSIC_LE_ONE_LABELS: &[(&str, &str)] = &[
    (METRIC_NAME_LABEL, "recursive_classic_bucket"),
    ("host", "a"),
    ("job", "api-v1"),
    ("le", "1"),
];
const CLASSIC_LE_INF_LABELS: &[(&str, &str)] = &[
    (METRIC_NAME_LABEL, "recursive_classic_bucket"),
    ("host", "a"),
    ("job", "api-v1"),
    ("le", "+Inf"),
];

struct ImmutableHeadBuilder {
    catalog: VersionedFlatInternedLabelSetStore,
    head: HeadBuffer,
    fragments: Vec<FrozenHeadFragment>,
}

impl ImmutableHeadBuilder {
    fn new() -> Self {
        let config = HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_out_of_order_time_window(Duration::from_secs(10))
        .with_compact_numeric_series(false);
        Self {
            catalog: VersionedFlatInternedLabelSetStore::default(),
            head: HeadBuffer::new(config).unwrap(),
            fragments: Vec::new(),
        }
    }

    fn series(&mut self, labels: &[(&str, &str)]) -> SeriesRef {
        let labels = labels
            .iter()
            .copied()
            .map(KeyValueRef::from)
            .collect::<Vec<_>>();
        self.catalog.intern(&labels).unwrap()
    }

    fn record(&mut self, series: SeriesRef, timestamp_ms: u64, value: SampleValue) {
        assert!(
            self.head
                .record_sample_with_outcome(series, timestamp_ms, value)
                .unwrap()
                .recorded
        );
    }

    fn publish(&mut self) {
        self.fragments
            .extend(self.head.try_freeze_for_publication().unwrap());
    }

    fn finish(mut self) -> HeadReadView {
        self.publish();
        let samples = Arc::new(FrozenHeadReadView::from_owned(self.fragments));
        let labels = Arc::new(self.catalog.snapshot().unwrap());
        let mut catalog = LiveSeriesCatalogBuilder::new(labels, 1).unwrap();
        catalog
            .reconcile_sample_store(samples.sample_store())
            .unwrap();
        HeadReadView::new_live(samples, Arc::new(catalog.finish().unwrap()), 1).unwrap()
    }
}

struct RecursiveSessionFixture {
    _sealed_dir: tempfile::TempDir,
    _live_dir: tempfile::TempDir,
    sealed: SegmentStoreReader,
    live_base: SegmentStoreReader,
    head: HeadReadView,
}

impl RecursiveSessionFixture {
    fn new() -> Self {
        let sealed_dir = tempfile::tempdir().unwrap();
        let live_dir = tempfile::tempdir().unwrap();
        let mut head = ImmutableHeadBuilder::new();
        let float_a = head.series(FLOAT_A_LABELS);
        let float_b = head.series(FLOAT_B_LABELS);
        let peer = head.series(PEER_LABELS);
        let histogram = head.series(HISTOGRAM_LABELS);
        let exponential = head.series(EXPONENTIAL_LABELS);
        let classic_le_one = head.series(CLASSIC_LE_ONE_LABELS);
        let classic_le_inf = head.series(CLASSIC_LE_INF_LABELS);

        for step in 0..5_u64 {
            let timestamp_ms = (step + 1) * 1_000;
            head.record(float_a, timestamp_ms, SampleValue::Float((step + 1) as f64));
            head.record(
                float_b,
                timestamp_ms,
                SampleValue::Float(((step + 1) * 10) as f64),
            );
            head.record(
                peer,
                timestamp_ms,
                SampleValue::Float(((step + 1) * 2) as f64),
            );
            head.record(
                histogram,
                timestamp_ms,
                SampleValue::Histogram(histogram_value(step)),
            );
            head.record(
                exponential,
                timestamp_ms,
                SampleValue::ExponentialHistogram(exponential_histogram_value(step)),
            );
            head.record(
                classic_le_one,
                timestamp_ms,
                SampleValue::Float((step + 1) as f64),
            );
            head.record(
                classic_le_inf,
                timestamp_ms,
                SampleValue::Float(((step + 1) * 2) as f64),
            );
            // Make the immutable side genuinely fragmented. Repeated queries
            // must re-enter the same pinned view rather than a mutable buffer.
            head.publish();
        }
        let head = head.finish();

        write_equivalent_sealed_fixture(
            sealed_dir.path(),
            float_a,
            float_b,
            peer,
            histogram,
            exponential,
            [classic_le_one, classic_le_inf],
        );

        Self {
            sealed: SegmentStoreReader::open(sealed_dir.path()).unwrap(),
            live_base: SegmentStoreReader::open(live_dir.path()).unwrap(),
            head,
            _sealed_dir: sealed_dir,
            _live_dir: live_dir,
        }
    }

    fn run_sealed(&self, query: &str) -> QueryExecution {
        let mut session = self.sealed.query_session().unwrap();
        let execution = session
            .query_promql_range_with_limits(query, 3_000, 5_000, 1_000, QueryLimits::unlimited())
            .unwrap_or_else(|error| panic!("sealed query `{query}` failed: {error}"));
        assert_repeated_summary(&session, query);
        execution
    }

    fn run_live(&self, query: &str) -> QueryExecution {
        let mut session = self
            .live_base
            .query_session_with_head_view(&self.head)
            .unwrap();
        let execution = session
            .query_promql_range_with_limits(query, 3_000, 5_000, 1_000, QueryLimits::unlimited())
            .unwrap_or_else(|error| panic!("live query `{query}` failed: {error}"));
        assert_repeated_summary(&session, query);
        execution
    }
}

fn assert_repeated_summary(
    session: &chronoxide_core::storage::segment::SegmentStoreQuerySession<'_>,
    query: &str,
) {
    let summary = session
        .last_range_execution_summary()
        .unwrap_or_else(|| panic!("query `{query}` did not record range telemetry"));
    assert_eq!(
        summary.effective_mode,
        RangeExecutionMode::Repeated,
        "query `{query}`"
    );
    assert_eq!(summary.evaluation_count, 3, "query `{query}`");
}

fn write_equivalent_sealed_fixture(
    path: &Path,
    float_a: SeriesRef,
    float_b: SeriesRef,
    peer: SeriesRef,
    histogram: SeriesRef,
    exponential: SeriesRef,
    classic: [SeriesRef; 2],
) {
    let mut writer =
        SegmentWriter::new(SegmentWriterConfig::new(path, Duration::from_secs(600))).unwrap();
    let timestamps = [1_000, 2_000, 3_000, 4_000, 5_000];
    write_float_series(
        &mut writer,
        float_a,
        FLOAT_A_LABELS,
        &timestamps.map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64 / 1_000.0)),
    );
    write_float_series(
        &mut writer,
        float_b,
        FLOAT_B_LABELS,
        &timestamps.map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64 / 100.0)),
    );
    write_float_series(
        &mut writer,
        peer,
        PEER_LABELS,
        &timestamps.map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64 / 500.0)),
    );
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            histogram,
            &timestamps
                .map(|timestamp_ms| (timestamp_ms, histogram_value(timestamp_ms / 1_000 - 1))),
            |visit| visit_labels(HISTOGRAM_LABELS, visit),
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            exponential,
            &timestamps.map(|timestamp_ms| {
                (
                    timestamp_ms,
                    exponential_histogram_value(timestamp_ms / 1_000 - 1),
                )
            }),
            |visit| visit_labels(EXPONENTIAL_LABELS, visit),
        )
        .unwrap();
    write_float_series(
        &mut writer,
        classic[0],
        CLASSIC_LE_ONE_LABELS,
        &timestamps.map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64 / 1_000.0)),
    );
    write_float_series(
        &mut writer,
        classic[1],
        CLASSIC_LE_INF_LABELS,
        &timestamps.map(|timestamp_ms| (timestamp_ms, timestamp_ms as f64 / 500.0)),
    );
    writer.flush().unwrap();
}

fn write_float_series(
    writer: &mut SegmentWriter,
    series: SeriesRef,
    labels: &[(&str, &str)],
    samples: &[(u64, f64)],
) {
    writer
        .record_samples_ordered_with_label_visitor(series, samples, |visit| {
            visit_labels(labels, visit);
        })
        .unwrap();
}

fn visit_labels(labels: &[(&str, &str)], visit: &mut dyn FnMut(&str, &str)) {
    for &(name, value) in labels {
        visit(name, value);
    }
}

fn histogram_value(seed: u64) -> HistogramValue {
    HistogramValue {
        count: 6 + seed,
        sum: Some(12.0 + seed as f64),
        min: Some(0.5),
        max: Some(8.0),
        metadata: cumulative_metadata(),
        explicit_bounds: vec![1.0, 5.0],
        bucket_counts: vec![1 + seed, 2, 3],
    }
}

fn exponential_histogram_value(seed: u64) -> ExponentialHistogramValue {
    ExponentialHistogramValue {
        count: 4 + seed,
        sum: Some(9.0 + seed as f64),
        min: Some(-1.0),
        max: Some(4.0),
        scale: 1,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: cumulative_metadata(),
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![2 + seed],
        },
        negative: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![1],
        },
    }
}

fn cumulative_metadata() -> TypedSampleMetadata {
    TypedSampleMetadata {
        start_time_ms: Some(100),
        flags: 0,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
    }
}

#[test]
fn immutable_head_reaches_every_recursive_promql_source_during_repeated_range_execution() {
    let fixture = RecursiveSessionFixture::new();
    let cases = [
        // Selector-owning source nodes in the production dispatch.
        ("vector", r#"recursive_float{host="a"}"#, true),
        (
            "range function",
            r#"sum_over_time(recursive_float{host="a"}[3s])"#,
            true,
        ),
        (
            "native range function custom",
            r#"changes(recursive_histogram{host="a"}[3s])"#,
            true,
        ),
        (
            "native range function exponential",
            r#"resets(recursive_exponential{host="a"}[3s])"#,
            true,
        ),
        (
            "quantile_over_time",
            r#"quantile_over_time(0.5, recursive_float{host="a"}[3s])"#,
            true,
        ),
        (
            "predict_linear",
            r#"predict_linear(recursive_float{host="a"}[3s], 1)"#,
            true,
        ),
        (
            "double_exponential_smoothing",
            r#"double_exponential_smoothing(recursive_float{host="a"}[3s], 0.5, 0.5)"#,
            true,
        ),
        (
            "absent_over_time",
            r#"absent_over_time(recursive_float{host="a"}[3s])"#,
            false,
        ),
        // Recursive boxed-input nodes.
        (
            "scalar function",
            r#"scalar(recursive_float{host="a"})"#,
            true,
        ),
        ("offset", r#"recursive_float{host="a"} offset 1s"#, true),
        (
            "label_replace",
            r#"label_replace(recursive_float{host="a"}, "service", "$1", "job", "(.+)-v[0-9]+")"#,
            true,
        ),
        (
            "label_join",
            r#"label_join(recursive_float{host="a"}, "target", "/", "job", "host")"#,
            true,
        ),
        (
            "terminal aggregation source",
            r#"sum by (job) (recursive_float)"#,
            true,
        ),
        (
            "nested aggregation",
            r#"sum by (job) (abs(recursive_float))"#,
            true,
        ),
        ("absent", r#"absent(recursive_float{host="a"})"#, false),
        (
            "instant function",
            r#"sgn(recursive_float{host="a"} - 4)"#,
            true,
        ),
        // Ordinary binary dispatch: set, both vector/scalar orientations,
        // vector/vector, and a non-static scalar/scalar operand.
        (
            "binary set",
            r#"recursive_float{host="a"} or recursive_peer{host="a"}"#,
            true,
        ),
        (
            "binary vector scalar",
            r#"recursive_float{host="a"} + 2"#,
            true,
        ),
        (
            "binary scalar vector",
            r#"2 + recursive_float{host="a"}"#,
            true,
        ),
        (
            "binary vector vector",
            r#"recursive_float{host="a"} + on(host, job) recursive_peer{host="a"}"#,
            true,
        ),
        (
            "binary scalar scalar",
            r#"scalar(recursive_float{host="a"}) + scalar(recursive_peer{host="a"})"#,
            true,
        ),
        // The three native-histogram boxed-input functions cover both native
        // source families. Additional cases force recursive native offset,
        // aggregation, and binary dispatch before conversion to a scalar.
        (
            "histogram fraction custom",
            r#"histogram_fraction(0, 5, recursive_histogram{host="a"})"#,
            true,
        ),
        (
            "histogram scalar custom",
            r#"histogram_count(recursive_histogram{host="a"})"#,
            true,
        ),
        (
            "histogram quantile custom",
            r#"histogram_quantile(0.5, recursive_histogram{host="a"})"#,
            true,
        ),
        (
            "histogram fraction exponential",
            r#"histogram_fraction(0, 5, recursive_exponential{host="a"})"#,
            true,
        ),
        (
            "histogram scalar exponential",
            r#"histogram_count(recursive_exponential{host="a"})"#,
            true,
        ),
        (
            "histogram quantile exponential",
            r#"histogram_quantile(0.5, recursive_exponential{host="a"})"#,
            true,
        ),
        (
            "histogram quantile classic fallback",
            r#"histogram_quantile(0.5, recursive_classic_bucket{host="a"})"#,
            true,
        ),
        (
            "native offset",
            r#"histogram_count(recursive_histogram{host="a"} offset 1s)"#,
            true,
        ),
        (
            "native offset exponential",
            r#"histogram_count(recursive_exponential{host="a"} offset 1s)"#,
            true,
        ),
        (
            "native aggregation",
            r#"histogram_count(sum by (job) (recursive_histogram))"#,
            true,
        ),
        (
            "native aggregation exponential",
            r#"histogram_count(sum by (job) (recursive_exponential))"#,
            true,
        ),
        (
            "native binary histogram scalar",
            r#"histogram_count(recursive_histogram{host="a"} * 2)"#,
            true,
        ),
        (
            "native binary scalar histogram",
            r#"histogram_count(2 * recursive_exponential{host="a"})"#,
            true,
        ),
        (
            "native binary vector vector",
            r#"histogram_count(recursive_histogram{host="a"} + on(host, job) recursive_histogram{host="a"})"#,
            true,
        ),
        (
            "native binary set",
            r#"histogram_count(recursive_histogram{host="a"} or recursive_histogram{host="a"})"#,
            true,
        ),
        (
            "native bool comparison",
            r#"recursive_exponential{host="a"} == bool recursive_exponential{host="a"}"#,
            true,
        ),
    ];

    for (branch, query, expect_nonempty) in cases {
        let sealed = fixture.run_sealed(query);
        let live = fixture.run_live(query);
        assert_eq!(
            live.results, sealed.results,
            "recursive branch `{branch}` differed for `{query}`"
        );
        assert_eq!(
            live.semantic_fingerprint_sha256(),
            sealed.semantic_fingerprint_sha256(),
            "recursive branch `{branch}` changed the semantic fingerprint for `{query}`"
        );
        assert_eq!(
            live.portable_semantic_fingerprint_sha256(),
            sealed.portable_semantic_fingerprint_sha256(),
            "recursive branch `{branch}` changed the portable fingerprint for `{query}`"
        );
        assert_eq!(
            live.stats.matched_series, sealed.stats.matched_series,
            "recursive branch `{branch}` changed matched-series accounting for `{query}`"
        );
        assert_eq!(
            live.stats.projected_series, sealed.stats.projected_series,
            "recursive branch `{branch}` changed projected-series accounting for `{query}`"
        );
        assert_eq!(
            !live.results.is_empty(),
            expect_nonempty,
            "recursive branch `{branch}` did not exercise the expected result shape for `{query}`"
        );
    }
}
