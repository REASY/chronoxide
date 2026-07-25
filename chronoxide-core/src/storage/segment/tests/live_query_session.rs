use super::*;

use std::collections::BTreeMap;
use std::sync::{Arc, Barrier};
use std::thread;

use crate::labels::{KeyValueRef, LabelSetStore, VersionedFlatInternedLabelSetStore};
use crate::promql::canonicalize_labelset;
use crate::storage::head::{
    FloatEncoding, FrozenHeadReadView, HeadConfig, HeadReadView, IntEncoding,
    LiveSeriesCatalogBuilder, OTLP_FLAG_NO_RECORDED_VALUE, SampleKind, SampleValue, SeriesSamples,
    prometheus_stale_nan,
};
use crate::storage::live_coverage::{
    CoverageLedger, MessageSequence, RecordedSampleContribution, RecordedSampleOrder,
};

fn live_head_config() -> HeadConfig {
    HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(10))
    .with_compact_numeric_series(false)
}

struct LiveHeadBuilder {
    catalog: VersionedFlatInternedLabelSetStore,
    head: HeadBuffer,
    fragments: Vec<crate::storage::head::FrozenHeadFragment>,
}

impl LiveHeadBuilder {
    fn new() -> Self {
        Self {
            catalog: VersionedFlatInternedLabelSetStore::default(),
            head: HeadBuffer::new(live_head_config()).unwrap(),
            fragments: Vec::new(),
        }
    }

    fn series(&mut self, labels: &[(&str, &str)]) -> SeriesRef {
        let metric_name = labels
            .iter()
            .find_map(|(name, value)| (*name == METRIC_NAME_LABEL).then_some(*value))
            .unwrap_or("");
        let attributes = labels
            .iter()
            .filter_map(|(name, value)| (*name != METRIC_NAME_LABEL).then_some((*name, *value)))
            .collect::<Vec<_>>();
        let canonical = canonicalize_labelset(metric_name, &attributes);
        let labels = canonical
            .labels()
            .iter()
            .map(|label| KeyValueRef::from((label.name.as_str(), label.value.as_str())))
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

fn histogram(seed: u64) -> HistogramValue {
    HistogramValue {
        count: 3 + seed,
        sum: Some(6.0 + seed as f64),
        min: Some(1.0),
        max: Some(3.0 + seed as f64),
        metadata: TypedSampleMetadata {
            start_time_ms: Some(100),
            flags: 0,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::NotCounterReset,
        },
        explicit_bounds: vec![1.0],
        bucket_counts: vec![1, 2 + seed],
    }
}

fn exponential_histogram(seed: u64) -> ExponentialHistogramValue {
    ExponentialHistogramValue {
        count: 3 + seed,
        sum: Some(7.0 + seed as f64),
        min: Some(-1.0),
        max: Some(4.0),
        scale: 1,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(100),
            flags: 0,
            temporality: OtlpAggregationTemporality::Delta,
            reset_hint: CounterResetHint::Unknown,
        },
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![1 + seed],
        },
        negative: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![1],
        },
    }
}

fn summary(seed: u64) -> SummaryValue {
    SummaryValue {
        count: 2 + seed,
        sum: 4.0 + seed as f64,
        metadata: TypedSampleMetadata {
            start_time_ms: Some(100),
            flags: 0,
            temporality: OtlpAggregationTemporality::Cumulative,
            reset_hint: CounterResetHint::GaugeType,
        },
        quantiles: vec![SummaryQuantileValue {
            quantile: 0.5,
            value: 2.0 + seed as f64,
        }],
    }
}

fn empty_store(dir: &Path) -> SegmentStoreReader {
    SegmentStoreReader::open(dir).unwrap()
}

fn append_series_samples(
    values: &mut BTreeMap<SampleKind, Vec<(u64, SampleValue)>>,
    samples: SeriesSamples,
) {
    let (kind, samples) = match samples {
        SeriesSamples::Float { samples, .. } => (
            SampleKind::Float,
            samples
                .into_iter()
                .map(|(timestamp_ms, value)| (timestamp_ms, SampleValue::Float(value)))
                .collect::<Vec<_>>(),
        ),
        SeriesSamples::Int64 { samples, .. } => (
            SampleKind::Int64,
            samples
                .into_iter()
                .map(|(timestamp_ms, value)| (timestamp_ms, SampleValue::Int64(value)))
                .collect::<Vec<_>>(),
        ),
        SeriesSamples::Histogram { samples } => (
            SampleKind::Histogram,
            samples
                .into_iter()
                .map(|(timestamp_ms, value)| (timestamp_ms, SampleValue::Histogram(value)))
                .collect::<Vec<_>>(),
        ),
        SeriesSamples::ExponentialHistogram { samples } => (
            SampleKind::ExponentialHistogram,
            samples
                .into_iter()
                .map(|(timestamp_ms, value)| {
                    (timestamp_ms, SampleValue::ExponentialHistogram(value))
                })
                .collect::<Vec<_>>(),
        ),
        SeriesSamples::Summary { samples } => (
            SampleKind::Summary,
            samples
                .into_iter()
                .map(|(timestamp_ms, value)| (timestamp_ms, SampleValue::Summary(value)))
                .collect::<Vec<_>>(),
        ),
    };
    values.entry(kind).or_default().extend(samples);
}

fn append_chunk_samples(
    values: &mut BTreeMap<SampleKind, Vec<(u64, SampleValue)>>,
    samples: ChunkSamples,
) {
    let samples = match samples {
        ChunkSamples::Float(samples) => SeriesSamples::Float {
            encoding: FloatEncoding::Raw,
            samples,
        },
        ChunkSamples::Int64(samples) => SeriesSamples::Int64 {
            encoding: IntEncoding::Raw,
            samples,
        },
        ChunkSamples::Histogram(samples) => SeriesSamples::Histogram { samples },
        ChunkSamples::ExponentialHistogram(samples) => {
            SeriesSamples::ExponentialHistogram { samples }
        }
        ChunkSamples::Summary(samples) => SeriesSamples::Summary { samples },
    };
    append_series_samples(values, samples);
}

fn raw_semantic_ledgers(
    mut values: BTreeMap<SampleKind, Vec<(u64, SampleValue)>>,
) -> BTreeMap<SampleKind, CoverageLedger> {
    values
        .iter_mut()
        .map(|(kind, samples)| {
            samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
            let mut scratch = Vec::new();
            let ledger = samples
                .iter()
                .enumerate()
                .try_fold(CoverageLedger::empty(), |ledger, (ordinal, sample)| {
                    let contribution = RecordedSampleContribution::for_sample(
                        RecordedSampleOrder::new(
                            MessageSequence::new(1),
                            u64::try_from(ordinal).unwrap(),
                        ),
                        SeriesRef::new(0),
                        sample.0,
                        &sample.1,
                        &mut scratch,
                    )?;
                    ledger.checked_with_contribution(contribution)
                })
                .unwrap();
            (*kind, ledger)
        })
        .collect()
}

fn raw_semantic_fixture() -> BTreeMap<SampleKind, Vec<(u64, SampleValue)>> {
    let ordinary_nan = f64::from_bits(0x7ff8_0000_0000_0042);
    let mut values = BTreeMap::new();
    values.insert(
        SampleKind::Float,
        vec![
            (1_000, SampleValue::Float(prometheus_stale_nan())),
            (2_000, SampleValue::Float(ordinary_nan)),
            (3_000, SampleValue::Float(f64::INFINITY)),
            (4_000, SampleValue::Float(f64::NEG_INFINITY)),
        ],
    );
    values.insert(
        SampleKind::Histogram,
        vec![
            (
                1_000,
                SampleValue::Histogram(HistogramValue {
                    count: 3,
                    sum: Some(-1.25),
                    min: Some(-2.0),
                    max: Some(4.0),
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(500),
                        flags: 0x20,
                        temporality: OtlpAggregationTemporality::Delta,
                        reset_hint: CounterResetHint::CounterReset,
                    },
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![1, 2],
                }),
            ),
            (
                2_000,
                SampleValue::Histogram(HistogramValue {
                    count: 4,
                    sum: Some(ordinary_nan),
                    min: None,
                    max: Some(f64::INFINITY),
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(1_500),
                        flags: OTLP_FLAG_NO_RECORDED_VALUE | 0x40,
                        temporality: OtlpAggregationTemporality::Cumulative,
                        reset_hint: CounterResetHint::NotCounterReset,
                    },
                    explicit_bounds: vec![1.0],
                    bucket_counts: vec![2, 2],
                }),
            ),
        ],
    );
    values.insert(
        SampleKind::ExponentialHistogram,
        vec![
            (
                1_000,
                SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                    count: 4,
                    sum: Some(f64::INFINITY),
                    min: Some(-4.0),
                    max: Some(8.0),
                    scale: 2,
                    zero_threshold: 0.0,
                    zero_count: 1,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(250),
                        flags: 0x80,
                        temporality: OtlpAggregationTemporality::Delta,
                        reset_hint: CounterResetHint::Unknown,
                    },
                    positive: ExponentialHistogramBuckets {
                        offset: -1,
                        counts: vec![2],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![1],
                    },
                }),
            ),
            (
                2_000,
                SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                    count: 3,
                    sum: Some(f64::NEG_INFINITY),
                    min: None,
                    max: None,
                    scale: 2,
                    zero_threshold: 0.0,
                    zero_count: 0,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(1_250),
                        flags: OTLP_FLAG_NO_RECORDED_VALUE | 0x100,
                        temporality: OtlpAggregationTemporality::Cumulative,
                        reset_hint: CounterResetHint::GaugeType,
                    },
                    positive: ExponentialHistogramBuckets {
                        offset: -1,
                        counts: vec![2],
                    },
                    negative: ExponentialHistogramBuckets {
                        offset: 0,
                        counts: vec![1],
                    },
                }),
            ),
        ],
    );
    values.insert(
        SampleKind::Summary,
        vec![
            (
                1_000,
                SampleValue::Summary(SummaryValue {
                    count: 2,
                    sum: -3.5,
                    metadata: TypedSampleMetadata {
                        start_time_ms: Some(100),
                        flags: 0x200,
                        temporality: OtlpAggregationTemporality::Cumulative,
                        reset_hint: CounterResetHint::GaugeType,
                    },
                    quantiles: vec![SummaryQuantileValue {
                        quantile: 0.5,
                        value: -1.75,
                    }],
                }),
            ),
            (
                2_000,
                SampleValue::Summary(SummaryValue {
                    count: 0,
                    sum: ordinary_nan,
                    metadata: TypedSampleMetadata {
                        start_time_ms: None,
                        flags: OTLP_FLAG_NO_RECORDED_VALUE | 0x400,
                        temporality: OtlpAggregationTemporality::Unspecified,
                        reset_hint: CounterResetHint::Unknown,
                    },
                    quantiles: Vec::new(),
                }),
            ),
        ],
    );
    values
}

fn raw_fixture_series_ref(kind: SampleKind) -> SeriesRef {
    SeriesRef::new(match kind {
        SampleKind::Float => 1,
        SampleKind::Int64 => 2,
        SampleKind::Histogram => 3,
        SampleKind::ExponentialHistogram => 4,
        SampleKind::Summary => 5,
    })
}

fn record_raw_fixture(
    head: &mut HeadBuffer,
    fixture: &BTreeMap<SampleKind, Vec<(u64, SampleValue)>>,
) {
    for (kind, samples) in fixture {
        for (timestamp_ms, sample) in samples {
            assert!(
                head.record_sample_with_outcome(
                    raw_fixture_series_ref(*kind),
                    *timestamp_ms,
                    sample.clone(),
                )
                .unwrap()
                .recorded
            );
        }
    }
}

#[test]
fn raw_semantics_match_unfrozen_immutable_live_and_schema8_sealed_storage() {
    let fixture = raw_semantic_fixture();
    let expected = raw_semantic_ledgers(fixture.clone());

    let mut unfrozen = HeadBuffer::new(live_head_config()).unwrap();
    record_raw_fixture(&mut unfrozen, &fixture);
    let mut unfrozen_values = BTreeMap::new();
    for window in unfrozen.drain_windows() {
        for (_series, samples) in window.into_series_samples().unwrap() {
            append_series_samples(&mut unfrozen_values, samples);
        }
    }
    assert_eq!(
        raw_semantic_ledgers(unfrozen_values),
        expected,
        "the mutable reference head changed exact raw semantics"
    );

    let mut publishing = HeadBuffer::new(live_head_config()).unwrap();
    record_raw_fixture(&mut publishing, &fixture);
    let fragments = publishing.try_freeze_for_publication().unwrap();
    let mut immutable_values = BTreeMap::new();
    for fragment in &fragments {
        for (_series, samples) in fragment.series_samples_in_range(0, 10_000).unwrap() {
            append_series_samples(&mut immutable_values, samples);
        }
    }
    assert_eq!(
        raw_semantic_ledgers(immutable_values),
        expected,
        "freezing into immutable live pages changed stale/NaN bits or typed metadata"
    );

    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(
        SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_storage_schema(SegmentStorageSchema::Schema8),
    )
    .unwrap();
    for (kind, samples) in &fixture {
        let metric_name = match kind {
            SampleKind::Float => "raw.float",
            SampleKind::Int64 => "raw.int",
            SampleKind::Histogram => "raw.histogram",
            SampleKind::ExponentialHistogram => "raw.exponential",
            SampleKind::Summary => "raw.summary",
        };
        match kind {
            SampleKind::Float => {
                let samples = samples
                    .iter()
                    .map(|(timestamp_ms, value)| {
                        let SampleValue::Float(value) = value else {
                            unreachable!("fixture kind and value disagree");
                        };
                        (*timestamp_ms, *value)
                    })
                    .collect::<Vec<_>>();
                writer
                    .record_samples_raw_ordered_with_label_visitor(
                        raw_fixture_series_ref(*kind),
                        &samples,
                        |visit| visit(METRIC_NAME_LABEL, metric_name),
                    )
                    .unwrap();
            }
            SampleKind::Int64 => unreachable!("the fixture has no integer series"),
            SampleKind::Histogram => {
                let samples = samples
                    .iter()
                    .map(|(timestamp_ms, value)| {
                        let SampleValue::Histogram(value) = value else {
                            unreachable!("fixture kind and value disagree");
                        };
                        (*timestamp_ms, value.clone())
                    })
                    .collect::<Vec<_>>();
                writer
                    .record_histogram_samples_ordered_with_label_visitor(
                        raw_fixture_series_ref(*kind),
                        &samples,
                        |visit| visit(METRIC_NAME_LABEL, metric_name),
                    )
                    .unwrap();
            }
            SampleKind::ExponentialHistogram => {
                let samples = samples
                    .iter()
                    .map(|(timestamp_ms, value)| {
                        let SampleValue::ExponentialHistogram(value) = value else {
                            unreachable!("fixture kind and value disagree");
                        };
                        (*timestamp_ms, value.clone())
                    })
                    .collect::<Vec<_>>();
                writer
                    .record_exponential_histogram_samples_ordered_with_label_visitor(
                        raw_fixture_series_ref(*kind),
                        &samples,
                        |visit| visit(METRIC_NAME_LABEL, metric_name),
                    )
                    .unwrap();
            }
            SampleKind::Summary => {
                let samples = samples
                    .iter()
                    .map(|(timestamp_ms, value)| {
                        let SampleValue::Summary(value) = value else {
                            unreachable!("fixture kind and value disagree");
                        };
                        (*timestamp_ms, value.clone())
                    })
                    .collect::<Vec<_>>();
                writer
                    .record_summary_samples_ordered_with_label_visitor(
                        raw_fixture_series_ref(*kind),
                        &samples,
                        |visit| visit(METRIC_NAME_LABEL, metric_name),
                    )
                    .unwrap();
            }
        }
    }
    writer.flush().unwrap();

    let segment_dir = fs::read_dir(tempdir.path())
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().starts_with("seg-"))
        })
        .unwrap();
    let reader = SegmentReader::open(segment_dir).unwrap();
    let mut chunks = ChunkReader::new(reader.open_chunks().unwrap());
    let mut sealed_values = BTreeMap::new();
    while let Some(chunk) = chunks.read_next().unwrap() {
        append_chunk_samples(&mut sealed_values, chunk.samples);
    }
    assert_eq!(
        raw_semantic_ledgers(sealed_values),
        expected,
        "Schema 8 changed exact stale/ordinary-NaN bits, reset hints, start times, temporality, flags, or signed/non-finite values"
    );
}

#[test]
fn head_read_view_rejects_a_catalog_older_than_its_samples() {
    let mut head = HeadBuffer::new(live_head_config()).unwrap();
    head.record_sample(SeriesRef::new(0), 1_000, SampleValue::Float(1.0))
        .unwrap();
    let fragments = head.try_freeze_for_publication().unwrap();
    let mut empty_catalog = VersionedFlatInternedLabelSetStore::default();
    let labels = Arc::new(empty_catalog.snapshot().unwrap());

    let error =
        HeadReadView::new(Arc::new(FrozenHeadReadView::from_owned(fragments)), labels).unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidData);
    assert!(error.to_string().contains("requires catalog revision 1"));
}

#[test]
fn head_only_session_supports_selectors_metadata_and_all_generic_kinds() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = empty_store(tempdir.path());
    let mut builder = LiveHeadBuilder::new();
    let float = builder.series(&[
        (METRIC_NAME_LABEL, "cpu.session"),
        ("host", "a"),
        ("zone", "east"),
    ]);
    let int = builder.series(&[(METRIC_NAME_LABEL, "cpu.session"), ("host", "b")]);
    let summary_series =
        builder.series(&[(METRIC_NAME_LABEL, "request.summary"), ("host", "summary")]);
    builder.record(float, 1_000, SampleValue::Float(1.5));
    builder.record(int, 1_100, SampleValue::Int64(-7));
    builder.record(summary_series, 1_200, SampleValue::Summary(summary(0)));
    let head = builder.finish();
    let mut session = store.query_session_with_head_view(&head).unwrap();

    let exact = session
        .query_selector(&SegmentSelector::metric("cpu.session"), 0, 2_000)
        .unwrap();
    assert_eq!(exact.len(), 2);
    let mut exact_samples = exact
        .iter()
        .flat_map(|result| result.samples.iter().copied())
        .collect::<Vec<_>>();
    exact_samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
    assert_eq!(exact_samples, vec![(1_000, 1.5), (1_100, -7.0)]);

    let negative = session
        .query_selector(
            &SegmentSelector::new(vec![LabelMatcher::not_eq("zone", "east")]),
            0,
            2_000,
        )
        .unwrap();
    assert_eq!(negative.len(), 1, "missing labels satisfy != matchers");
    assert!(negative.iter().all(|result| {
        result
            .labels
            .pairs()
            .all(|(name, value)| name != "host" || value != "a")
    }));

    let regex = session
        .query_selector(
            &SegmentSelector::new(vec![LabelMatcher::regex("host", "a|b")]),
            0,
            2_000,
        )
        .unwrap();
    assert_eq!(regex.len(), 2);

    let summary_sum = session
        .query_promql("request.summary_sum", 0, 2_000)
        .unwrap();
    assert_eq!(summary_sum.len(), 1);
    assert_eq!(summary_sum[0].samples, vec![(1_200, 4.0)]);

    assert_eq!(
        session.metric_names(0, 2_000).unwrap(),
        vec![
            normalize_metric_name("cpu.session"),
            normalize_metric_name("request.summary")
        ]
    );
    assert_eq!(
        session.label_values("host", 0, 2_000).unwrap(),
        vec!["a", "b", "summary"]
    );
    assert_eq!(
        session.label_names(0, 2_000).unwrap(),
        vec![METRIC_NAME_LABEL, "host", "zone"]
    );
}

#[test]
fn sealed_and_two_head_fragments_merge_once_with_head_last_write_wins() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_samples_ordered_with_label_visitor(SeriesRef::new(77), &[(1_000, 1.0)], |visit| {
            visit(METRIC_NAME_LABEL, "handoff.metric");
            visit("host", "a");
        })
        .unwrap();
    writer.flush().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let mut builder = LiveHeadBuilder::new();
    let series = builder.series(&[(METRIC_NAME_LABEL, "handoff.metric"), ("host", "a")]);
    builder.record(series, 1_000, SampleValue::Float(2.0));
    builder.publish();
    builder.record(series, 1_000, SampleValue::Float(3.0));
    builder.publish();
    let head = builder.finish();

    let selector = SegmentSelector::metric("handoff.metric");
    let mut normal = store.query_session_with_head_view(&head).unwrap();
    let normal = normal.query_selector(&selector, 0, 2_000).unwrap();
    let mut cross = store.query_session_with_head_view(&head).unwrap();
    cross.set_experimental_cross_segment_chunk_reads(true);
    let cross = cross.query_selector(&selector, 0, 2_000).unwrap();

    assert_eq!(cross, normal);
    assert_eq!(normal.len(), 1);
    assert_eq!(normal[0].samples, vec![(1_000, 3.0)]);

    let mut limited = store.query_session_with_head_view(&head).unwrap();
    let limited = limited
        .query_selector_with_limits(
            &selector,
            0,
            2_000,
            QueryLimits {
                max_matched_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap();
    assert_eq!(limited.results, normal);
    assert_eq!(
        limited.stats.matched_series, 1,
        "one logical series must be charged once across sealed and head suppliers"
    );
}

#[test]
fn native_histogram_head_flows_match_normal_and_cross_segment_execution() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(10),
            &[(1_000, histogram(0))],
            |visit| {
                visit(METRIC_NAME_LABEL, "native.histogram");
                visit("route", "/a");
            },
        )
        .unwrap();
    writer
        .record_exponential_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(11),
            &[(1_000, exponential_histogram(0))],
            |visit| {
                visit(METRIC_NAME_LABEL, "native.exponential");
                visit("route", "/a");
            },
        )
        .unwrap();
    writer.flush().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let mut builder = LiveHeadBuilder::new();
    let histogram_series =
        builder.series(&[(METRIC_NAME_LABEL, "native.histogram"), ("route", "/a")]);
    let exponential_series =
        builder.series(&[(METRIC_NAME_LABEL, "native.exponential"), ("route", "/a")]);
    builder.record(
        histogram_series,
        2_000,
        SampleValue::Histogram(histogram(1)),
    );
    builder.publish();
    builder.record(
        exponential_series,
        2_000,
        SampleValue::ExponentialHistogram(exponential_histogram(1)),
    );
    let head = builder.finish();

    let histogram_selector = SegmentSelector::metric("native.histogram");
    let exponential_selector = SegmentSelector::metric("native.exponential");
    let query = |cross_segment| {
        let mut session = store.query_session_with_head_view(&head).unwrap();
        session.set_experimental_cross_segment_chunk_reads(cross_segment);
        let histogram = session
            .query_native_histogram_selector_with_limits(
                &histogram_selector,
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        let exponential = session
            .query_native_exponential_histogram_selector_with_limits(
                &exponential_selector,
                0,
                3_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        (histogram, exponential)
    };

    let normal = query(false);
    let cross = query(true);
    assert_eq!(cross, normal);
    assert_eq!(normal.0.0.len(), 1);
    assert_eq!(normal.0.0[0].samples.len(), 2);
    assert_eq!(normal.0.0[0].samples[1].timestamp_ms, 2_000);
    assert_eq!(normal.0.0[0].samples[1].start_time_ms, None);
    assert_eq!(
        normal.0.0[0].samples[1].temporality,
        OtlpAggregationTemporality::Cumulative
    );
    assert_eq!(normal.1.0.len(), 1);
    assert_eq!(normal.1.0[0].samples.len(), 2);
    assert_eq!(normal.1.0[0].samples[1].timestamp_ms, 2_000);
    assert_eq!(normal.1.0[0].samples[1].start_time_ms, Some(100));
    assert_eq!(
        normal.1.0[0].samples[1].temporality,
        OtlpAggregationTemporality::Delta
    );
}

#[test]
fn head_session_enforces_union_budget_and_rejects_one_pass_range_mode() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = empty_store(tempdir.path());
    let mut builder = LiveHeadBuilder::new();
    let first = builder.series(&[(METRIC_NAME_LABEL, "budget.metric"), ("host", "a")]);
    let second = builder.series(&[(METRIC_NAME_LABEL, "budget.metric"), ("host", "b")]);
    builder.record(first, 1_000, SampleValue::Float(1.0));
    builder.record(second, 1_000, SampleValue::Float(2.0));
    let head = builder.finish();

    let mut session = store.query_session_with_head_view(&head).unwrap();
    let error = session
        .query_selector_with_limits(
            &SegmentSelector::metric("budget.metric"),
            0,
            2_000,
            QueryLimits {
                max_matched_series: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("matched_series"));

    let mut session = store.query_session_with_head_view(&head).unwrap();
    let error = session
        .query_selector_with_limits(
            &SegmentSelector::new(vec![LabelMatcher::regex("host", ".*")]),
            0,
            2_000,
            QueryLimits {
                max_regex_values_examined: Some(1),
                ..QueryLimits::unlimited()
            },
        )
        .unwrap_err();
    assert!(error.to_string().contains("regex_values_examined"));

    let mut session = store.query_session_with_head_view(&head).unwrap();
    let error = session
        .set_range_execution_mode(RangeExecutionMode::OnePassAssumeScalar)
        .unwrap_err();
    assert!(error.to_string().contains("non-empty head"));
    assert_eq!(session.range_execution_mode(), RangeExecutionMode::Repeated);
}

#[test]
fn range_scalar_cache_remains_sealed_only_when_a_head_is_attached() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    writer
        .record_histogram_samples_ordered_with_label_visitor(
            SeriesRef::new(1),
            &[(1_000, histogram(0))],
            |visit| visit(METRIC_NAME_LABEL, "cache.live"),
        )
        .unwrap();
    writer.flush().unwrap();
    let store = SegmentStoreReader::open(tempdir.path()).unwrap();

    let mut builder = LiveHeadBuilder::new();
    let series = builder.series(&[(METRIC_NAME_LABEL, "cache.live")]);
    builder.record(series, 2_000, SampleValue::Histogram(histogram(1)));
    let head = builder.finish();

    let run = |cache_budget_bytes| {
        let mut session = store.query_session_with_head_view(&head).unwrap();
        session
            .set_range_scalar_cache_budget_bytes(cache_budget_bytes)
            .unwrap();
        let execution = session
            .query_promql_range_with_limits(
                "cache.live_count",
                1_000,
                2_000,
                1_000,
                QueryLimits::unlimited(),
            )
            .unwrap();
        (
            execution,
            session.last_range_scalar_cache_summary().copied().unwrap(),
        )
    };

    let (uncached, uncached_summary) = run(0);
    let (cached, cached_summary) = run(1024 * 1024);
    assert_eq!(cached, uncached);
    assert_eq!(cached.results.len(), 1);
    assert_eq!(cached.results[0].samples, vec![(1_000, 3.0), (2_000, 4.0)]);
    assert_eq!(uncached_summary.admitted_entries, 0);
    assert!(cached_summary.admitted_entries > 0, "{cached_summary:?}");
    assert!(
        cached_summary.hits > 0,
        "the sealed chunk should be reusable across range steps: {cached_summary:?}"
    );
}

#[test]
fn head_results_honor_all_query_label_storage_and_materialization_policies() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = empty_store(tempdir.path());
    let mut builder = LiveHeadBuilder::new();
    let series = builder.series(&[
        (METRIC_NAME_LABEL, "labels.metric"),
        ("host", "a"),
        ("unused", "large"),
    ]);
    builder.record(series, 1_000, SampleValue::Float(2.0));
    let head = builder.finish();

    for policy in [
        QueryLabelStoragePolicy::OwnedStrings,
        QueryLabelStoragePolicy::SharedAtoms,
        QueryLabelStoragePolicy::CompactIds,
    ] {
        let mut session = store.query_session_with_head_view(&head).unwrap();
        session.set_query_label_storage_policy(policy).unwrap();
        let results = session
            .query_selector(&SegmentSelector::metric("labels.metric"), 0, 2_000)
            .unwrap();
        assert_eq!(results.len(), 1);
        match policy {
            QueryLabelStoragePolicy::OwnedStrings => {
                assert!(!results[0].labels.uses_shared_atoms());
                assert!(!results[0].labels.uses_compact_ids());
            }
            QueryLabelStoragePolicy::SharedAtoms => {
                assert!(results[0].labels.uses_shared_atoms());
            }
            QueryLabelStoragePolicy::CompactIds => {
                assert!(results[0].labels.uses_compact_ids());
            }
        }
    }

    let query = "sum by (host)(labels.metric)";
    let mut demand = store.query_session_with_head_view(&head).unwrap();
    demand.set_label_materialization_policy(QueryLabelMaterializationPolicy::DemandDriven);
    let demand = demand.query_promql(query, 0, 2_000).unwrap();
    let mut full = store.query_session_with_head_view(&head).unwrap();
    full.set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
    let full = full.query_promql(query, 0, 2_000).unwrap();
    assert_eq!(demand, full);
    assert_eq!(
        demand[0].labels.to_vec(),
        vec![("host".to_string(), "a".to_string())]
    );
}

#[test]
fn immutable_head_sessions_are_safe_for_parallel_queries() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = Arc::new(empty_store(tempdir.path()));
    let mut builder = LiveHeadBuilder::new();
    let series = builder.series(&[(METRIC_NAME_LABEL, "parallel.metric"), ("worker", "one")]);
    for publication in 0..4 {
        builder.record(
            series,
            1_000 + publication,
            SampleValue::Float(publication as f64),
        );
        builder.publish();
    }
    let head = Arc::new(builder.finish());
    let barrier = Arc::new(Barrier::new(9));
    let threads = (0..8)
        .map(|_| {
            let store = Arc::clone(&store);
            let head = Arc::clone(&head);
            let barrier = Arc::clone(&barrier);
            thread::spawn(move || {
                barrier.wait();
                for _ in 0..100 {
                    let mut session = store.query_session_with_head_view(&head).unwrap();
                    let results = session
                        .query_selector(&SegmentSelector::metric("parallel.metric"), 0, 2_000)
                        .unwrap();
                    assert_eq!(results.len(), 1);
                    assert_eq!(
                        results[0].samples,
                        vec![(1_000, 0.0), (1_001, 1.0), (1_002, 2.0), (1_003, 3.0)]
                    );
                }
            })
        })
        .collect::<Vec<_>>();
    barrier.wait();
    for worker in threads {
        worker.join().unwrap();
    }
}

#[test]
fn catalog_only_rows_do_not_leak_into_session_metadata() {
    let tempdir = tempfile::tempdir().unwrap();
    let store = empty_store(tempdir.path());
    let mut builder = LiveHeadBuilder::new();
    let visible = builder.series(&[(METRIC_NAME_LABEL, "visible.metric"), ("host", "visible")]);
    let _catalog_only = builder.series(&[(METRIC_NAME_LABEL, "ghost.metric"), ("host", "ghost")]);
    builder.record(visible, 1_000, SampleValue::Float(1.0));
    let head = builder.finish();
    let session = store.query_session_with_head_view(&head).unwrap();

    assert_eq!(
        session.metric_names(0, 2_000).unwrap(),
        vec![normalize_metric_name("visible.metric")]
    );
    assert_eq!(
        session.label_values("host", 0, 2_000).unwrap(),
        vec!["visible"]
    );
}
