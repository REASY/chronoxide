use super::*;
use crate::labels::{DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore};
use crate::storage::arena::BlockArena;
use crate::storage::block::{
    BlockBuilder, BlockCodec, FloatChimp128DuckDBDeferredCodec, FloatGorillaCodec, FloatRawCodec,
    IntDeltaCodec, IntRawCodec,
};
use crate::storage::encoding::chimp::Chimp128DuckDBEncoder;
use crate::storage::encoding::{GorillaEncoder, encode_varint, encode_zigzag_i64};
use crate::storage::segment::{LabelMatcher, QueryLimits};

fn labels(
    store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    values: &[(&str, &str)],
) -> SeriesRef {
    let refs: Vec<_> = values.iter().copied().map(KeyValueRef::from).collect();
    store.intern(&refs).unwrap()
}

#[test]
fn head_buffer_rotates_windows() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    let flushed = head
        .record_sample(SeriesRef::new(1), 1_000, SampleValue::Float(1.0))
        .unwrap();
    assert!(flushed.is_none());
    let flushed = head
        .record_sample(SeriesRef::new(1), 15_000, SampleValue::Float(2.0))
        .unwrap();
    let mut flushed = flushed.expect("expected window flush");
    assert_eq!(flushed.start_ms, 0);
    assert_eq!(flushed.end_ms, 10_000);
    assert_eq!(flushed.datapoints, 1);

    let encoded = flushed.series.remove(&SeriesRef::new(1)).unwrap();
    let samples = encoded.into_samples(&flushed.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Float {
            encoding: FloatEncoding::Gorilla,
            samples: vec![(1_000, 1.0)]
        }
    );

    let current = head.window_range().unwrap();
    assert_eq!(current, (10_000, 20_000));
}

#[test]
fn head_buffer_groups_series() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 2_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(2), 3_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 4_000, SampleValue::Float(3.0))
        .unwrap();

    let mut window = head.drain().unwrap();
    assert_eq!(window.datapoints, 3);
    assert_eq!(window.series.len(), 2);

    let series1 = window.series.remove(&SeriesRef::new(1)).unwrap();
    let series1_samples = series1.into_samples(&window.arena).unwrap();
    let SeriesSamples::Float {
        encoding,
        samples: series1_samples,
    } = series1_samples
    else {
        panic!("expected float samples");
    };
    assert_eq!(encoding, FloatEncoding::Gorilla);
    assert_eq!(series1_samples.len(), 2);
    assert_eq!(series1_samples[0], (2_000, 1.0));
    assert_eq!(series1_samples[1], (4_000, 3.0));

    let series2 = window.series.remove(&SeriesRef::new(2)).unwrap();
    let series2_samples = series2.into_samples(&window.arena).unwrap();
    let SeriesSamples::Float {
        encoding,
        samples: series2_samples,
    } = series2_samples
    else {
        panic!("expected float samples");
    };
    assert_eq!(encoding, FloatEncoding::Gorilla);
    assert_eq!(series2_samples.len(), 1);
    assert_eq!(series2_samples[0], (3_000, 2.0));
}

#[test]
fn head_buffer_out_of_order_default_zero_window_rejects_late_sample() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 5_000, SampleValue::Float(1.0))
        .unwrap();
    let err = head
        .record_sample(SeriesRef::new(1), 4_999, SampleValue::Float(2.0))
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn head_buffer_out_of_order_accepts_sample_within_configured_window() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(2));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 3_500, SampleValue::Float(2.0))
        .unwrap();

    let mut windows = head.drain_windows();
    assert_eq!(windows.len(), 2);
    let mut samples = Vec::new();
    for window in &mut windows {
        assert_eq!((window.start_ms, window.end_ms), (0, 10_000));
        let SeriesSamples::Float {
            encoding,
            samples: window_samples,
        } = window
            .series
            .remove(&SeriesRef::new(1))
            .unwrap()
            .into_samples(&window.arena)
            .unwrap()
        else {
            panic!("expected float samples");
        };
        assert_eq!(encoding, FloatEncoding::Raw);
        samples.extend(window_samples);
    }
    samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);

    assert_eq!(samples, vec![(3_500, 2.0), (5_000, 1.0)]);
}

#[test]
fn head_buffer_out_of_order_rejects_sample_older_than_configured_window() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(2));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 5_000, SampleValue::Float(1.0))
        .unwrap();
    let err = head
        .record_sample(SeriesRef::new(1), 2_999, SampleValue::Float(2.0))
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn head_buffer_out_of_order_policy_is_per_series() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(2), 1_000, SampleValue::Float(2.0))
        .unwrap();

    let err = head
        .record_sample(SeriesRef::new(1), 4_999, SampleValue::Float(3.0))
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
}

#[test]
fn head_buffer_routes_late_samples_to_ooo_window_without_rotating_active() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    let mut head = HeadBuffer::new(config).unwrap();

    let flushed = head
        .record_sample(SeriesRef::new(1), 15_000, SampleValue::Float(1.0))
        .unwrap();
    assert!(flushed.is_none());
    assert_eq!(head.window_range(), Some((10_000, 20_000)));

    let flushed = head
        .record_sample(SeriesRef::new(1), 9_500, SampleValue::Float(2.0))
        .unwrap();
    assert!(flushed.is_none());
    assert_eq!(head.window_range(), Some((10_000, 20_000)));

    let mut windows = head.drain_windows();
    assert_eq!(windows.len(), 2);
    windows.sort_by_key(|window| window.start_ms);

    assert_eq!((windows[0].start_ms, windows[0].end_ms), (0, 10_000));
    let ooo_samples = windows[0]
        .series
        .remove(&SeriesRef::new(1))
        .unwrap()
        .into_samples(&windows[0].arena)
        .unwrap();
    assert_eq!(
        ooo_samples,
        SeriesSamples::Float {
            encoding: FloatEncoding::Raw,
            samples: vec![(9_500, 2.0)]
        }
    );

    assert_eq!((windows[1].start_ms, windows[1].end_ms), (10_000, 20_000));
    let active_samples = windows[1]
        .series
        .remove(&SeriesRef::new(1))
        .unwrap()
        .into_samples(&windows[1].arena)
        .unwrap();
    assert_eq!(
        active_samples,
        SeriesSamples::Float {
            encoding: FloatEncoding::Raw,
            samples: vec![(15_000, 1.0)]
        }
    );
}

#[test]
fn head_query_merges_active_and_ooo_windows_before_flush() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(series, 15_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series, 9_500, SampleValue::Float(2.0))
        .unwrap();

    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = head
        .query_selector(&label_store, &selector, 0, 20_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(9_500, 2.0), (15_000, 1.0)]);
}

#[test]
fn head_query_dedupes_duplicate_timestamps_with_active_last_write() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let mut head = HeadBuffer::new(HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    ))
    .unwrap();

    head.record_sample(series, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series, 5_000, SampleValue::Float(2.0))
        .unwrap();

    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(5_000, 2.0)]);
}

#[test]
fn head_query_dedupes_duplicate_timestamps_with_ooo_last_write() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(2));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(series, 4_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(series, 4_000, SampleValue::Float(3.0))
        .unwrap();

    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].samples, vec![(4_000, 3.0), (5_000, 2.0)]);
}

#[test]
fn head_metadata_includes_ooo_only_series_before_flush() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-2")],
    );
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(10));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(series, 25_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(series, 15_000, SampleValue::Float(2.0))
        .unwrap();

    let values = head
        .label_values(&label_store, "pod.name", 10_000, 19_000)
        .unwrap();

    assert_eq!(values, vec!["backend-2"]);
}

#[test]
fn head_window_blocks_and_range_decode() {
    let config = HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 1_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 2_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 9_000, SampleValue::Float(3.0))
        .unwrap();

    let window = head.drain().unwrap();
    let series = window.series.get(&SeriesRef::new(1)).unwrap();
    let EncodedSeries::FloatGorilla(series) = series else {
        panic!("expected gorilla float series");
    };
    assert_eq!(series.blocks.len(), 2);

    let in_range = window.series_samples_in_range(1_500, 5_000).unwrap();
    assert_eq!(in_range.len(), 1);
    assert_eq!(in_range[0].0, SeriesRef::new(1));
    assert_eq!(
        in_range[0].1,
        SeriesSamples::Float {
            encoding: FloatEncoding::Gorilla,
            samples: vec![(2_000, 2.0)]
        }
    );
}

#[test]
fn head_window_block_stats_include_current_block() {
    let config = HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 1_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 2_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 3_000, SampleValue::Float(3.0))
        .unwrap();

    let window = head.window.as_ref().unwrap();
    let block_counts: Vec<usize> = window.series_block_counts().collect();
    assert_eq!(block_counts, vec![2]);

    let mut samples_per_block = Vec::new();
    window.for_each_block_sample(|count| samples_per_block.push(count));
    assert_eq!(samples_per_block, vec![2, 1]);
}

#[test]
fn head_window_block_stats_sealed_multi_series() {
    let config = HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    for (idx, ts_ms) in [1_000, 2_000, 3_000, 4_000, 5_000].iter().enumerate() {
        let value = *ts_ms as f64 + idx as f64;
        head.record_sample(SeriesRef::new(1), *ts_ms, SampleValue::Float(value))
            .unwrap();
    }
    head.record_sample(SeriesRef::new(2), 1_500, SampleValue::Float(10.0))
        .unwrap();

    let window = head.drain().unwrap();
    let mut block_counts: Vec<usize> = window.series_block_counts().collect();
    block_counts.sort_unstable();
    assert_eq!(block_counts, vec![1, 3]);

    let mut samples_per_block = Vec::new();
    window.for_each_block_sample(|count| samples_per_block.push(count));
    samples_per_block.sort_unstable();
    assert_eq!(samples_per_block, vec![1, 1, 2, 2]);
}

#[test]
fn head_window_block_stats_empty_window() {
    let window = HeadWindow {
        start_ms: 0,
        end_ms: 10_000,
        series: HashMap::new(),
        datapoints: 0,
        arena: BlockArena::new(DEFAULT_HEAD_ARENA_PAGE_BYTES),
    };

    assert_eq!(window.series_block_counts().count(), 0);
    let mut called = false;
    window.for_each_block_sample(|_| called = true);
    assert!(!called);
}

#[test]
fn head_selector_index_resolves_exact_and_negative_matchers() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend_1 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let backend_2 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-2")],
    );
    let missing_pod = labels(&mut label_store, &[(METRIC_NAME_LABEL, "cpu.usage")]);

    let mut head = HeadBuffer::new(HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap();
    head.record_sample(backend_1, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(backend_2, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(missing_pod, 5_000, SampleValue::Float(3.0))
        .unwrap();

    let index = HeadSelectorIndex::build(head.window.as_ref().unwrap(), &label_store).unwrap();
    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![LabelMatcher::not_eq("pod.name", "backend-1")],
    );
    let mut budget = QueryBudget::unlimited();
    let matches = index
        .matching_series(&selector.normalized_matchers(), &mut budget, false)
        .unwrap();

    assert_eq!(matches, vec![backend_2, missing_pod]);
}

#[test]
fn head_selector_index_resolves_regex_matchers_and_counts_value_expansion() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend_1 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );
    let backend_2 = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-2")],
    );
    let frontend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "frontend-1")],
    );

    let mut head = HeadBuffer::new(HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap();
    head.record_sample(backend_1, 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(backend_2, 5_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(frontend, 5_000, SampleValue::Float(3.0))
        .unwrap();

    let index = HeadSelectorIndex::build(head.window.as_ref().unwrap(), &label_store).unwrap();
    let selector = SegmentSelector::with_metric(
        "cpu.usage",
        vec![LabelMatcher::regex("pod.name", "backend-[12]")],
    );
    let mut budget = QueryBudget::new(QueryLimits {
        max_regex_values_examined: Some(3),
        ..QueryLimits::unlimited()
    });
    let matches = index
        .matching_series(&selector.normalized_matchers(), &mut budget, false)
        .unwrap();

    assert_eq!(matches, vec![backend_1, backend_2]);
    assert_eq!(budget.stats().regex_values_examined, 3);
}

#[test]
fn head_query_populates_and_invalidates_selector_index_cache() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let backend = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
    );

    let mut head = HeadBuffer::new(HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    ))
    .unwrap();
    head.record_sample(backend, 5_000, SampleValue::Float(1.0))
        .unwrap();

    let selector =
        SegmentSelector::with_metric("cpu.usage", vec![LabelMatcher::eq("pod.name", "backend-1")]);
    let results = head
        .query_selector(&label_store, &selector, 0, 10_000)
        .unwrap();
    assert_eq!(results.len(), 1);
    assert!(head.selector_index.lock().unwrap().is_some());

    head.record_sample(backend, 6_000, SampleValue::Float(2.0))
        .unwrap();
    assert!(head.selector_index.lock().unwrap().is_none());
}

#[test]
fn head_buffer_int_series_roundtrip() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(7), 1_000, SampleValue::Int64(5))
        .unwrap();
    head.record_sample(SeriesRef::new(7), 2_000, SampleValue::Int64(-3))
        .unwrap();

    let mut window = head.drain().unwrap();
    let series = window.series.remove(&SeriesRef::new(7)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Int64 {
            encoding: IntEncoding::DeltaZigZag,
            samples: vec![(1_000, 5), (2_000, -3)]
        }
    );
}

#[test]
fn head_buffer_histogram_roundtrip() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    let value = HistogramValue {
        count: 5,
        sum: Some(12.5),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 2.0, 3.0],
        bucket_counts: vec![1, 2, 2, 0],
    };

    head.record_sample(
        SeriesRef::new(11),
        1_000,
        SampleValue::Histogram(value.clone()),
    )
    .unwrap();

    let mut window = head.drain().unwrap();
    let series = window.series.remove(&SeriesRef::new(11)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Histogram {
            samples: vec![(1_000, value)]
        }
    );
}

#[test]
fn head_buffer_exponential_histogram_roundtrip() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    let value = ExponentialHistogramValue {
        count: 10,
        sum: Some(42.0),
        min: None,
        max: Some(9.0),
        scale: -2,
        zero_threshold: 0.0,
        zero_count: 3,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: 1,
            counts: vec![1, 2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![4],
        },
    };

    head.record_sample(
        SeriesRef::new(12),
        2_000,
        SampleValue::ExponentialHistogram(value.clone()),
    )
    .unwrap();

    let mut window = head.drain().unwrap();
    let series = window.series.remove(&SeriesRef::new(12)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::ExponentialHistogram {
            samples: vec![(2_000, value)]
        }
    );
}

#[test]
fn head_buffer_summary_roundtrip() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    let value = SummaryValue {
        count: 9,
        sum: 18.0,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![
            SummaryQuantileValue {
                quantile: 0.5,
                value: 2.0,
            },
            SummaryQuantileValue {
                quantile: 0.9,
                value: 4.0,
            },
        ],
    };

    head.record_sample(
        SeriesRef::new(13),
        3_000,
        SampleValue::Summary(value.clone()),
    )
    .unwrap();

    let mut window = head.drain().unwrap();
    let series = window.series.remove(&SeriesRef::new(13)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Summary {
            samples: vec![(3_000, value)]
        }
    );
}

#[test]
fn head_buffer_histogram_schema_roundtrip() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_varlen_encoding(VarLenEncodingKind::Schema);
    let mut head = HeadBuffer::new(config).unwrap();

    let value = HistogramValue {
        count: 5,
        sum: Some(12.5),
        min: Some(1.0),
        max: Some(4.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0, 2.0, 3.0],
        bucket_counts: vec![1, 2, 2, 0],
    };

    head.record_sample(
        SeriesRef::new(21),
        1_000,
        SampleValue::Histogram(value.clone()),
    )
    .unwrap();

    let mut window = head.drain().unwrap();
    let series = window.series.remove(&SeriesRef::new(21)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Histogram {
            samples: vec![(1_000, value)]
        }
    );
}

#[test]
fn head_buffer_exponential_histogram_schema_roundtrip() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_varlen_encoding(VarLenEncodingKind::Schema);
    let mut head = HeadBuffer::new(config).unwrap();

    let value = ExponentialHistogramValue {
        count: 10,
        sum: Some(42.0),
        min: None,
        max: Some(9.0),
        scale: -2,
        zero_threshold: 0.0,
        zero_count: 3,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: 1,
            counts: vec![1, 2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![4],
        },
    };

    head.record_sample(
        SeriesRef::new(22),
        2_000,
        SampleValue::ExponentialHistogram(value.clone()),
    )
    .unwrap();

    let mut window = head.drain().unwrap();
    let series = window.series.remove(&SeriesRef::new(22)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::ExponentialHistogram {
            samples: vec![(2_000, value)]
        }
    );
}

#[test]
fn exponential_histogram_schema_encoding_does_not_churn_on_bucket_span_length() {
    let first = ExponentialHistogramValue {
        count: 1,
        sum: Some(1.0),
        min: None,
        max: None,
        scale: 2,
        zero_threshold: 0.125,
        zero_count: 0,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![1],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    };
    let second = ExponentialHistogramValue {
        count: 15,
        sum: Some(15.0),
        min: None,
        max: Some(8.0),
        scale: 2,
        zero_threshold: 0.125,
        zero_count: 0,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![1, 2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: -3,
            counts: vec![4, 5],
        },
    };

    let mut codec = ExponentialHistogramSchemaCodec::new(first.clone()).unwrap();
    codec.push(second.clone()).unwrap();

    let bytes = codec.snapshot_bytes();
    let mut cursor = 0;
    let schema_count = decode_varint(&bytes, &mut cursor).unwrap();
    assert_eq!(schema_count, 1);

    let decoded = ExponentialHistogramSchemaCodec::decode_values(&bytes, 2).unwrap();
    assert_eq!(decoded, vec![first, second]);
}

#[test]
fn exponential_histogram_projected_bucket_count_uses_bucket_upper_bounds() {
    let value = ExponentialHistogramValue {
        count: 9,
        sum: None,
        min: None,
        max: None,
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![4],
        },
    };

    assert_eq!(
        exponential_histogram_projected_bucket_count(&value, -1.0),
        4
    );
    assert_eq!(exponential_histogram_projected_bucket_count(&value, 0.0), 5);
    assert_eq!(exponential_histogram_projected_bucket_count(&value, 2.0), 7);
    assert_eq!(exponential_histogram_projected_bucket_count(&value, 4.0), 9);
    assert_eq!(
        exponential_histogram_projected_bucket_count(&value, f64::INFINITY),
        9
    );
}

#[test]
fn exponential_histogram_downscale_folds_negative_indexes_with_floor_division() {
    let value = ExponentialHistogramValue {
        count: 16,
        sum: Some(16.0),
        min: Some(-4.0),
        max: Some(4.0),
        scale: 2,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: -3,
            counts: vec![1, 2, 3, 4, 5],
        },
        negative: ExponentialHistogramBuckets {
            offset: -5,
            counts: vec![1, 2, 3, 4],
        },
    };

    let direct = downscale_exponential_histogram(&value, 0).unwrap();
    let repeated =
        downscale_exponential_histogram(&downscale_exponential_histogram(&value, 1).unwrap(), 0)
            .unwrap();

    assert_eq!(direct, repeated);
    assert_eq!(direct.scale, 0);
    assert_eq!(
        direct.positive,
        ExponentialHistogramBuckets {
            offset: -1,
            counts: vec![6, 9]
        }
    );
    assert_eq!(
        direct.negative,
        ExponentialHistogramBuckets {
            offset: -2,
            counts: vec![1, 9]
        }
    );
    assert_eq!(direct.count, value.count);
    assert_eq!(direct.zero_count, value.zero_count);
    assert_eq!(direct.sum, value.sum);
    assert_eq!(direct.min, value.min);
    assert_eq!(direct.max, value.max);
}

#[test]
fn exponential_histogram_merge_downscales_to_common_scale_and_merges_fields() {
    let metadata = TypedSampleMetadata::default();
    let finer = ExponentialHistogramValue {
        count: 6,
        sum: Some(6.0),
        min: Some(1.0),
        max: Some(4.0),
        scale: 1,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata,
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![2, 3],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    };
    let coarser = ExponentialHistogramValue {
        count: 12,
        sum: Some(18.0),
        min: Some(0.5),
        max: Some(8.0),
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 2,
        metadata,
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![4, 6],
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    };

    let merged =
        merge_exponential_histograms(&[finer, coarser], ExponentialHistogramScalePolicy::Keep)
            .unwrap()
            .unwrap();

    assert_eq!(merged.scale, 0);
    assert_eq!(merged.count, 18);
    assert_eq!(merged.zero_count, 3);
    assert_eq!(merged.sum, Some(24.0));
    assert_eq!(merged.min, Some(0.5));
    assert_eq!(merged.max, Some(8.0));
    assert_eq!(
        merged.positive,
        ExponentialHistogramBuckets {
            offset: 0,
            counts: vec![9, 6]
        }
    );
}

#[test]
fn exponential_histogram_merge_rejects_different_zero_thresholds() {
    let mut first = ExponentialHistogramValue {
        count: 1,
        sum: None,
        min: None,
        max: None,
        scale: 0,
        zero_threshold: 0.0,
        zero_count: 1,
        metadata: TypedSampleMetadata::default(),
        positive: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
        negative: ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        },
    };
    let mut second = first.clone();
    second.zero_threshold = 0.01;

    let err = merge_exponential_histograms(
        &[first.clone(), second],
        ExponentialHistogramScalePolicy::Keep,
    )
    .unwrap_err();
    assert_eq!(err, ExponentialHistogramMergeError::ZeroThresholdMismatch);

    first.scale = 0;
    assert!(downscale_exponential_histogram(&first, 1).is_err());
}

#[test]
fn head_buffer_summary_schema_roundtrip() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_varlen_encoding(VarLenEncodingKind::Schema);
    let mut head = HeadBuffer::new(config).unwrap();

    let value = SummaryValue {
        count: 9,
        sum: 18.0,
        metadata: TypedSampleMetadata::default(),
        quantiles: vec![
            SummaryQuantileValue {
                quantile: 0.5,
                value: 2.0,
            },
            SummaryQuantileValue {
                quantile: 0.9,
                value: 4.0,
            },
        ],
    };

    head.record_sample(
        SeriesRef::new(23),
        3_000,
        SampleValue::Summary(value.clone()),
    )
    .unwrap();

    let mut window = head.drain().unwrap();
    let series = window.series.remove(&SeriesRef::new(23)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Summary {
            samples: vec![(3_000, value)]
        }
    );
}

#[test]
fn head_buffer_respects_raw_encodings() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 1_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(2), 2_000, SampleValue::Int64(7))
        .unwrap();

    let mut window = head.drain().unwrap();

    let float_series = window.series.remove(&SeriesRef::new(1)).unwrap();
    let float_samples = float_series.into_samples(&window.arena).unwrap();
    let SeriesSamples::Float { encoding, samples } = float_samples else {
        panic!("expected float samples");
    };
    assert_eq!(encoding, FloatEncoding::Raw);
    assert_eq!(samples, vec![(1_000, 1.0)]);

    let int_series = window.series.remove(&SeriesRef::new(2)).unwrap();
    let int_samples = int_series.into_samples(&window.arena).unwrap();
    let SeriesSamples::Int64 { encoding, samples } = int_samples else {
        panic!("expected int samples");
    };
    assert_eq!(encoding, IntEncoding::Raw);
    assert_eq!(samples, vec![(2_000, 7)]);
}

#[test]
fn head_buffer_rejects_type_mismatch() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(9), 1_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(9), 2_000, SampleValue::Int64(2))
        .unwrap();

    let mut window = head.drain().unwrap();
    assert_eq!(window.datapoints, 1);
    let series = window.series.remove(&SeriesRef::new(9)).unwrap();
    let samples = series.into_samples(&window.arena).unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Float {
            encoding: FloatEncoding::Gorilla,
            samples: vec![(1_000, 1.0)]
        }
    );
}

#[test]
fn block_estimated_bytes_matches_timestamp_and_value_buffers() {
    let base_ms = 1_000;
    let mut block = BlockBuilder::<FloatRawCodec>::new(base_ms, base_ms, 1.0, 2).unwrap();
    block.push_sample(base_ms + 127, 2.0, 2).unwrap();
    let mut arena = BlockArena::new(1024);
    let block = block.seal(&mut arena);

    let mut expected_ts = Vec::new();
    encode_varint(0, &mut expected_ts);
    encode_varint(127, &mut expected_ts);
    let expected = expected_ts.len() + 2 * std::mem::size_of::<f64>();

    assert_eq!(block.estimated_bytes(), expected);
}

#[test]
fn float_raw_encoded_len_bytes_counts_values() {
    let mut codec = <FloatRawCodec as BlockCodec>::new(1.0).unwrap();
    codec.push(2.0).unwrap();
    assert_eq!(codec.encoded_len_bytes(), 2 * std::mem::size_of::<f64>());
}

#[test]
fn int_raw_encoded_len_bytes_counts_values() {
    let mut codec = <IntRawCodec as BlockCodec>::new(5).unwrap();
    codec.push(-7).unwrap();
    codec.push(9).unwrap();
    assert_eq!(codec.encoded_len_bytes(), 3 * std::mem::size_of::<i64>());
}

#[test]
fn int_delta_encoded_len_bytes_matches_varint_buffer() {
    let first = 10;
    let mut expected = Vec::new();
    encode_varint(encode_zigzag_i64(first), &mut expected);
    let mut prev = first;
    for value in [12_i64, 7_i64] {
        let delta = value.wrapping_sub(prev);
        encode_varint(encode_zigzag_i64(delta), &mut expected);
        prev = value;
    }

    let mut codec = <IntDeltaCodec as BlockCodec>::new(first).unwrap();
    codec.push(12).unwrap();
    codec.push(7).unwrap();
    assert_eq!(codec.encoded_len_bytes(), expected.len());
}

#[test]
fn float_gorilla_encoded_len_bytes_matches_encoder() {
    let values = [1.0, 1.5, 1.5, 2.25];
    let mut encoder = GorillaEncoder::new();
    for value in values {
        encoder.push(value).unwrap();
    }
    let expected = encoder.len_bytes();

    let mut codec = <FloatGorillaCodec as BlockCodec>::new(values[0]).unwrap();
    for value in &values[1..] {
        codec.push(*value).unwrap();
    }
    assert_eq!(codec.encoded_len_bytes(), expected);
}

#[test]
fn float_chimp128_duckdb_encoded_len_bytes_matches_encoder() {
    let values = [1.0, 1.5, 1.5, 2.25];
    let mut encoder = Chimp128DuckDBEncoder::new();
    for value in values {
        encoder.push(value).unwrap();
    }
    let expected = encoder.len_bytes();

    let mut codec = <FloatChimp128DuckDBDeferredCodec as BlockCodec>::new(values[0]).unwrap();
    for value in &values[1..] {
        codec.push(*value).unwrap();
    }
    assert_eq!(codec.encoded_len_bytes(), expected);
}

#[test]
fn series_estimated_bytes_matches_block_sum() {
    let mut series = Series::<FloatRawCodec>::new();
    let mut arena = BlockArena::new(1024);
    let base_ms = 1_000;
    let series_ref = SeriesRef::new(1);
    series
        .push_sample(series_ref, "float", base_ms, base_ms, 1.0, 2, &mut arena)
        .unwrap();
    series
        .push_sample(
            series_ref,
            "float",
            base_ms,
            base_ms + 127,
            2.0,
            2,
            &mut arena,
        )
        .unwrap();
    series.seal_current(&mut arena);

    let mut expected_ts = Vec::new();
    encode_varint(0, &mut expected_ts);
    encode_varint(127, &mut expected_ts);
    let block_bytes = expected_ts.len() + 2 * std::mem::size_of::<f64>();
    let expected = std::mem::size_of::<Series<FloatRawCodec>>() + block_bytes;

    assert_eq!(series.estimated_bytes(), expected);
}
