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
fn dense_series_ref_map_keeps_sparse_and_extreme_refs_distinct() {
    let mut map = SeriesRefHashMap::default();
    for series in [0, 1, 2, 65_535, 1 << 24, u32::MAX] {
        map.insert(SeriesRef::new(series), u64::from(series) + 1);
    }

    for series in [0, 1, 2, 65_535, 1 << 24, u32::MAX] {
        assert_eq!(
            map.get(&SeriesRef::new(series)),
            Some(&(u64::from(series) + 1))
        );
    }
}

#[test]
fn last_timestamp_table_preserves_sparse_extreme_refs_and_all_u64_values() {
    let mut table = LastTimestampTable::default();
    let entries = [
        (SeriesRef::new(0), 0),
        (SeriesRef::new(1), 1),
        (SeriesRef::new(4_095), u64::MAX),
        (SeriesRef::new(4_096), u64::MAX - 1),
        (SeriesRef::new((1 << 24) - 1), 16),
        (SeriesRef::new(1 << 24), 17),
        (SeriesRef::new(u32::MAX), 23),
    ];

    assert!(table.is_empty());
    for (series, timestamp_ms) in entries {
        assert_eq!(table.insert(series, timestamp_ms), None);
    }
    assert_eq!(table.len(), entries.len());
    for (series, timestamp_ms) in entries {
        assert_eq!(table.get(series), Some(timestamp_ms));
    }
    assert_eq!(table.get(SeriesRef::new(2)), None);
    assert_eq!(table.insert(SeriesRef::new(4_095), 99), Some(u64::MAX));
    assert_eq!(table.get(SeriesRef::new(4_095)), Some(99));
    assert_eq!(table.len(), entries.len());
    assert!(table.paged_allocated_bytes() < 128 * 1024);
    assert_eq!(table.dense_page_count(), 0);
    assert_eq!(table.sparse_len(), entries.len());
}

#[test]
fn last_timestamp_table_promotes_dense_pages_but_keeps_strided_pages_sparse() {
    let mut table = LastTimestampTable::default();

    let strided_page_start = PAGE_LEN as u32;
    for slot in (0..PAGE_LEN as u32).step_by(4) {
        let raw = strided_page_start + slot;
        table.insert(SeriesRef::new(raw), u64::from(raw));
    }
    let extreme = SeriesRef::new(u32::MAX);
    table.insert(extreme, 7);
    assert_eq!(table.dense_page_count(), 0);
    assert_eq!(table.sparse_len(), PAGE_LEN / 4 + 1);

    let permuted_even_ref = |index: u32| ((index * 5) % DENSE_PAGE_THRESHOLD as u32) * 2;
    for index in 0..(DENSE_PAGE_THRESHOLD as u32 - 1) {
        let raw = permuted_even_ref(index);
        table.insert(SeriesRef::new(raw), u64::from(raw));
    }
    assert_eq!(table.dense_page_count(), 0);
    assert_eq!(
        table.sparse_len(),
        PAGE_LEN / 4 + 1 + DENSE_PAGE_THRESHOLD - 1
    );

    let transition_ref = SeriesRef::new(permuted_even_ref(DENSE_PAGE_THRESHOLD as u32 - 1));
    table.insert(transition_ref, u64::MAX);
    assert_eq!(table.dense_page_count(), 1);
    assert_eq!(table.sparse_len(), PAGE_LEN / 4 + 1);
    for index in 0..DENSE_PAGE_THRESHOLD as u32 {
        let raw = permuted_even_ref(index);
        let expected = if raw == transition_ref.get() {
            u64::MAX
        } else {
            u64::from(raw)
        };
        assert_eq!(table.get(SeriesRef::new(raw)), Some(expected));
    }

    assert_eq!(
        table.get(SeriesRef::new(strided_page_start)),
        Some(u64::from(strided_page_start))
    );
    assert_eq!(table.get(extreme), Some(7));

    let absent_dense_slot = SeriesRef::new(1);
    let len_before = table.len();
    assert_eq!(table.get(absent_dense_slot), None);
    assert_eq!(table.insert(absent_dense_slot, 11), None);
    assert_eq!(table.len(), len_before + 1);
    assert_eq!(table.insert(absent_dense_slot, 13), Some(11));
    assert_eq!(table.get(absent_dense_slot), Some(13));
    assert_eq!(table.len(), len_before + 1);
    assert!(table.paged_allocated_bytes() < 128 * 1024);
}

#[test]
fn last_timestamp_table_mutates_dense_and_sparse_entries_in_place() {
    let mut table = LastTimestampTable::default();
    for raw in 0..DENSE_PAGE_THRESHOLD as u32 {
        table.insert(SeriesRef::new(raw), u64::from(raw));
    }
    let dense = SeriesRef::new(17);
    *table.get_mut(dense).unwrap() = u64::MAX;
    assert_eq!(table.get(dense), Some(u64::MAX));

    let sparse = SeriesRef::new(PAGE_LEN as u32 + 17);
    table.insert(sparse, 1);
    *table.get_mut(sparse).unwrap() = 2;
    assert_eq!(table.get(sparse), Some(2));
}

#[test]
fn last_timestamp_stats_counters_match_scans_across_updates_promotion_and_high_refs() {
    for adaptive in [false, true] {
        let mut table = LastTimestampTable::new(adaptive);
        table.assert_stats_counters();

        let residual_sparse = SeriesRef::new(PAGE_LEN as u32 + 1);
        assert_eq!(table.insert(residual_sparse, 10), None);
        assert_eq!(table.insert(residual_sparse, 11), Some(10));
        let high = [SeriesRef::new(PAGED_REF_LIMIT), SeriesRef::new(u32::MAX)];
        for (index, series) in high.into_iter().enumerate() {
            assert_eq!(table.insert(series, 20 + index as u64), None);
            assert_eq!(
                table.insert(series, 30 + index as u64),
                Some(20 + index as u64)
            );
        }
        table.assert_stats_counters();

        for raw in 0..DENSE_PAGE_THRESHOLD as u32 {
            assert_eq!(table.insert(SeriesRef::new(raw), u64::from(raw)), None);
        }
        table.assert_stats_counters();
        assert_eq!(table.insert(SeriesRef::new(0), 99), Some(0));
        table.assert_stats_counters();

        let stats = table.stats();
        assert_eq!(stats.series, DENSE_PAGE_THRESHOLD + 3);
        assert_eq!(stats.refs_above_paged_limit, 2);
        if adaptive {
            assert_eq!(stats.dense_pages, 1);
            assert_eq!(stats.dense_series, DENSE_PAGE_THRESHOLD);
            assert_eq!(stats.sparse_pages, 1);
            assert_eq!(stats.sparse_series, 3);
        } else {
            assert_eq!(stats.dense_pages, 0);
            assert_eq!(stats.dense_series, 0);
            assert_eq!(stats.sparse_pages, 0);
            assert_eq!(stats.sparse_series, DENSE_PAGE_THRESHOLD + 3);
        }
    }
}

#[test]
fn last_timestamp_table_matches_hash_map_for_deterministic_trace() {
    let mut table = LastTimestampTable::default();
    let mut oracle = std::collections::HashMap::new();
    let mut state = 0x6a09_e667_f3bc_c909u64;

    for step in 0..50_000u32 {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let raw = match step % 257 {
            0 => u32::MAX,
            1 => 0,
            _ => (state as u32) % 100_000,
        };
        let series = SeriesRef::new(raw);
        let timestamp_ms = state.rotate_left(step % 64);

        assert_eq!(table.get(series), oracle.get(&series).copied());
        assert_eq!(
            table.insert(series, timestamp_ms),
            oracle.insert(series, timestamp_ms)
        );
        assert_eq!(table.get(series), Some(timestamp_ms));
    }

    assert_eq!(table.len(), oracle.len());
    for (series, timestamp_ms) in oracle {
        assert_eq!(table.get(series), Some(timestamp_ms));
    }
}

#[test]
fn plain_and_adaptive_last_timestamp_tables_match_promotion_rotation_and_ooo() {
    let run = |adaptive_last_timestamps| {
        let config = HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Raw,
            IntEncoding::Raw,
        )
        .with_out_of_order_time_window(Duration::from_secs(6))
        .with_adaptive_last_timestamp_table(adaptive_last_timestamps);
        let mut head = HeadBuffer::new(config).unwrap();
        let mut windows = Vec::new();
        for raw in 0..DENSE_PAGE_THRESHOLD as u32 {
            head.record_sample(
                SeriesRef::new(raw),
                1_000,
                SampleValue::Float(f64::from(raw)),
            )
            .unwrap();
        }
        head.record_sample(SeriesRef::new(0), 2_000, SampleValue::Float(2.0))
            .unwrap();
        windows.push(
            head.record_sample(SeriesRef::new(0), 15_000, SampleValue::Float(15.0))
                .unwrap()
                .unwrap(),
        );
        head.record_sample(SeriesRef::new(0), 12_000, SampleValue::Float(12.0))
            .unwrap();

        let timestamp_stats = head.last_timestamp_table_stats();
        assert_eq!(timestamp_stats.adaptive, adaptive_last_timestamps);
        assert_eq!(timestamp_stats.series, DENSE_PAGE_THRESHOLD);
        if adaptive_last_timestamps {
            assert_eq!(timestamp_stats.dense_pages, 1);
            assert_eq!(timestamp_stats.dense_series, DENSE_PAGE_THRESHOLD);
            assert_eq!(timestamp_stats.sparse_series, 0);
        } else {
            assert_eq!(timestamp_stats.dense_pages, 0);
            assert_eq!(timestamp_stats.dense_series, 0);
            assert_eq!(timestamp_stats.sparse_series, DENSE_PAGE_THRESHOLD);
        }

        windows.extend(head.drain_windows());
        assert_eq!(
            windows
                .iter()
                .filter(|window| window.is_out_of_order())
                .count(),
            1
        );
        let mut decoded = Vec::new();
        for window in windows {
            let lane = window.is_out_of_order();
            for (series, samples) in window.into_series_samples().unwrap() {
                let SeriesSamples::Float { samples, .. } = samples else {
                    panic!("expected float samples");
                };
                decoded.push((lane, series.get(), samples));
            }
        }
        decoded.sort_by_key(|(lane, series, _)| (*lane, *series));
        decoded
    };

    assert_eq!(run(false), run(true));
}

#[test]
fn plain_and_adaptive_head_series_tables_match_strided_partition_heads() {
    let run = |adaptive| {
        let config = HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Raw,
            IntEncoding::Raw,
        )
        .with_adaptive_series_table(adaptive);
        let mut heads = (0..64)
            .map(|_| HeadBuffer::new(config.clone()).unwrap())
            .collect::<Vec<_>>();
        let head_count = heads.len();

        for raw in 0..series_table::PAGE_LEN as u32 {
            heads[raw as usize % head_count]
                .record_sample(
                    SeriesRef::new(raw),
                    1_000,
                    SampleValue::Float(f64::from(raw)),
                )
                .unwrap();
        }

        let mut decoded = Vec::new();
        let mut direct_pages = 0;
        let mut sparse_series = 0;
        for head in &mut heads {
            let window = head.drain().unwrap();
            assert_eq!(window.series_len(), series_table::PAGE_LEN / head_count);
            direct_pages += window.series.direct_page_count();
            sparse_series += window.series.sparse_len();
            for (series, samples) in window.into_series_samples().unwrap() {
                let SeriesSamples::Float { samples, .. } = samples else {
                    panic!("expected float samples");
                };
                assert_eq!(samples.len(), 1);
                decoded.push((series.get(), samples[0].1.to_bits()));
            }
        }
        decoded.sort_unstable();
        (decoded, direct_pages, sparse_series)
    };

    let (plain, plain_direct_pages, plain_sparse_series) = run(false);
    let (adaptive, adaptive_direct_pages, adaptive_sparse_series) = run(true);
    assert_eq!(plain, adaptive);
    assert_eq!(plain.len(), series_table::PAGE_LEN);
    assert_eq!(plain_direct_pages, 0);
    assert_eq!(adaptive_direct_pages, 0);
    assert_eq!(plain_sparse_series, series_table::PAGE_LEN);
    assert_eq!(adaptive_sparse_series, series_table::PAGE_LEN);
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
fn head_buffer_record_samples_preserves_accepted_prefix_on_later_error() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    );
    let mut head = HeadBuffer::new(config).unwrap();
    let series = SeriesRef::new(1);

    let err = head
        .record_samples(
            series,
            &[
                (1_000, SampleValue::Float(1.0)),
                (999, SampleValue::Float(2.0)),
            ],
        )
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(head.last_timestamps.get(series), Some(1_000));
    let mut window = head.drain().unwrap();
    assert_eq!(window.datapoints, 1);
    let samples = window
        .series
        .remove(&series)
        .unwrap()
        .into_samples(&window.arena)
        .unwrap();
    assert_eq!(
        samples,
        SeriesSamples::Float {
            encoding: FloatEncoding::Raw,
            samples: vec![(1_000, 1.0)],
        }
    );
}

#[test]
fn head_series_table_first_insert_failure_is_atomic_in_both_modes() {
    let series = SeriesRef::new(0);
    for adaptive in [false, true] {
        let config = HeadConfig::new(Duration::from_secs(1), FloatEncoding::Raw, IntEncoding::Raw)
            .with_adaptive_series_table(adaptive);
        let mut window = HeadWindow::new(1_000, 2_000, adaptive);

        let err = HeadBuffer::push_sample_to_window(
            &config,
            &mut window,
            series,
            999,
            SampleValue::Float(1.0),
        )
        .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(window.series_len(), 0);
        assert_eq!(window.datapoints, 0);
        assert_eq!(window.arena_used_bytes(), 0);

        assert!(
            HeadBuffer::push_sample_to_window(
                &config,
                &mut window,
                series,
                1_000,
                SampleValue::Float(1.0),
            )
            .unwrap()
        );
        assert!(
            !HeadBuffer::push_sample_to_window(
                &config,
                &mut window,
                series,
                1_001,
                SampleValue::Int64(7),
            )
            .unwrap()
        );
        assert_eq!(window.series_len(), 1);
        assert_eq!(window.datapoints, 1);
        assert_eq!(
            window.into_series_samples().unwrap(),
            vec![(
                series,
                SeriesSamples::Float {
                    encoding: FloatEncoding::Raw,
                    samples: vec![(1_000, 1.0)],
                },
            )]
        );
    }
}

#[test]
fn owned_single_sample_and_borrowed_batch_paths_are_equivalent() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(5));
    let metadata = TypedSampleMetadata {
        start_time_ms: Some(500),
        flags: 7,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    let values = [
        SampleValue::Float(-1.25),
        SampleValue::Int64(i64::MIN),
        SampleValue::Histogram(HistogramValue {
            count: 6,
            sum: Some(f64::NEG_INFINITY),
            min: Some(-3.0),
            max: Some(8.0),
            metadata,
            explicit_bounds: vec![-1.0, 0.0, 4.0],
            bucket_counts: vec![1, 2, 2, 1],
        }),
        SampleValue::ExponentialHistogram(ExponentialHistogramValue {
            count: 9,
            sum: Some(-2.5),
            min: Some(-8.0),
            max: Some(16.0),
            scale: -1,
            zero_threshold: 0.125,
            zero_count: 2,
            metadata,
            positive: ExponentialHistogramBuckets {
                offset: -2,
                counts: vec![1, 0, 2],
            },
            negative: ExponentialHistogramBuckets {
                offset: 3,
                counts: vec![3, 1],
            },
        }),
        SampleValue::Summary(SummaryValue {
            count: 3,
            sum: 12.0,
            metadata,
            quantiles: vec![
                SummaryQuantileValue {
                    quantile: 0.5,
                    value: 3.0,
                },
                SummaryQuantileValue {
                    quantile: 0.99,
                    value: 8.0,
                },
            ],
        }),
    ];

    let mut trace = Vec::new();
    for (index, value) in values.iter().enumerate() {
        let series = SeriesRef::new(index as u32 + 1);
        trace.push((series, 1_000, value.clone()));
        trace.push((series, 15_000, value.clone()));
        trace.push((series, 12_000, value.clone()));
        trace.push((series, 15_000, value.clone()));
    }

    let run = |owned: bool| {
        let mut head = HeadBuffer::new(config.clone()).unwrap();
        let mut windows = Vec::new();
        for (series, timestamp_ms, value) in &trace {
            if owned {
                if let Some(window) = head
                    .record_sample(*series, *timestamp_ms, value.clone())
                    .unwrap()
                {
                    windows.push(window);
                }
            } else {
                windows.extend(
                    head.record_samples(*series, &[(*timestamp_ms, value.clone())])
                        .unwrap(),
                );
            }
        }
        windows.extend(head.drain_windows());
        windows
            .into_iter()
            .map(|mut window| {
                let encoded = std::mem::take(&mut window.series);
                let mut decoded: Vec<_> = encoded
                    .into_iter()
                    .map(|(series, encoded)| (series, encoded.into_samples(&window.arena).unwrap()))
                    .collect();
                decoded.sort_by_key(|(series, _)| series.get());
                (window.start_ms, window.end_ms, window.datapoints, decoded)
            })
            .collect::<Vec<_>>()
    };

    assert_eq!(run(true), run(false));
}

#[test]
fn plain_and_adaptive_head_series_tables_match_rotation_ooo_and_sealing() {
    let run = |adaptive| {
        let config = HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Raw,
            IntEncoding::Raw,
        )
        .with_out_of_order_time_window(Duration::from_secs(6))
        .with_adaptive_series_table(adaptive);
        let mut head = HeadBuffer::new(config).unwrap();
        let mut windows = Vec::new();

        for raw in 0..series_table::DIRECT_PAGE_THRESHOLD as u32 {
            head.record_sample(
                SeriesRef::new(raw),
                1_000,
                SampleValue::Float(f64::from(raw)),
            )
            .unwrap();
        }
        head.record_sample(SeriesRef::new(0), 2_000, SampleValue::Float(2.0))
            .unwrap();
        windows.push(
            head.record_sample(SeriesRef::new(0), 15_000, SampleValue::Float(15.0))
                .unwrap()
                .unwrap(),
        );
        head.record_sample(
            SeriesRef::new(series_table::DIRECT_PAGE_THRESHOLD as u32),
            9_000,
            SampleValue::Float(9.0),
        )
        .unwrap();
        head.record_sample(SeriesRef::new(1 << 24), 16_000, SampleValue::Float(16.0))
            .unwrap();
        windows.extend(head.drain_windows());

        let direct_pages: usize = windows
            .iter()
            .map(|window| window.series.direct_page_count())
            .sum();
        let mut decoded = windows
            .into_iter()
            .map(|window| {
                let start_ms = window.start_ms;
                let end_ms = window.end_ms;
                let datapoints = window.datapoints;
                let mut series = window.into_series_samples().unwrap();
                series.sort_by_key(|(series, _)| series.get());
                (start_ms, end_ms, datapoints, series)
            })
            .collect::<Vec<_>>();
        decoded.sort_by_key(|(start_ms, end_ms, _, series)| {
            (
                *start_ms,
                *end_ms,
                series.first().map_or(u32::MAX, |v| v.0.get()),
            )
        });
        (decoded, direct_pages)
    };

    let (plain, plain_direct_pages) = run(false);
    let (adaptive, adaptive_direct_pages) = run(true);
    assert_eq!(plain, adaptive);
    assert_eq!(plain_direct_pages, 0);
    assert!(adaptive_direct_pages >= 1);
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
fn head_buffer_out_of_order_window_boundary_is_exact() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(2));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 5_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 3_000, SampleValue::Float(2.0))
        .unwrap();
    let err = head
        .record_sample(SeriesRef::new(1), 2_999, SampleValue::Float(3.0))
        .unwrap_err();

    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        head.drain_windows()
            .iter()
            .map(|window| window.datapoints)
            .sum::<u64>(),
        2
    );
}

#[test]
fn head_buffer_first_series_sample_older_than_active_window_routes_to_ooo() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 15_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(2), 9_500, SampleValue::Float(2.0))
        .unwrap();

    let windows = head.drain_windows();
    assert_eq!(windows.len(), 2);
    assert!(windows.iter().any(|window| {
        (window.start_ms, window.end_ms) == (0, 10_000)
            && window.series.contains_key(&SeriesRef::new(2))
    }));
    assert!(windows.iter().any(|window| {
        (window.start_ms, window.end_ms) == (10_000, 20_000)
            && window.series.contains_key(&SeriesRef::new(1))
    }));
}

#[test]
fn head_buffer_timestamp_state_survives_drain_for_late_arrivals() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(6));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 15_000, SampleValue::Float(1.0))
        .unwrap();
    let drained = head.drain().unwrap();
    assert_eq!((drained.start_ms, drained.end_ms), (10_000, 20_000));

    head.record_sample(SeriesRef::new(1), 9_500, SampleValue::Float(2.0))
        .unwrap();
    let windows = head.drain_windows();
    assert_eq!(windows.len(), 1);
    assert_eq!((windows[0].start_ms, windows[0].end_ms), (0, 10_000));
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
    )
    .with_compact_numeric_series(false);
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(1), 1_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 2_000, SampleValue::Float(2.0))
        .unwrap();
    head.record_sample(SeriesRef::new(1), 9_000, SampleValue::Float(3.0))
        .unwrap();

    let window = head.drain().unwrap();
    let series = window.series.get(SeriesRef::new(1)).unwrap();
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
fn compact_numeric_series_promotes_without_changing_numeric_bits_or_blocks() {
    #[cfg(target_pointer_width = "64")]
    assert!(std::mem::size_of::<EncodedSeries>() <= 96);

    let make_config = |compact| {
        HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_compact_numeric_series(compact)
    };
    let mut compact = HeadBuffer::new(make_config(true)).unwrap();
    let mut general = HeadBuffer::new(make_config(false)).unwrap();
    let float_series = SeriesRef::new(1);
    let int_series = SeriesRef::new(2);
    let short_series = SeriesRef::new(3);
    let float_values = [
        f64::from_bits(0x8000_0000_0000_0000),
        f64::from_bits(0x7ff8_0000_0000_1234),
        f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
        f64::INFINITY,
        f64::NEG_INFINITY,
    ];
    let int_values = [i64::MIN, -1, 0, 1, i64::MAX];

    for index in 0..INLINE_NUMERIC_SERIES_CAPACITY {
        let timestamp_ms = 1_000 + index as u64;
        compact
            .record_sample(
                float_series,
                timestamp_ms,
                SampleValue::Float(float_values[index]),
            )
            .unwrap();
        general
            .record_sample(
                float_series,
                timestamp_ms,
                SampleValue::Float(float_values[index]),
            )
            .unwrap();
        compact
            .record_sample(
                int_series,
                timestamp_ms,
                SampleValue::Int64(int_values[index]),
            )
            .unwrap();
        general
            .record_sample(
                int_series,
                timestamp_ms,
                SampleValue::Int64(int_values[index]),
            )
            .unwrap();
    }

    let compact_window = compact.window.as_ref().unwrap();
    assert!(compact_window.series[&float_series].is_inline_numeric());
    assert!(compact_window.series[&int_series].is_inline_numeric());
    let mut compact_block_counts: Vec<_> = compact_window.series_block_counts().collect();
    compact_block_counts.sort_unstable();
    assert_eq!(compact_block_counts, vec![2, 2]);
    let mut compact_block_samples = Vec::new();
    compact_window.for_each_block_sample(|samples| compact_block_samples.push(samples));
    compact_block_samples.sort_unstable();
    assert_eq!(compact_block_samples, vec![2, 2, 2, 2]);

    let fifth_timestamp_ms = 2_000;
    compact
        .record_sample(
            float_series,
            fifth_timestamp_ms,
            SampleValue::Float(float_values[4]),
        )
        .unwrap();
    general
        .record_sample(
            float_series,
            fifth_timestamp_ms,
            SampleValue::Float(float_values[4]),
        )
        .unwrap();
    compact
        .record_sample(
            int_series,
            fifth_timestamp_ms,
            SampleValue::Int64(int_values[4]),
        )
        .unwrap();
    general
        .record_sample(
            int_series,
            fifth_timestamp_ms,
            SampleValue::Int64(int_values[4]),
        )
        .unwrap();

    for (timestamp_ms, value) in [(3_000, 5.0), (3_001, 6.0), (3_002, 7.0)] {
        compact
            .record_sample(short_series, timestamp_ms, SampleValue::Float(value))
            .unwrap();
        general
            .record_sample(short_series, timestamp_ms, SampleValue::Float(value))
            .unwrap();
    }

    let compact_window = compact.window.as_ref().unwrap();
    assert!(!compact_window.series[&float_series].is_inline_numeric());
    assert!(!compact_window.series[&int_series].is_inline_numeric());
    assert!(compact_window.series[&short_series].is_inline_numeric());

    let mut compact_samples = compact.drain().unwrap().into_series_samples().unwrap();
    let mut general_samples = general.drain().unwrap().into_series_samples().unwrap();
    compact_samples.sort_by_key(|(series, _)| series.get());
    general_samples.sort_by_key(|(series, _)| series.get());
    assert_eq!(compact_samples.len(), general_samples.len());

    for ((compact_series, compact_values), (general_series, general_values)) in
        compact_samples.iter().zip(&general_samples)
    {
        assert_eq!(compact_series, general_series);
        match (compact_values, general_values) {
            (
                SeriesSamples::Float {
                    encoding: compact_encoding,
                    samples: compact_values,
                },
                SeriesSamples::Float {
                    encoding: general_encoding,
                    samples: general_values,
                },
            ) => {
                assert_eq!(compact_encoding, general_encoding);
                let compact_bits: Vec<_> = compact_values
                    .iter()
                    .map(|(timestamp_ms, value)| (*timestamp_ms, value.to_bits()))
                    .collect();
                let general_bits: Vec<_> = general_values
                    .iter()
                    .map(|(timestamp_ms, value)| (*timestamp_ms, value.to_bits()))
                    .collect();
                assert_eq!(compact_bits, general_bits);
            }
            (SeriesSamples::Int64 { .. }, SeriesSamples::Int64 { .. }) => {
                assert_eq!(compact_values, general_values);
            }
            _ => panic!("compact and general numeric kinds differ"),
        }
    }
}

#[test]
fn compact_numeric_ooo_promotion_preserves_duplicate_append_order() {
    fn ooo_samples(compact_numeric_series: bool) -> Vec<(u64, u64)> {
        let config = HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_out_of_order_time_window(Duration::from_secs(20))
        .with_compact_numeric_series(compact_numeric_series);
        let mut head = HeadBuffer::new(config).unwrap();
        let series = SeriesRef::new(7);
        head.record_sample(series, 15_000, SampleValue::Float(100.0))
            .unwrap();
        for (timestamp_ms, value) in [(5_000, 1.0), (4_000, 2.0), (5_000, 3.0), (3_000, 4.0)] {
            head.record_sample(series, timestamp_ms, SampleValue::Float(value))
                .unwrap();
        }
        if compact_numeric_series {
            assert!(head.ooo_windows[&(0, 10_000)].series[&series].is_inline_numeric());
        }
        head.record_sample(series, 2_000, SampleValue::Float(5.0))
            .unwrap();
        if compact_numeric_series {
            assert!(!head.ooo_windows[&(0, 10_000)].series[&series].is_inline_numeric());
        }

        let window = head
            .drain_windows()
            .into_iter()
            .find(|window| window.start_ms == 0)
            .unwrap();
        let values = window
            .into_series_samples()
            .unwrap()
            .into_iter()
            .find(|(candidate, _)| *candidate == series)
            .unwrap()
            .1;
        let SeriesSamples::Float { samples, .. } = values else {
            panic!("expected float samples");
        };
        samples
            .into_iter()
            .map(|(timestamp_ms, value)| (timestamp_ms, value.to_bits()))
            .collect()
    }

    let compact = ooo_samples(true);
    let general = ooo_samples(false);
    assert_eq!(compact, general);
    assert_eq!(
        compact,
        vec![
            (2_000, 5.0f64.to_bits()),
            (3_000, 4.0f64.to_bits()),
            (4_000, 2.0f64.to_bits()),
            (5_000, 1.0f64.to_bits()),
            (5_000, 3.0f64.to_bits()),
        ]
    );
}

#[test]
fn compact_numeric_rejections_do_not_promote_or_advance_state() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(2));
    let mut head = HeadBuffer::new(config).unwrap();
    let series = SeriesRef::new(8);
    for timestamp_ms in 5_000..5_004 {
        head.record_sample(series, timestamp_ms, SampleValue::Float(1.0))
            .unwrap();
    }
    assert!(head.window.as_ref().unwrap().series[&series].is_inline_numeric());

    head.record_sample(series, 5_004, SampleValue::Int64(1))
        .unwrap();
    let err = head
        .record_sample(series, 3_002, SampleValue::Float(2.0))
        .unwrap_err();
    assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(head.last_timestamps.get(series), Some(5_003));
    let encoded = &head.window.as_ref().unwrap().series[&series];
    assert!(encoded.is_inline_numeric());
    assert_eq!(encoded.sample_count(), 4);

    head.record_sample(series, 5_004, SampleValue::Float(3.0))
        .unwrap();
    let encoded = &head.window.as_ref().unwrap().series[&series];
    assert!(!encoded.is_inline_numeric());
    assert_eq!(encoded.sample_count(), 5);
}

#[test]
fn compact_numeric_series_matches_general_across_block_boundaries() {
    let float_values = [
        f64::from_bits(0x8000_0000_0000_0000),
        f64::from_bits(0x7ff8_0000_0000_1234),
        f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
        f64::INFINITY,
        f64::NEG_INFINITY,
        42.5,
    ];
    let int_values = [i64::MIN, -1, 0, 1, i64::MAX, 42];

    for block_size in [1, 2, 3, 4, 5, 1_024] {
        for sample_count in 1..=float_values.len() {
            let make_head = |compact_numeric_series| {
                HeadBuffer::new(
                    HeadConfig::with_block_size(
                        Duration::from_secs(10),
                        block_size,
                        FloatEncoding::Gorilla,
                        IntEncoding::DeltaZigZag,
                    )
                    .with_compact_numeric_series(compact_numeric_series),
                )
                .unwrap()
            };
            let mut compact = make_head(true);
            let mut general = make_head(false);

            for index in 0..sample_count {
                let timestamp_ms = 1_000 + u64::try_from(index).unwrap();
                for head in [&mut compact, &mut general] {
                    head.record_sample(
                        SeriesRef::new(1),
                        timestamp_ms,
                        SampleValue::Float(float_values[index]),
                    )
                    .unwrap();
                    head.record_sample(
                        SeriesRef::new(2),
                        timestamp_ms,
                        SampleValue::Int64(int_values[index]),
                    )
                    .unwrap();
                }
            }

            let block_samples = |head: &HeadBuffer| {
                let window = head.window.as_ref().unwrap();
                let mut samples = Vec::new();
                window.for_each_block_sample(|count| samples.push(count));
                samples.sort_unstable();
                samples
            };
            assert_eq!(
                block_samples(&compact),
                block_samples(&general),
                "block_size={block_size} sample_count={sample_count}"
            );

            let decode = |head: &mut HeadBuffer| {
                let mut decoded = head.drain().unwrap().into_series_samples().unwrap();
                decoded.sort_by_key(|(series, _)| series.get());
                decoded
            };
            let compact = decode(&mut compact);
            let general = decode(&mut general);
            assert_eq!(compact.len(), general.len());
            for ((compact_series, compact_values), (general_series, general_values)) in
                compact.iter().zip(&general)
            {
                assert_eq!(compact_series, general_series);
                match (compact_values, general_values) {
                    (
                        SeriesSamples::Float {
                            samples: compact, ..
                        },
                        SeriesSamples::Float {
                            samples: general, ..
                        },
                    ) => {
                        let bits = |samples: &[(u64, f64)]| {
                            samples
                                .iter()
                                .map(|(timestamp_ms, value)| (*timestamp_ms, value.to_bits()))
                                .collect::<Vec<_>>()
                        };
                        assert_eq!(bits(compact), bits(general));
                    }
                    (SeriesSamples::Int64 { .. }, SeriesSamples::Int64 { .. }) => {
                        assert_eq!(compact_values, general_values);
                    }
                    _ => panic!("compact and general numeric kinds differ"),
                }
            }
        }
    }
}

#[test]
fn compact_numeric_series_live_query_matches_before_and_after_promotion() {
    fn query_after_samples(compact_numeric_series: bool, sample_count: usize) -> Vec<(u64, u64)> {
        let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
        let series = labels(
            &mut label_store,
            &[(METRIC_NAME_LABEL, "cpu.usage"), ("pod.name", "backend-1")],
        );
        let config = HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_compact_numeric_series(compact_numeric_series);
        let mut head = HeadBuffer::new(config).unwrap();
        let values = [
            f64::from_bits(0x8000_0000_0000_0000),
            f64::from_bits(0x7ff8_0000_0000_1234),
            f64::from_bits(PROMETHEUS_STALE_NAN_BITS),
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for (index, value) in values.into_iter().take(sample_count).enumerate() {
            head.record_sample(
                series,
                1_000 + u64::try_from(index).unwrap(),
                SampleValue::Float(value),
            )
            .unwrap();
        }

        let selector = SegmentSelector::with_metric(
            "cpu.usage",
            vec![LabelMatcher::eq("pod.name", "backend-1")],
        );
        head.query_selector(&label_store, &selector, 0, 10_000)
            .unwrap()
            .into_iter()
            .flat_map(|result| result.samples)
            .map(|(timestamp_ms, value)| (timestamp_ms, value.to_bits()))
            .collect()
    }

    for sample_count in [4, 5] {
        assert_eq!(
            query_after_samples(true, sample_count),
            query_after_samples(false, sample_count),
            "sample_count={sample_count}"
        );
    }
}

#[test]
fn compact_numeric_int_live_query_matches_partial_range_and_duplicate_timestamps() {
    fn query_after_samples(compact_numeric_series: bool, sample_count: usize) -> Vec<(u64, u64)> {
        let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
        let series = labels(
            &mut label_store,
            &[
                (METRIC_NAME_LABEL, "requests.total"),
                ("service.name", "backend"),
            ],
        );
        let config = HeadConfig::with_block_size(
            Duration::from_secs(10),
            2,
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_compact_numeric_series(compact_numeric_series);
        let mut head = HeadBuffer::new(config).unwrap();
        let samples = [
            (1_000, i64::MIN),
            (1_001, -1),
            (1_001, 7),
            (1_002, i64::MAX),
            (1_003, 42),
        ];
        for (timestamp_ms, value) in samples.into_iter().take(sample_count) {
            head.record_sample(series, timestamp_ms, SampleValue::Int64(value))
                .unwrap();
        }

        let selector = SegmentSelector::with_metric(
            "requests.total",
            vec![LabelMatcher::eq("service.name", "backend")],
        );
        head.query_selector(&label_store, &selector, 1_001, 1_002)
            .unwrap()
            .into_iter()
            .flat_map(|result| result.samples)
            .map(|(timestamp_ms, value)| (timestamp_ms, value.to_bits()))
            .collect()
    }

    for sample_count in [4, 5] {
        assert_eq!(
            query_after_samples(true, sample_count),
            query_after_samples(false, sample_count),
            "sample_count={sample_count}"
        );
    }
}

#[test]
fn compact_numeric_series_only_applies_to_default_numeric_codecs() {
    for encoding in [
        FloatEncoding::Raw,
        FloatEncoding::Elf,
        FloatEncoding::Alp,
        FloatEncoding::AlpRd,
        FloatEncoding::AlpSpiral,
        FloatEncoding::AlpRdSpiral,
        FloatEncoding::Chimp128DuckDB,
        FloatEncoding::Chimp128Baseline,
    ] {
        assert!(!EncodedSeries::new(SeriesEncoding::Float(encoding), true, 2).is_inline_numeric());
    }
    assert!(
        !EncodedSeries::new(SeriesEncoding::Int(IntEncoding::Raw), true, 2,).is_inline_numeric()
    );
    for encoding in [VarLenEncodingKind::Raw, VarLenEncodingKind::Schema] {
        assert!(
            !EncodedSeries::new(SeriesEncoding::Histogram(encoding), true, 2).is_inline_numeric()
        );
        assert!(
            !EncodedSeries::new(SeriesEncoding::ExponentialHistogram(encoding), true, 2,)
                .is_inline_numeric()
        );
        assert!(
            !EncodedSeries::new(SeriesEncoding::Summary(encoding), true, 2).is_inline_numeric()
        );
    }
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
        series: HeadSeriesTable::default(),
        datapoints: 0,
        arena: BlockArena::new(DEFAULT_HEAD_ARENA_PAGE_BYTES),
        out_of_order: false,
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
fn rejected_histogram_append_does_not_corrupt_existing_head_block() {
    for varlen_encoding in [VarLenEncodingKind::Raw, VarLenEncodingKind::Schema] {
        let config = HeadConfig::new(
            Duration::from_secs(10),
            FloatEncoding::Gorilla,
            IntEncoding::DeltaZigZag,
        )
        .with_varlen_encoding(varlen_encoding);
        let mut head = HeadBuffer::new(config).unwrap();
        let series_ref = SeriesRef::new(11);
        let valid = HistogramValue {
            count: 3,
            sum: Some(6.0),
            min: Some(1.0),
            max: Some(3.0),
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![1.0],
            bucket_counts: vec![1, 2],
        };
        let invalid = HistogramValue {
            count: u64::MAX,
            bucket_counts: vec![u64::MAX, 1],
            ..valid.clone()
        };

        head.record_sample(series_ref, 1_000, SampleValue::Histogram(valid.clone()))
            .unwrap();
        let error = head
            .record_sample(series_ref, 2_000, SampleValue::Histogram(invalid))
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "histogram bucket total overflows u64");
        head.record_sample(series_ref, 3_000, SampleValue::Histogram(valid.clone()))
            .unwrap();

        let mut window = head.drain().unwrap();
        assert_eq!(window.datapoints, 2);
        let series = window.series.remove(&series_ref).unwrap();
        let samples = series.into_samples(&window.arena).unwrap();
        assert_eq!(
            samples,
            SeriesSamples::Histogram {
                samples: vec![(1_000, valid.clone()), (3_000, valid)]
            },
            "varlen encoding {varlen_encoding:?}"
        );
    }
}

#[test]
fn rejected_rotating_sample_does_not_discard_completed_head_window() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();
    let series_ref = SeriesRef::new(12);
    let valid = HistogramValue {
        count: 3,
        sum: Some(6.0),
        min: Some(1.0),
        max: Some(3.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0],
        bucket_counts: vec![1, 2],
    };
    let invalid = HistogramValue {
        count: u64::MAX,
        bucket_counts: vec![u64::MAX, 1],
        ..valid.clone()
    };

    head.record_sample(series_ref, 1_000, SampleValue::Histogram(valid.clone()))
        .unwrap();
    let error = head
        .record_sample(series_ref, 11_000, SampleValue::Histogram(invalid))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(head.window_range(), Some((0, 10_000)));

    let completed = head
        .record_sample(series_ref, 11_000, SampleValue::Histogram(valid.clone()))
        .unwrap()
        .unwrap();
    assert_eq!(head.window_range(), Some((10_000, 20_000)));
    let completed_samples = completed.into_series_samples().unwrap();
    assert_eq!(
        completed_samples,
        vec![(
            series_ref,
            SeriesSamples::Histogram {
                samples: vec![(1_000, valid.clone())]
            }
        )]
    );

    let active = head.drain().unwrap();
    let active_samples = active.into_series_samples().unwrap();
    assert_eq!(
        active_samples,
        vec![(
            series_ref,
            SeriesSamples::Histogram {
                samples: vec![(11_000, valid)]
            }
        )]
    );
}

#[test]
fn rejected_later_batch_sample_retains_an_earlier_rotated_window() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    );
    let mut head = HeadBuffer::new(config).unwrap();
    let series_ref = SeriesRef::new(13);
    let valid = HistogramValue {
        count: 3,
        sum: Some(6.0),
        min: Some(1.0),
        max: Some(3.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0],
        bucket_counts: vec![1, 2],
    };
    let invalid = HistogramValue {
        count: u64::MAX,
        bucket_counts: vec![u64::MAX, 1],
        ..valid.clone()
    };

    head.record_sample(series_ref, 1_000, SampleValue::Histogram(valid.clone()))
        .unwrap();
    let error = head
        .record_samples(
            series_ref,
            &[
                (11_000, SampleValue::Histogram(valid.clone())),
                (12_000, SampleValue::Histogram(invalid)),
            ],
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(head.window_range(), Some((10_000, 20_000)));

    let retained = head
        .record_sample(series_ref, 13_000, SampleValue::Histogram(valid.clone()))
        .unwrap()
        .unwrap();
    assert_eq!((retained.start_ms, retained.end_ms), (0, 10_000));
    assert_eq!(
        retained.into_series_samples().unwrap(),
        vec![(
            series_ref,
            SeriesSamples::Histogram {
                samples: vec![(1_000, valid.clone())]
            }
        )]
    );

    let active = head.drain().unwrap();
    assert_eq!((active.start_ms, active.end_ms), (10_000, 20_000));
    assert_eq!(
        active.into_series_samples().unwrap(),
        vec![(
            series_ref,
            SeriesSamples::Histogram {
                samples: vec![(11_000, valid.clone()), (13_000, valid)]
            }
        )]
    );
}

#[test]
fn rejected_first_ooo_sample_does_not_publish_an_empty_window() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(20));
    let mut head = HeadBuffer::new(config).unwrap();
    let series_ref = SeriesRef::new(14);
    let valid = HistogramValue {
        count: 3,
        sum: Some(6.0),
        min: Some(1.0),
        max: Some(3.0),
        metadata: TypedSampleMetadata::default(),
        explicit_bounds: vec![1.0],
        bucket_counts: vec![1, 2],
    };
    let invalid = HistogramValue {
        count: u64::MAX,
        bucket_counts: vec![u64::MAX, 1],
        ..valid.clone()
    };

    head.record_sample(series_ref, 15_000, SampleValue::Histogram(valid.clone()))
        .unwrap();
    let error = head
        .record_sample(series_ref, 5_000, SampleValue::Histogram(invalid))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(head.ooo_windows.is_empty());
    assert_eq!(head.window_range(), Some((10_000, 20_000)));

    head.record_sample(series_ref, 5_000, SampleValue::Histogram(valid))
        .unwrap();
    assert_eq!(head.ooo_windows.len(), 1);
    assert_eq!(head.ooo_windows[&(0, 10_000)].datapoints, 1);
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
        count: 13,
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
        count: 13,
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
fn exponential_histogram_schema_push_is_transactional_after_shape_error() {
    let first = ExponentialHistogramValue {
        count: 1,
        sum: None,
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
    let mut codec = ExponentialHistogramSchemaCodec::new(first.clone()).unwrap();
    let before = codec.snapshot_bytes();

    let mut invalid = first.clone();
    invalid.scale = 3;
    invalid.count = 2;
    let error = codec.push(invalid).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(codec.snapshot_bytes(), before);

    let mut valid = first.clone();
    valid.scale = 3;
    codec.push(valid.clone()).unwrap();
    let decoded =
        ExponentialHistogramSchemaCodec::decode_values(&codec.snapshot_bytes(), 2).unwrap();
    assert_eq!(decoded, vec![first, valid]);
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
fn head_buffer_type_mismatch_does_not_advance_last_timestamp() {
    let config = HeadConfig::new(
        Duration::from_secs(10),
        FloatEncoding::Raw,
        IntEncoding::Raw,
    )
    .with_out_of_order_time_window(Duration::from_secs(2));
    let mut head = HeadBuffer::new(config).unwrap();

    head.record_sample(SeriesRef::new(9), 1_000, SampleValue::Float(1.0))
        .unwrap();
    head.record_sample(SeriesRef::new(9), 5_000, SampleValue::Int64(2))
        .unwrap();
    head.record_sample(SeriesRef::new(9), 500, SampleValue::Float(3.0))
        .unwrap();

    let mut samples = Vec::new();
    for window in head.drain_windows() {
        let encoded = window.series.get(SeriesRef::new(9)).unwrap();
        let SeriesSamples::Float {
            samples: window_samples,
            ..
        } = encoded.samples_in_range(&window.arena, 0, 10_000).unwrap()
        else {
            panic!("expected float samples");
        };
        samples.extend(window_samples);
    }
    samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
    assert_eq!(samples, vec![(500, 3.0), (1_000, 1.0)]);
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
