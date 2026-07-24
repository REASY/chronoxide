use std::{
    env, fs,
    io::{self, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use chronoxide_core::{
    labels::{
        DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore,
        METRIC_NAME_LABEL, SeriesRef,
    },
    storage::{
        head::{
            CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue,
            FloatEncoding, HeadBuffer, HeadConfig, HistogramValue, IntEncoding,
            OTLP_FLAG_NO_RECORDED_VALUE, OtlpAggregationTemporality, SampleValue,
            SummaryQuantileValue, SummaryValue, TypedSampleMetadata, prometheus_stale_nan,
        },
        segment::{
            QueryProjectionConfig, SegmentQueryResult, SegmentStoreReader, SegmentWriter,
            SegmentWriterConfig,
        },
    },
};

#[path = "prometheus_golden/fixtures.rs"]
mod fixtures;
#[path = "prometheus_golden/harness.rs"]
mod harness;

use fixtures::write_missing_label_semantics;
use harness::{
    assert_double_exponential_smoothing_matches_prometheus_http_api,
    assert_prometheus_exact_counter_float_order, assert_prometheus_golden_cases,
    assert_sort_order_matches_prometheus_http_api, find_promtool, query_chronoxide_golden_instant,
};

#[test]
#[ignore = "requires promtool; set CHRONOXIDE_PROMTOOL or install promtool"]
fn prometheus_golden_suite_matches_current_promql_surface() {
    assert_prometheus_golden_cases();
}

#[test]
fn prometheus_golden_instant_selector_uses_explicit_evaluation_time() {
    let tempdir = tempfile::tempdir().unwrap();
    let mut writer = SegmentWriter::new(SegmentWriterConfig::new(
        tempdir.path(),
        Duration::from_secs(10),
    ))
    .unwrap();
    write_missing_label_semantics(&mut writer);
    writer.flush().unwrap();

    let store = SegmentStoreReader::open(tempdir.path()).unwrap();
    let results = query_chronoxide_golden_instant(
        &store,
        r#"missing_semantics{env=""}"#,
        40_000,
        "instant selector regression",
    );

    let mut values = results
        .iter()
        .map(|result| {
            assert_eq!(result.samples.len(), 1, "{result:?}");
            assert_eq!(result.samples[0].0, 40_000, "{result:?}");
            result.samples[0].1
        })
        .collect::<Vec<_>>();
    values.sort_by(f64::total_cmp);
    assert_eq!(values, vec![1.0, 2.0]);
}

#[test]
#[ignore = "requires promtool; set CHRONOXIDE_PROMTOOL or install promtool"]
fn prometheus_exact_counter_float_order_matches() {
    let promtool = find_promtool();
    assert_prometheus_exact_counter_float_order(&promtool);
}

#[test]
#[ignore = "requires prometheus and promtool; set CHRONOXIDE_PROMETHEUS/CHRONOXIDE_PROMTOOL"]
fn sort_order_matches_prometheus_http_api() {
    assert_sort_order_matches_prometheus_http_api();
}

#[test]
#[ignore = "requires prometheus and promtool; set CHRONOXIDE_PROMETHEUS/CHRONOXIDE_PROMTOOL"]
fn double_exponential_smoothing_matches_prometheus_http_api() {
    assert_double_exponential_smoothing_matches_prometheus_http_api();
}

struct PromInputSeries {
    series: &'static str,
    values: &'static str,
}

struct GoldenCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    eval_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide: fn(&mut SegmentWriter),
    projection_config: fn() -> QueryProjectionConfig,
    expect_non_empty: bool,
}

struct GoldenRangeCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    start_secs: u64,
    end_secs: u64,
    step_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide: fn(&mut SegmentWriter),
    projection_config: fn() -> QueryProjectionConfig,
}

struct GoldenHeadRangeCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    start_secs: u64,
    end_secs: u64,
    step_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide:
        fn(&mut SegmentWriter, &mut FlatInternedLabelSetStore<DefaultSymbolTable>, &mut HeadBuffer),
    projection_config: fn() -> QueryProjectionConfig,
}

struct GoldenErrorCase {
    name: &'static str,
    chronoxide_query: &'static str,
    prom_query: &'static str,
    interval_secs: u64,
    eval_secs: u64,
    prom_input_series: &'static [PromInputSeries],
    write_chronoxide: fn(&mut SegmentWriter),
    projection_config: fn() -> QueryProjectionConfig,
    expected_chronoxide_error: &'static str,
    expected_promtool_error: &'static str,
}
