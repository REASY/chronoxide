use std::collections::{HashMap, HashSet};
use std::env;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;

use chronoxide_capture::OtlpCaptureReader;
use chronoxide_core::labels::{METRIC_NAME_LABEL, SeriesRef, TmpLabel, TmpValue};
use chronoxide_core::otlp::{datapoint_time_ms, number_value};
use chronoxide_core::storage::head::{
    FloatEncoding, HeadBuffer, HeadConfig, HeadWindow, IntEncoding, SampleValue, SeriesSamples,
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::KeyValue;
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use prost::Message;
use rand::rngs::SmallRng;
use rand::{RngExt, SeedableRng};

const DEFAULT_WINDOW_MS: u64 = 60 * 60 * 1_000;
const DEFAULT_BLOCKS_PER_BENCH: usize = 16;
const DEFAULT_SEED: u64 = 0x7f4a_7c15_16b2_1a5d;
const DEFAULT_SERIES_PER_BENCH: usize = 64;
const ENV_CAPTURE_PATH: &str = "HEAD_BENCH_CAPTURE";
const ENV_CAPTURE_PATH_FALLBACK: &str = "HEAD_DECODE_CAPTURE";
const ENV_MAX_MESSAGES: &str = "HEAD_BENCH_MAX_MESSAGES";
const ENV_MAX_MESSAGES_FALLBACK: &str = "HEAD_DECODE_MAX_MESSAGES";
const ENV_MAX_SAMPLES: &str = "HEAD_BENCH_MAX_SAMPLES";
const ENV_MAX_SAMPLES_FALLBACK: &str = "HEAD_DECODE_MAX_SAMPLES";
const ENV_SERIES_COUNT: &str = "HEAD_BENCH_SERIES";

struct CaptureData {
    floats: Vec<(u64, f64)>,
    ints: Vec<(u64, i64)>,
    float_series: Vec<Vec<(u64, f64)>>,
    int_series: Vec<Vec<(u64, i64)>>,
}

struct SeriesBucket {
    floats: Vec<(u64, f64)>,
    ints: Vec<(u64, i64)>,
}

fn head_buffer_benches(c: &mut Criterion) {
    let capture = load_capture_data();

    let mut group = c.benchmark_group("head_read_float");
    for block_size in [256usize, 1024, 4096] {
        let target_samples = block_size * DEFAULT_BLOCKS_PER_BENCH;
        let samples = build_float_samples(&capture, target_samples, DEFAULT_WINDOW_MS, block_size);
        let sample_pairs = to_float_pairs(&samples);
        let (start_ms, end_ms) = sample_range(&samples, DEFAULT_WINDOW_MS);
        let sample_count = sample_pairs.len();
        let series_samples = vec![(SeriesRef::new(1), sample_pairs)];

        let raw_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Raw,
            IntEncoding::Raw,
        );
        let gorilla_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Gorilla,
            IntEncoding::Raw,
        );
        let elf_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Elf,
            IntEncoding::Raw,
        );
        let alp_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Alp,
            IntEncoding::Raw,
        );
        let alp_rd_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::AlpRd,
            IntEncoding::Raw,
        );
        let alp_spiral_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::AlpSpiral,
            IntEncoding::Raw,
        );
        let alp_rd_spiral_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::AlpRdSpiral,
            IntEncoding::Raw,
        );
        let chimp_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Chimp128DuckDB,
            IntEncoding::Raw,
        );
        let chimp_baseline_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Chimp128Baseline,
            IntEncoding::Raw,
        );

        log_window_stats(&format!("read_float raw block={block_size}"), &raw_window);
        log_window_stats(
            &format!("read_float gorilla block={block_size}"),
            &gorilla_window,
        );
        log_window_stats(&format!("read_float elf block={block_size}"), &elf_window);
        log_window_stats(&format!("read_float alp block={block_size}"), &alp_window);
        log_window_stats(
            &format!("read_float alp_rd block={block_size}"),
            &alp_rd_window,
        );
        log_window_stats(
            &format!("read_float alp_spiral block={block_size}"),
            &alp_spiral_window,
        );
        log_window_stats(
            &format!("read_float alp_rd_spiral block={block_size}"),
            &alp_rd_spiral_window,
        );
        log_window_stats(
            &format!("read_float chimp128_duckdb block={block_size}"),
            &chimp_window,
        );
        log_window_stats(
            &format!("read_float chimp128_baseline block={block_size}"),
            &chimp_baseline_window,
        );

        let raw_label = format!("read_float raw block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("raw", block_size),
            &raw_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&raw_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let gorilla_label = format!("read_float gorilla block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("gorilla", block_size),
            &gorilla_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&gorilla_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let elf_label = format!("read_float elf block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("elf", block_size),
            &elf_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&elf_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let alp_label = format!("read_float alp block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp", block_size),
            &alp_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let alp_rd_label = format!("read_float alp_rd block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp_rd", block_size),
            &alp_rd_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_rd_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let alp_spiral_label = format!("read_float alp_spiral block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp_spiral", block_size),
            &alp_spiral_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_spiral_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let alp_rd_spiral_label = format!("read_float alp_rd_spiral block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp_rd_spiral", block_size),
            &alp_rd_spiral_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_rd_spiral_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let chimp_label = format!("read_float chimp128_duckdb block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("chimp128_duckdb", block_size),
            &chimp_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&chimp_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let chimp_baseline_label = format!("read_float chimp128_baseline block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("chimp128_baseline", block_size),
            &chimp_baseline_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&chimp_baseline_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("head_read_int");
    for block_size in [256usize, 1024, 4096] {
        let target_samples = block_size * DEFAULT_BLOCKS_PER_BENCH;
        let samples = build_int_samples(&capture, target_samples, DEFAULT_WINDOW_MS, block_size);
        let sample_pairs = to_int_pairs(&samples);
        let (start_ms, end_ms) = sample_range(&samples, DEFAULT_WINDOW_MS);
        let sample_count = sample_pairs.len();
        let series_samples = vec![(SeriesRef::new(1), sample_pairs)];

        let raw_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Raw,
            IntEncoding::Raw,
        );
        let zigzag_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Raw,
            IntEncoding::DeltaZigZag,
        );

        log_window_stats(&format!("read_int raw block={block_size}"), &raw_window);
        log_window_stats(
            &format!("read_int delta_zigzag block={block_size}"),
            &zigzag_window,
        );

        let raw_label = format!("read_int raw block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("raw", block_size),
            &raw_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&raw_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
        let zigzag_label = format!("read_int delta_zigzag block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("delta_zigzag", block_size),
            &zigzag_window,
            |b, window| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let total = decode_count(window, start_ms, end_ms);
                        std::hint::black_box(total);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&zigzag_label, elapsed, iters, sample_count);
                    elapsed
                })
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("head_write_float");
    for block_size in [256usize, 1024, 4096] {
        let target_samples = block_size * DEFAULT_BLOCKS_PER_BENCH;
        let series_count = env_series_count();
        let series_samples = build_write_float_series(
            &capture,
            target_samples,
            DEFAULT_WINDOW_MS,
            block_size,
            series_count,
        );

        let total_samples = total_series_samples(&series_samples);
        let raw_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Raw,
            IntEncoding::Raw,
        );
        let gorilla_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Gorilla,
            IntEncoding::Raw,
        );
        let elf_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Elf,
            IntEncoding::Raw,
        );
        let alp_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Alp,
            IntEncoding::Raw,
        );
        let alp_rd_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::AlpRd,
            IntEncoding::Raw,
        );
        let alp_spiral_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::AlpSpiral,
            IntEncoding::Raw,
        );
        let alp_rd_spiral_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::AlpRdSpiral,
            IntEncoding::Raw,
        );
        let chimp_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Chimp128DuckDB,
            IntEncoding::Raw,
        );
        let chimp_baseline_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Chimp128Baseline,
            IntEncoding::Raw,
        );
        log_window_stats(&format!("write_float raw block={block_size}"), &raw_window);
        log_window_stats(
            &format!("write_float gorilla block={block_size}"),
            &gorilla_window,
        );
        log_window_stats(&format!("write_float elf block={block_size}"), &elf_window);
        log_window_stats(&format!("write_float alp block={block_size}"), &alp_window);
        log_window_stats(
            &format!("write_float alp_rd block={block_size}"),
            &alp_rd_window,
        );
        log_window_stats(
            &format!("write_float alp_spiral block={block_size}"),
            &alp_spiral_window,
        );
        log_window_stats(
            &format!("write_float alp_rd_spiral block={block_size}"),
            &alp_rd_spiral_window,
        );
        log_window_stats(
            &format!("write_float chimp128_duckdb block={block_size}"),
            &chimp_window,
        );
        log_window_stats(
            &format!("write_float chimp128_baseline block={block_size}"),
            &chimp_baseline_window,
        );

        let raw_label = format!("write_float raw block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("raw", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Raw,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&raw_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let gorilla_label = format!("write_float gorilla block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("gorilla", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Gorilla,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&gorilla_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let elf_label = format!("write_float elf block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("elf", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Elf,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&elf_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let alp_label = format!("write_float alp block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Alp,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let alp_rd_label = format!("write_float alp_rd block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp_rd", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::AlpRd,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_rd_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let alp_spiral_label = format!("write_float alp_spiral block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp_spiral", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::AlpSpiral,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_spiral_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let alp_rd_spiral_label = format!("write_float alp_rd_spiral block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("alp_rd_spiral", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::AlpRdSpiral,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&alp_rd_spiral_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let chimp_label = format!("write_float chimp128_duckdb block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("chimp128_duckdb", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Chimp128DuckDB,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&chimp_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let chimp_baseline_label = format!("write_float chimp128_baseline block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("chimp128_baseline", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Chimp128Baseline,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&chimp_baseline_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
    }
    group.finish();

    let mut group = c.benchmark_group("head_write_int");
    for block_size in [256usize, 1024, 4096] {
        let target_samples = block_size * DEFAULT_BLOCKS_PER_BENCH;
        let series_count = env_series_count();
        let series_samples = build_write_int_series(
            &capture,
            target_samples,
            DEFAULT_WINDOW_MS,
            block_size,
            series_count,
        );

        let total_samples = total_series_samples(&series_samples);
        let raw_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Raw,
            IntEncoding::Raw,
        );
        let zigzag_window = build_head_window_from_series(
            &series_samples,
            block_size,
            FloatEncoding::Raw,
            IntEncoding::DeltaZigZag,
        );
        log_window_stats(&format!("write_int raw block={block_size}"), &raw_window);
        log_window_stats(
            &format!("write_int delta_zigzag block={block_size}"),
            &zigzag_window,
        );

        let raw_label = format!("write_int raw block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("raw", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Raw,
                            IntEncoding::Raw,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&raw_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
        let zigzag_label = format!("write_int delta_zigzag block={block_size}");
        group.bench_with_input(
            BenchmarkId::new("delta_zigzag", block_size),
            &series_samples,
            |b, series_samples| {
                b.iter_custom(|iters| {
                    let start = std::time::Instant::now();
                    for _ in 0..iters {
                        let mut head = HeadBuffer::new(HeadConfig::with_block_size(
                            Duration::from_millis(DEFAULT_WINDOW_MS),
                            block_size,
                            FloatEncoding::Raw,
                            IntEncoding::DeltaZigZag,
                        ))
                        .expect("head buffer config");
                        for (series, samples) in series_samples {
                            let flushed = head
                                .record_samples(*series, samples)
                                .expect("record samples");
                            std::hint::black_box(flushed);
                        }
                        drain_head_for_bench(&mut head);
                    }
                    let elapsed = start.elapsed();
                    log_ns_per_sample(&zigzag_label, elapsed, iters, total_samples);
                    elapsed
                })
            },
        );
    }
    group.finish();
}

fn build_head_window_from_series(
    series_samples: &[(SeriesRef, Vec<(u64, SampleValue)>)],
    block_size: usize,
    float_encoding: FloatEncoding,
    int_encoding: IntEncoding,
) -> HeadWindow {
    let config = HeadConfig::with_block_size(
        Duration::from_millis(DEFAULT_WINDOW_MS),
        block_size,
        float_encoding,
        int_encoding,
    );
    let mut head = HeadBuffer::new(config).expect("head buffer config");
    for (series, samples) in series_samples {
        let flushed = head
            .record_samples(*series, samples)
            .expect("record samples");
        assert!(flushed.is_empty(), "unexpected head rotation");
    }
    head.drain().expect("head window")
}

fn decode_count(window: &HeadWindow, start_ms: u64, end_ms: u64) -> usize {
    let series = window
        .series_samples_in_range(start_ms, end_ms)
        .expect("decode samples");
    let mut total = 0usize;
    for (_, samples) in series {
        match samples {
            SeriesSamples::Float { samples, .. } => total = total.saturating_add(samples.len()),
            SeriesSamples::Int64 { samples, .. } => total = total.saturating_add(samples.len()),
            SeriesSamples::Histogram { samples } => total = total.saturating_add(samples.len()),
            SeriesSamples::ExponentialHistogram { samples } => {
                total = total.saturating_add(samples.len())
            }
            SeriesSamples::Summary { samples } => total = total.saturating_add(samples.len()),
        }
    }
    total
}

fn to_float_pairs(samples: &[(u64, f64)]) -> Vec<(u64, SampleValue)> {
    samples
        .iter()
        .map(|(ts, value)| (*ts, SampleValue::Float(*value)))
        .collect()
}

fn to_int_pairs(samples: &[(u64, i64)]) -> Vec<(u64, SampleValue)> {
    samples
        .iter()
        .map(|(ts, value)| (*ts, SampleValue::Int64(*value)))
        .collect()
}

fn sample_range<T>(samples: &[(u64, T)], window_ms: u64) -> (u64, u64) {
    if samples.is_empty() {
        return (0, window_ms);
    }
    let min_ts = samples.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
    let max_ts = samples.iter().map(|(ts, _)| *ts).max().unwrap_or(min_ts);
    (min_ts, max_ts.saturating_add(1))
}

fn total_series_samples(series_samples: &[(SeriesRef, Vec<(u64, SampleValue)>)]) -> usize {
    series_samples
        .iter()
        .map(|(_, samples)| samples.len())
        .sum()
}

fn log_window_stats(label: &str, window: &HeadWindow) {
    println!(
        "head_window_stats label={} series={} datapoints={} estimated_bytes={} payload_bytes={} arena_used={} arena_capacity={} arena_slack={}",
        label,
        window.series_len(),
        window.datapoints,
        window.estimated_bytes(),
        window.payload_bytes(),
        window.arena_used_bytes(),
        window.arena_capacity_bytes(),
        window.arena_slack_bytes(),
    );
}

fn log_ns_per_sample(label: &str, elapsed: Duration, iters: u64, samples_per_iter: usize) {
    if samples_per_iter == 0 || iters == 0 {
        return;
    }
    let total_samples = (iters as u128).saturating_mul(samples_per_iter as u128);
    if total_samples == 0 {
        return;
    }
    let ns_per_sample = elapsed.as_nanos() as f64 / total_samples as f64;
    if mark_logged(label) {
        println!(
            "head_ns_per_sample label={} ns_per_sample={:.2} samples_per_iter={}",
            label, ns_per_sample, samples_per_iter
        );
    }
}

fn drain_head_for_bench(head: &mut HeadBuffer) {
    let drained = head.drain();
    std::hint::black_box(drained);
}

fn mark_logged(label: &str) -> bool {
    static LOGGED: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let set = LOGGED.get_or_init(|| Mutex::new(HashSet::new()));
    let mut guard = set.lock().expect("log mutex");
    guard.insert(label.to_string())
}

fn env_var_any(keys: &[&str]) -> Option<String> {
    keys.iter().find_map(|key| env::var(key).ok())
}

fn env_usize(keys: &[&str], default: usize) -> usize {
    env_var_any(keys)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
}

fn env_series_count() -> usize {
    env_usize(&[ENV_SERIES_COUNT], DEFAULT_SERIES_PER_BENCH).max(1)
}

fn build_write_float_series(
    capture: &Option<CaptureData>,
    target_samples: usize,
    window_ms: u64,
    block_size: usize,
    series_count: usize,
) -> Vec<(SeriesRef, Vec<(u64, SampleValue)>)> {
    let samples_per_series = (target_samples / series_count.max(1)).max(1);
    let mut series = Vec::new();

    if let Some(capture) = capture {
        for bucket in capture.float_series.iter().take(series_count) {
            if bucket.is_empty() {
                continue;
            }
            let take = samples_per_series.min(bucket.len());
            series.push(normalize_timestamps(&bucket[..take], window_ms));
        }
    }

    while series.len() < series_count {
        let seed =
            DEFAULT_SEED ^ (block_size as u64).wrapping_mul(0x9e37_79b9) ^ (series.len() as u64);
        series.push(synthetic_floats(samples_per_series, window_ms, seed));
    }

    series
        .into_iter()
        .enumerate()
        .map(|(idx, samples)| {
            let series_ref = SeriesRef::new((idx + 1) as u32);
            let samples = samples
                .into_iter()
                .map(|(ts, value)| (ts, SampleValue::Float(value)))
                .collect();
            (series_ref, samples)
        })
        .collect()
}

fn build_write_int_series(
    capture: &Option<CaptureData>,
    target_samples: usize,
    window_ms: u64,
    block_size: usize,
    series_count: usize,
) -> Vec<(SeriesRef, Vec<(u64, SampleValue)>)> {
    let samples_per_series = (target_samples / series_count.max(1)).max(1);
    let mut series = Vec::new();

    if let Some(capture) = capture {
        for bucket in capture.int_series.iter().take(series_count) {
            if bucket.is_empty() {
                continue;
            }
            let take = samples_per_series.min(bucket.len());
            series.push(normalize_timestamps(&bucket[..take], window_ms));
        }
    }

    while series.len() < series_count {
        let seed =
            DEFAULT_SEED ^ (block_size as u64).wrapping_mul(0xbf58_476d) ^ (series.len() as u64);
        series.push(synthetic_ints(samples_per_series, window_ms, seed));
    }

    series
        .into_iter()
        .enumerate()
        .map(|(idx, samples)| {
            let series_ref = SeriesRef::new((idx + 1) as u32);
            let samples = samples
                .into_iter()
                .map(|(ts, value)| (ts, SampleValue::Int64(value)))
                .collect();
            (series_ref, samples)
        })
        .collect()
}

fn build_float_samples(
    capture: &Option<CaptureData>,
    target_samples: usize,
    window_ms: u64,
    block_size: usize,
) -> Vec<(u64, f64)> {
    if let Some(capture) = capture
        && capture.floats.len() >= target_samples
    {
        return normalize_timestamps(&capture.floats[..target_samples], window_ms);
    }
    let seed = DEFAULT_SEED ^ (block_size as u64).wrapping_mul(0x9e37_79b9);
    synthetic_floats(target_samples, window_ms, seed)
}

fn build_int_samples(
    capture: &Option<CaptureData>,
    target_samples: usize,
    window_ms: u64,
    block_size: usize,
) -> Vec<(u64, i64)> {
    if let Some(capture) = capture
        && capture.ints.len() >= target_samples
    {
        return normalize_timestamps(&capture.ints[..target_samples], window_ms);
    }
    let seed = DEFAULT_SEED ^ (block_size as u64).wrapping_mul(0xbf58_476d);
    synthetic_ints(target_samples, window_ms, seed)
}

fn normalize_timestamps<T: Copy>(samples: &[(u64, T)], window_ms: u64) -> Vec<(u64, T)> {
    if samples.is_empty() {
        return Vec::new();
    }
    let min_ts = samples.iter().map(|(ts, _)| *ts).min().unwrap_or(0);
    let max_ts = samples.iter().map(|(ts, _)| *ts).max().unwrap_or(min_ts);
    let range = max_ts.saturating_sub(min_ts);
    let scale = if range >= window_ms {
        (window_ms.saturating_sub(1)) as f64 / range.max(1) as f64
    } else {
        1.0
    };
    samples
        .iter()
        .map(|(ts, value)| {
            let rel = (*ts).saturating_sub(min_ts) as f64;
            let scaled = (rel * scale).round() as u64;
            (scaled.min(window_ms.saturating_sub(1)), *value)
        })
        .collect()
}

fn synthetic_floats(count: usize, window_ms: u64, seed: u64) -> Vec<(u64, f64)> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let timestamps = synthetic_timestamps(count, window_ms, &mut rng);
    let mut value = 100.0 + rng.random_range(-5.0..5.0);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let roll = rng.random_range(0u8..100);
        if roll < 70 {
            value += rng.random_range(-0.01..0.01);
        } else if roll < 90 {
            value += rng.random_range(-0.1..0.1);
        } else if roll < 95 {
            // keep value stable
        } else {
            value += rng.random_range(-5.0..5.0);
        }
        values.push(value);
    }
    timestamps.into_iter().zip(values).collect()
}

fn synthetic_ints(count: usize, window_ms: u64, seed: u64) -> Vec<(u64, i64)> {
    let mut rng = SmallRng::seed_from_u64(seed);
    let timestamps = synthetic_timestamps(count, window_ms, &mut rng);
    let mut value: i64 = 1_000 + rng.random_range(-50..50);
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let roll = rng.random_range(0u8..100);
        let delta = if roll < 80 {
            rng.random_range(0..=3)
        } else if roll < 95 {
            rng.random_range(4..=20)
        } else {
            -rng.random_range(1..=5)
        };
        value = value.saturating_add(delta);
        values.push(value);
    }
    timestamps.into_iter().zip(values).collect()
}

fn synthetic_timestamps(count: usize, window_ms: u64, rng: &mut SmallRng) -> Vec<u64> {
    let mut timestamps = Vec::with_capacity(count);
    let mut current: u64 = rng.random_range(0..1_000);
    for _ in 0..count {
        let roll = rng.random_range(0u8..100);
        let dt: u64 = if roll < 50 {
            0
        } else if roll < 80 {
            rng.random_range(1u64..=5)
        } else if roll < 95 {
            rng.random_range(10u64..=100)
        } else {
            rng.random_range(1_000u64..=10_000)
        };
        if current.saturating_add(dt) >= window_ms {
            // stay within the head window to avoid rotation.
        } else {
            current = current.saturating_add(dt);
        }
        timestamps.push(current);
    }
    timestamps
}

fn load_capture_data() -> Option<CaptureData> {
    let path = env_var_any(&[ENV_CAPTURE_PATH, ENV_CAPTURE_PATH_FALLBACK])?;
    let max_messages = env_usize(&[ENV_MAX_MESSAGES, ENV_MAX_MESSAGES_FALLBACK], 2_000);
    let max_samples = env_usize(&[ENV_MAX_SAMPLES, ENV_MAX_SAMPLES_FALLBACK], 200_000);

    let mut reader = OtlpCaptureReader::open(Path::new(&path)).ok()?;
    let mut series_map: HashMap<u64, SeriesBucket> = HashMap::new();
    let mut total_samples = 0usize;

    for _ in 0..max_messages {
        let Some(msg) = reader.next().ok().flatten() else {
            break;
        };
        let req = ExportMetricsServiceRequest::decode(msg.payload.as_slice()).ok()?;
        total_samples = extract_samples(&req, &mut series_map, total_samples, max_samples);
        if total_samples >= max_samples {
            break;
        }
    }

    let mut float_series: Vec<Vec<(u64, f64)>> = series_map
        .values()
        .filter(|bucket| !bucket.floats.is_empty())
        .map(|bucket| bucket.floats.clone())
        .collect();
    float_series.sort_by_key(|series| std::cmp::Reverse(series.len()));

    let mut int_series: Vec<Vec<(u64, i64)>> = series_map
        .values()
        .filter(|bucket| !bucket.ints.is_empty())
        .map(|bucket| bucket.ints.clone())
        .collect();
    int_series.sort_by_key(|series| std::cmp::Reverse(series.len()));

    Some(CaptureData {
        floats: float_series.first().cloned().unwrap_or_default(),
        ints: int_series.first().cloned().unwrap_or_default(),
        float_series,
        int_series,
    })
}

fn extract_samples(
    req: &ExportMetricsServiceRequest,
    series_map: &mut HashMap<u64, SeriesBucket>,
    mut total_samples: usize,
    max_samples: usize,
) -> usize {
    let mut tmp_labels: Vec<TmpLabel<'_>> = Vec::new();
    let mut scratch_values: Vec<Box<str>> = Vec::new();

    for resource_metrics in &req.resource_metrics {
        let resource_attrs = resource_metrics
            .resource
            .as_ref()
            .map(|res| res.attributes.as_slice())
            .unwrap_or(&[]);
        for scope_metrics in &resource_metrics.scope_metrics {
            for metric in &scope_metrics.metrics {
                let metric_name = metric.name.as_str();
                let Some(metric_data) = metric.data.as_ref() else {
                    continue;
                };
                match metric_data {
                    MetricData::Gauge(gauge) => {
                        total_samples = ingest_number_points(
                            metric_name,
                            resource_attrs,
                            &gauge.data_points,
                            series_map,
                            &mut tmp_labels,
                            &mut scratch_values,
                            total_samples,
                            max_samples,
                        );
                    }
                    MetricData::Sum(sum) => {
                        total_samples = ingest_number_points(
                            metric_name,
                            resource_attrs,
                            &sum.data_points,
                            series_map,
                            &mut tmp_labels,
                            &mut scratch_values,
                            total_samples,
                            max_samples,
                        );
                    }
                    MetricData::Histogram(_) => {}
                    MetricData::ExponentialHistogram(_) => {}
                    MetricData::Summary(_) => {}
                }
                if total_samples >= max_samples {
                    return total_samples;
                }
            }
        }
    }
    total_samples
}

#[expect(
    clippy::too_many_arguments,
    reason = "the capture-data benchmark harness keeps decoding scratch state and sample limits explicit"
)]
fn ingest_number_points<'a>(
    metric_name: &'a str,
    resource_attrs: &'a [KeyValue],
    points: &'a [opentelemetry_proto::tonic::metrics::v1::NumberDataPoint],
    series_map: &mut HashMap<u64, SeriesBucket>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    scratch_values: &mut Vec<Box<str>>,
    mut total_samples: usize,
    max_samples: usize,
) -> usize {
    for dp in points {
        if total_samples >= max_samples {
            break;
        }
        let Some(ts_ms) = datapoint_time_ms(dp.time_unix_nano) else {
            continue;
        };
        let value = number_value(dp);
        let Some(series_hash) = series_hash_for(
            metric_name,
            resource_attrs,
            &dp.attributes,
            tmp_labels,
            scratch_values,
        ) else {
            continue;
        };
        if let Some(value) = value {
            let entry = series_map
                .entry(series_hash)
                .or_insert_with(|| SeriesBucket {
                    floats: Vec::new(),
                    ints: Vec::new(),
                });
            match value {
                SampleValue::Float(val) => entry.floats.push((ts_ms, val)),
                SampleValue::Int64(val) => entry.ints.push((ts_ms, val)),
                SampleValue::Histogram(_)
                | SampleValue::ExponentialHistogram(_)
                | SampleValue::Summary(_) => {}
            }
            total_samples += 1;
        }
    }
    total_samples
}

fn series_hash_for<'a>(
    metric_name: &'a str,
    resource_attrs: &'a [KeyValue],
    datapoint_attrs: &'a [KeyValue],
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    scratch_values: &mut Vec<Box<str>>,
) -> Option<u64> {
    tmp_labels.clear();
    scratch_values.clear();

    tmp_labels.push(TmpLabel {
        key: METRIC_NAME_LABEL,
        value: TmpValue::Borrowed(metric_name),
        rank: 3,
        ordinal: 0,
    });
    push_kvs(tmp_labels, scratch_values, resource_attrs, 0);
    push_kvs(tmp_labels, scratch_values, datapoint_attrs, 2);

    if tmp_labels.is_empty() {
        return None;
    }

    tmp_labels.sort_by(|a, b| a.key.cmp(b.key).then_with(|| a.rank.cmp(&b.rank)));

    let mut hash = 0xcbf29ce484222325u64;
    let mut i = 0usize;
    while i < tmp_labels.len() {
        let key = tmp_labels[i].key;
        let mut j = i + 1;
        while j < tmp_labels.len() && tmp_labels[j].key == key {
            j += 1;
        }
        let chosen = tmp_labels[j - 1];
        let value = chosen.value.as_str(scratch_values);
        hash = fnv_update(hash, key.as_bytes());
        hash = fnv_update(hash, &[0x1f]);
        hash = fnv_update(hash, value.as_bytes());
        hash = fnv_update(hash, &[0x1e]);
        i = j;
    }
    Some(hash)
}

fn fnv_update(mut hash: u64, bytes: &[u8]) -> u64 {
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

fn push_kvs<'a>(
    out: &mut Vec<TmpLabel<'a>>,
    scratch_values: &mut Vec<Box<str>>,
    kvs: &'a [KeyValue],
    rank: u8,
) {
    for kv in kvs {
        let key = kv.key.as_str();
        if key.is_empty() || key == METRIC_NAME_LABEL {
            continue;
        }
        let Some(any_value) = kv.value.as_ref() else {
            continue;
        };
        let Some(value) = any_value.value.as_ref() else {
            continue;
        };

        let value = match value {
            AnyValue::StringValue(value) => TmpValue::Borrowed(value.as_str()),
            AnyValue::BoolValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            AnyValue::IntValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            AnyValue::DoubleValue(value) => {
                scratch_values.push(value.to_string().into_boxed_str());
                TmpValue::Scratch(scratch_values.len() - 1)
            }
            AnyValue::BytesValue(_)
            | AnyValue::ArrayValue(_)
            | AnyValue::KvlistValue(_)
            | AnyValue::StringValueStrindex(_) => {
                continue;
            }
        };
        let ordinal = out.len();
        out.push(TmpLabel {
            key,
            value,
            rank,
            ordinal,
        });
    }
}

criterion_group!(benches, head_buffer_benches);
criterion_main!(benches);
