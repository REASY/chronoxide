use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};

use chrono::Utc;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::chunk::{
    ChunkIndexReader, ChunkKind, ChunkRecord, ChunkSamples, read_chunk_record_at,
};
use chronoxide_core::storage::head::{
    OtlpAggregationTemporality, TypedSampleMetadata, prometheus_stale_nan,
};
use chronoxide_core::storage::segment::{
    SegmentFile, SegmentReader, SegmentStoreReader, SegmentStoreSmokeKindStats,
    SegmentStoreSmokeReport,
};
use chronoxide_core::storage::series::{
    SegmentSymbols, SeriesEntry, SeriesReader, read_symbols_bin,
};
use clap::Parser;

#[derive(Debug, Parser)]
#[command(about = "Run read-path smoke queries against sealed Chronoxide segments")]
struct Args {
    #[arg(long, default_value = "data/smoke/segments-001")]
    segments_dir: PathBuf,
    #[arg(long)]
    output: Option<PathBuf>,
    #[arg(long, default_value_t = 0)]
    start_ms: u64,
    #[arg(long, default_value_t = u64::MAX)]
    end_ms: u64,
    #[arg(long, default_value_t = 2)]
    sample_limit_per_kind: usize,
    #[arg(long)]
    verify_readbacks: bool,
}

fn main() {
    let args = Args::parse();
    let output = args
        .output
        .unwrap_or_else(|| default_output_path(&args.segments_dir));
    let config = QuerySmokeConfig {
        segments_dir: args.segments_dir,
        output,
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        sample_limit_per_kind: args.sample_limit_per_kind,
        verify_readbacks: args.verify_readbacks,
    };

    match run_query_smoke(&config) {
        Ok(report) => {
            println!(
                "wrote {} with {} readback queries over {} segments",
                config.output.display(),
                report.queries.len(),
                report.totals.segments
            );
        }
        Err(err) => {
            eprintln!("query smoke failed: {err}");
            std::process::exit(1);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct QuerySmokeConfig {
    segments_dir: PathBuf,
    output: PathBuf,
    start_ms: u64,
    end_ms: u64,
    sample_limit_per_kind: usize,
    verify_readbacks: bool,
}

fn default_output_path(segments_dir: &Path) -> PathBuf {
    let parent = segments_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = format!("query_smoke_{}.md", Utc::now().format("%Y%m%d_%H%M%S"));
    parent.join(filename)
}

fn render_markdown(
    config: &QuerySmokeConfig,
    report: &SegmentStoreSmokeReport,
    verification: Option<&QueryReadbackVerification>,
) -> String {
    let mut markdown = String::new();

    markdown.push_str("# Chronoxide Query Smoke Report\n\n");
    markdown.push_str(&format!(
        "- Generated At: {}\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    markdown.push_str(&format!(
        "- Segments Directory: `{}`\n",
        config.segments_dir.display()
    ));
    markdown.push_str(&format!(
        "- Time Range: {}..{}\n",
        config.start_ms,
        format_end_ms(config.end_ms)
    ));
    markdown.push_str(&format!(
        "- Sample Limit Per Kind: {}\n\n",
        config.sample_limit_per_kind
    ));

    markdown.push_str("## Segment Totals\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Segments | {} |\n", report.totals.segments));
    markdown.push_str(&format!(
        "| Segment Datapoints | {} |\n",
        report.totals.datapoints
    ));
    markdown.push_str(&format!("| Segment Series | {} |\n", report.totals.series));
    markdown.push_str(&format!("| Chunks | {} |\n", report.totals.chunks));
    markdown.push_str(&format!(
        "| Chunk Bytes | {} |\n\n",
        report.totals.chunk_bytes
    ));

    markdown.push_str("## Chunk Kinds\n\n");
    markdown.push_str("| Kind | Chunks | Chunk Bytes |\n");
    markdown.push_str("| --- | ---: | ---: |\n");
    for kind in [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ] {
        let stats = kind_stats(report, kind);
        markdown.push_str(&format!(
            "| {} | {} | {} |\n",
            kind_name(kind),
            stats.chunks,
            stats.chunk_bytes
        ));
    }
    markdown.push('\n');

    markdown.push_str("## Sampled Native Series\n\n");
    markdown.push_str(
        "| Kind | Metric | Segment | Series Ref | Samples | Time Range Ms | Chunk Bytes | Labels |\n",
    );
    markdown.push_str("| --- | --- | --- | ---: | ---: | --- | ---: | --- |\n");
    for sample in &report.sample_series {
        markdown.push_str(&format!(
            "| {} | `{}` | `{}` | {} | {} | {}..{} | {} | `{}` |\n",
            kind_name(sample.kind),
            markdown_escape_inline(sample_metric_name(&sample.labels)),
            markdown_escape_inline(&sample.segment_id),
            sample.series_ref,
            sample.samples,
            sample.min_time_ms,
            sample.max_time_ms,
            sample.chunk_bytes,
            markdown_escape_inline(&format_labels(&sample.labels))
        ));
    }
    markdown.push('\n');

    markdown.push_str("## PromQL Readbacks\n\n");
    markdown.push_str("| Kind | Query | result_series | result_samples | matched_series | chunk_reads | bytes_read | samples_decoded |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for query in &report.queries {
        markdown.push_str(&format!(
            "| {} | `{}` | {} | {} | {} | {} | {} | {} |\n",
            kind_name(query.kind),
            markdown_escape_inline(&query.query),
            query.result_series,
            query.result_samples,
            query.matched_series,
            query.chunk_reads,
            query.bytes_read,
            query.samples_decoded
        ));
    }

    if let Some(verification) = verification {
        markdown.push_str("\n## Readback Verification\n\n");
        markdown.push_str("| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Checked Queries | {} |\n",
            verification.checked_queries
        ));
        markdown.push_str(&format!(
            "| Mismatches | {} |\n",
            verification.mismatches.len()
        ));

        if !verification.mismatches.is_empty() {
            markdown.push_str("\n| Query | Expected Missing Samples | Actual Samples |\n");
            markdown.push_str("| --- | --- | --- |\n");
            for mismatch in &verification.mismatches {
                markdown.push_str(&format!(
                    "| `{}` | `{}` | `{}` |\n",
                    markdown_escape_inline(&mismatch.query),
                    markdown_escape_inline(&format_samples(&mismatch.missing_expected_samples)),
                    markdown_escape_inline(&format_samples(&mismatch.actual_samples))
                ));
            }
        }
    }

    markdown
}

fn run_query_smoke(config: &QuerySmokeConfig) -> io::Result<SegmentStoreSmokeReport> {
    let store = SegmentStoreReader::open(&config.segments_dir)?;
    let report =
        store.smoke_verify(config.start_ms, config.end_ms, config.sample_limit_per_kind)?;
    let verification = if config.verify_readbacks {
        Some(verify_readbacks(config)?)
    } else {
        None
    };
    let markdown = render_markdown(config, &report, verification.as_ref());

    if let Some(parent) = config
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.output, markdown)?;

    if let Some(verification) = verification
        && !verification.mismatches.is_empty()
    {
        return Err(io::Error::other(format!(
            "readback verification found {} mismatches",
            verification.mismatches.len()
        )));
    }

    Ok(report)
}

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryReadbackVerification {
    checked_queries: usize,
    mismatches: Vec<QueryReadbackMismatch>,
}

#[derive(Debug, Clone, PartialEq)]
struct QueryReadbackMismatch {
    query: String,
    missing_expected_samples: Vec<(u64, f64)>,
    actual_samples: Vec<(u64, f64)>,
}

#[derive(Debug, Clone, PartialEq)]
struct ExpectedReadback {
    query: String,
    samples: Vec<(u64, f64)>,
}

fn verify_readbacks(config: &QuerySmokeConfig) -> io::Result<QueryReadbackVerification> {
    let expected = collect_expected_readbacks(config)?;
    let store = SegmentStoreReader::open(&config.segments_dir)?;
    let mut mismatches = Vec::new();

    for expected in &expected {
        let results = store
            .query_promql(&expected.query, config.start_ms, config.end_ms)
            .map_err(|err| io::Error::other(format!("query failed: {}: {err}", expected.query)))?;
        let actual_samples = results
            .iter()
            .flat_map(|result| result.samples.iter().copied())
            .collect::<Vec<_>>();
        let missing_expected_samples = expected
            .samples
            .iter()
            .copied()
            .filter(|sample| {
                !actual_samples
                    .iter()
                    .any(|actual| promql_sample_eq(*actual, *sample))
            })
            .collect::<Vec<_>>();
        if !missing_expected_samples.is_empty() {
            mismatches.push(QueryReadbackMismatch {
                query: expected.query.clone(),
                missing_expected_samples,
                actual_samples,
            });
        }
    }

    Ok(QueryReadbackVerification {
        checked_queries: expected.len(),
        mismatches,
    })
}

fn collect_expected_readbacks(config: &QuerySmokeConfig) -> io::Result<Vec<ExpectedReadback>> {
    let mut expected = Vec::new();
    let mut samples_by_kind = [0usize; 5];

    for segment_dir in segment_dirs(&config.segments_dir)? {
        let reader = SegmentReader::open(&segment_dir)?;
        if reader.meta().end_ms < config.start_ms || reader.meta().start_ms > config.end_ms {
            continue;
        }

        let symbols = read_symbols_bin(File::open(reader.file_path(SegmentFile::Symbols))?)?;
        let mut series_reader =
            SeriesReader::open(File::open(reader.file_path(SegmentFile::Series))?)?;
        let mut chunk_index_reader =
            ChunkIndexReader::open(File::open(reader.file_path(SegmentFile::ChunkIndex))?)?;
        let mut chunk_file = reader.open_chunks()?;

        for series_ref in 0..chunk_index_reader.len() {
            let series_ref = u32::try_from(series_ref).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "series_ref exceeds u32")
            })?;
            let Some(entries) = chunk_index_reader.read_entries(series_ref)? else {
                continue;
            };

            let mut labels = None;
            for entry in entries {
                if entry.max_time_ms < config.start_ms || entry.min_time_ms > config.end_ms {
                    continue;
                }
                let kind_index = chunk_kind_index(entry.kind);
                if config.sample_limit_per_kind == 0
                    || samples_by_kind[kind_index] >= config.sample_limit_per_kind
                {
                    continue;
                }

                if labels.is_none() {
                    let Some(series_entry) = series_reader.read_entry(series_ref)? else {
                        continue;
                    };
                    labels = Some(resolve_series_labels(&symbols, &series_entry)?);
                }
                let Some(labels) = labels.as_ref() else {
                    continue;
                };

                let record = read_chunk_record_at(&mut chunk_file, entry.offset, entry.length)?;
                let mut readbacks =
                    expected_readbacks_for_record(labels, &record, config.start_ms, config.end_ms);
                if !readbacks.is_empty() {
                    samples_by_kind[kind_index] = samples_by_kind[kind_index].saturating_add(1);
                    expected.append(&mut readbacks);
                }
            }
        }
    }

    Ok(expected)
}

fn segment_dirs(segments_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut dirs = Vec::new();
    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("seg-") {
            dirs.push(entry.path());
        }
    }
    dirs.sort();
    Ok(dirs)
}

fn resolve_series_labels(
    symbols: &SegmentSymbols,
    series_entry: &SeriesEntry,
) -> io::Result<Vec<(String, String)>> {
    series_entry
        .labels
        .iter()
        .map(|(key, value)| {
            let key = symbols.resolve(*key).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series label key missing")
            })?;
            let value = symbols.resolve(*value).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "series label value missing")
            })?;
            Ok((key.to_string(), value.to_string()))
        })
        .collect()
}

fn expected_readbacks_for_record(
    labels: &[(String, String)],
    record: &ChunkRecord,
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let Some(metric_name) = labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
    else {
        return Vec::new();
    };

    match &record.samples {
        ChunkSamples::Float(samples) => vec![ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            samples: filter_samples(samples.iter().copied(), start_ms, end_ms),
        }],
        ChunkSamples::Int64(samples) => vec![ExpectedReadback {
            query: promql_exact_selector(metric_name, labels, None),
            samples: filter_samples(
                samples.iter().map(|(ts, value)| (*ts, *value as f64)),
                start_ms,
                end_ms,
            ),
        }],
        ChunkSamples::Histogram(samples) => {
            histogram_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
        ChunkSamples::ExponentialHistogram(samples) => {
            exponential_histogram_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
        ChunkSamples::Summary(samples) => {
            summary_expected_readbacks(metric_name, labels, samples, start_ms, end_ms)
        }
    }
    .into_iter()
    .filter(|readback| !readback.samples.is_empty())
    .collect()
}

fn histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![ExpectedReadback {
        query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
        samples: project_u64_counter_samples(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        ),
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.sum)),
                start_ms,
                end_ms,
            ),
        });
    }

    if let Some(le) = samples
        .first()
        .and_then(|(_, value)| value.explicit_bounds.first().copied())
        .map(format_promql_float_label)
    {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(
                &format!("{metric_name}_bucket"),
                labels,
                Some(("le", le.as_str())),
            ),
            samples: project_histogram_bucket_samples(samples, Some(le.as_str()), start_ms, end_ms),
        });
    }

    readbacks.push(ExpectedReadback {
        query: promql_exact_selector(
            &format!("{metric_name}_bucket"),
            labels,
            Some(("le", "+Inf")),
        ),
        samples: project_histogram_bucket_samples(samples, Some("+Inf"), start_ms, end_ms),
    });
    readbacks
}

fn exponential_histogram_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(
        u64,
        chronoxide_core::storage::head::ExponentialHistogramValue,
    )],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![ExpectedReadback {
        query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
        samples: project_u64_counter_samples(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        ),
    }];

    if samples.iter().all(|(_, value)| value.sum.is_some()) {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.sum)),
                start_ms,
                end_ms,
            ),
        });
    }

    readbacks.push(ExpectedReadback {
        query: promql_exact_selector(
            &format!("{metric_name}_bucket"),
            labels,
            Some(("le", "+Inf")),
        ),
        samples: project_u64_counter_samples(
            samples
                .iter()
                .map(|(ts, value)| (*ts, value.metadata, value.count)),
            start_ms,
            end_ms,
        ),
    });
    readbacks
}

fn summary_expected_readbacks(
    metric_name: &str,
    labels: &[(String, String)],
    samples: &[(u64, chronoxide_core::storage::head::SummaryValue)],
    start_ms: u64,
    end_ms: u64,
) -> Vec<ExpectedReadback> {
    let mut readbacks = vec![
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_count"), labels, None),
            samples: project_u64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, value.count)),
                start_ms,
                end_ms,
            ),
        },
        ExpectedReadback {
            query: promql_exact_selector(&format!("{metric_name}_sum"), labels, None),
            samples: project_optional_f64_counter_samples(
                samples
                    .iter()
                    .map(|(ts, value)| (*ts, value.metadata, Some(value.sum))),
                start_ms,
                end_ms,
            ),
        },
    ];

    if let Some(quantile) = samples
        .first()
        .and_then(|(_, value)| value.quantiles.first())
        .map(|quantile| format_promql_float_label(quantile.quantile))
    {
        readbacks.push(ExpectedReadback {
            query: promql_exact_selector(
                metric_name,
                labels,
                Some(("quantile", quantile.as_str())),
            ),
            samples: filter_samples(
                samples.iter().map(|(ts, value)| {
                    let sample_value = value
                        .quantiles
                        .first()
                        .map(|quantile| quantile.value)
                        .unwrap_or(f64::NAN);
                    (
                        *ts,
                        typed_f64_value(value.metadata.is_stale(), sample_value),
                    )
                }),
                start_ms,
                end_ms,
            ),
        });
    }

    readbacks
}

fn project_u64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    let mut accumulator = 0u64;
    samples
        .into_iter()
        .filter(|(ts, _, _)| *ts >= start_ms && *ts <= end_ms)
        .map(|(ts, metadata, raw)| {
            let value = if metadata.is_stale() {
                prometheus_stale_nan()
            } else if metadata.temporality == OtlpAggregationTemporality::Delta {
                accumulator = accumulator.saturating_add(raw);
                accumulator as f64
            } else {
                raw as f64
            };
            (ts, value)
        })
        .collect()
}

fn project_optional_f64_counter_samples(
    samples: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    let mut accumulator = 0.0f64;
    samples
        .into_iter()
        .filter(|(ts, _, _)| *ts >= start_ms && *ts <= end_ms)
        .filter_map(|(ts, metadata, raw)| {
            let value = if metadata.is_stale() {
                prometheus_stale_nan()
            } else if let Some(raw) = raw {
                if metadata.temporality == OtlpAggregationTemporality::Delta {
                    accumulator += raw;
                    accumulator
                } else {
                    raw
                }
            } else {
                return None;
            };
            Some((ts, value))
        })
        .collect()
}

fn project_histogram_bucket_samples(
    samples: &[(u64, chronoxide_core::storage::head::HistogramValue)],
    le_filter: Option<&str>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    let mut accumulator = 0u64;
    let mut out = Vec::new();
    for (ts, value) in samples {
        if *ts < start_ms || *ts > end_ms {
            continue;
        }

        let mut cumulative = 0u64;
        let mut raw = None;
        for (idx, bound) in value.explicit_bounds.iter().enumerate() {
            cumulative =
                cumulative.saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
            let le = format_promql_float_label(*bound);
            if le_filter.is_some_and(|filter| filter == le) {
                raw = Some(cumulative);
                break;
            }
        }
        if le_filter.is_some_and(|filter| filter == "+Inf") {
            raw = Some(value.count);
        }
        let Some(raw) = raw else {
            continue;
        };

        let projected = if value.metadata.is_stale() {
            prometheus_stale_nan()
        } else if value.metadata.temporality == OtlpAggregationTemporality::Delta {
            accumulator = accumulator.saturating_add(raw);
            accumulator as f64
        } else {
            raw as f64
        };
        out.push((*ts, projected));
    }
    out
}

fn filter_samples(
    samples: impl IntoIterator<Item = (u64, f64)>,
    start_ms: u64,
    end_ms: u64,
) -> Vec<(u64, f64)> {
    samples
        .into_iter()
        .filter(|(ts, _)| *ts >= start_ms && *ts <= end_ms)
        .collect()
}

fn typed_f64_value(stale: bool, value: f64) -> f64 {
    if stale { prometheus_stale_nan() } else { value }
}

fn promql_sample_eq(left: (u64, f64), right: (u64, f64)) -> bool {
    left.0 == right.0 && left.1.to_bits() == right.1.to_bits()
}

fn chunk_kind_index(kind: ChunkKind) -> usize {
    match kind {
        ChunkKind::Float => 0,
        ChunkKind::Int64 => 1,
        ChunkKind::Histogram => 2,
        ChunkKind::ExponentialHistogram => 3,
        ChunkKind::Summary => 4,
    }
}

fn format_end_ms(end_ms: u64) -> String {
    if end_ms == u64::MAX {
        "max".to_string()
    } else {
        end_ms.to_string()
    }
}

fn kind_stats(report: &SegmentStoreSmokeReport, kind: ChunkKind) -> SegmentStoreSmokeKindStats {
    match kind {
        ChunkKind::Float => report.totals.by_kind.float,
        ChunkKind::Int64 => report.totals.by_kind.int64,
        ChunkKind::Histogram => report.totals.by_kind.histogram,
        ChunkKind::ExponentialHistogram => report.totals.by_kind.exponential_histogram,
        ChunkKind::Summary => report.totals.by_kind.summary,
    }
}

fn kind_name(kind: ChunkKind) -> &'static str {
    match kind {
        ChunkKind::Float => "Float",
        ChunkKind::Int64 => "Int64",
        ChunkKind::Histogram => "Histogram",
        ChunkKind::ExponentialHistogram => "ExponentialHistogram",
        ChunkKind::Summary => "Summary",
    }
}

fn sample_metric_name(labels: &[(String, String)]) -> &str {
    labels
        .iter()
        .find_map(|(name, value)| (name == METRIC_NAME_LABEL).then_some(value.as_str()))
        .unwrap_or("<missing>")
}

fn format_labels(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn promql_exact_selector(
    metric_name: &str,
    labels: &[(String, String)],
    extra_label: Option<(&str, &str)>,
) -> String {
    let mut matchers = Vec::with_capacity(labels.len() + usize::from(extra_label.is_some()));
    matchers.push(format!(
        r#"{}="{}""#,
        METRIC_NAME_LABEL,
        promql_escape_string(metric_name)
    ));
    for (key, value) in labels {
        if key == METRIC_NAME_LABEL || extra_label.is_some_and(|(extra_key, _)| extra_key == key) {
            continue;
        }
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    if let Some((key, value)) = extra_label {
        matchers.push(format!(r#"{key}="{}""#, promql_escape_string(value)));
    }
    format!("{{{}}}", matchers.join(","))
}

fn promql_escape_string(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out
}

fn format_promql_float_label(value: f64) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "+Inf".to_string()
    } else {
        value.to_string()
    }
}

fn format_samples(samples: &[(u64, f64)]) -> String {
    samples
        .iter()
        .map(|(ts, value)| format!("({ts}, {value:?})"))
        .collect::<Vec<_>>()
        .join(", ")
}

fn markdown_escape_inline(value: &str) -> String {
    value
        .replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace('|', "\\|")
        .replace(['\n', '\r'], " ")
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::time::Duration;

    use chronoxide_core::labels::SeriesRef;
    use chronoxide_core::promql::METRIC_NAME_LABEL;
    use chronoxide_core::storage::head::{
        HistogramValue, OtlpAggregationTemporality, TypedSampleMetadata,
    };
    use chronoxide_core::storage::segment::{
        SegmentStoreReader, SegmentWriter, SegmentWriterConfig,
    };

    use super::*;

    #[test]
    fn default_output_path_is_next_to_segments_dir() {
        let output = default_output_path(Path::new("data/smoke/segments-001"));

        assert!(output.starts_with("data/smoke"));
        assert!(
            output
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("query_smoke_")
        );
    }

    #[test]
    fn render_markdown_reports_real_readback_queries() {
        let tempdir = segment_store_with_float_and_histogram();

        let store = SegmentStoreReader::open(tempdir.path()).unwrap();
        let report = store.smoke_verify(0, 10_000, 1).unwrap();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: false,
        };

        let markdown = render_markdown(&config, &report, None);

        assert!(markdown.contains("# Chronoxide Query Smoke Report"));
        assert!(markdown.contains("cpu_usage"));
        assert!(markdown.contains("request_duration"));
        assert!(markdown.contains("_count"));
        assert!(markdown.contains("_bucket"));
        assert!(markdown.contains("| Float | 1 |"));
        assert!(markdown.contains("| Histogram | 1 |"));
        assert!(markdown.contains("matched_series"));

        fs::write(&config.output, markdown).unwrap();
        assert!(config.output.exists());
    }

    #[test]
    fn run_query_smoke_writes_report_from_real_segments() {
        let tempdir = segment_store_with_float_and_histogram();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: false,
        };

        let report = run_query_smoke(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert_eq!(report.totals.segments, 1);
        assert!(markdown.contains("request_duration"));
        assert!(markdown.contains("_bucket"));
        assert!(markdown.contains("## PromQL Readbacks"));
    }

    #[test]
    fn run_query_smoke_verifies_readbacks_against_decoded_chunks() {
        let tempdir = segment_store_with_float_and_histogram();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: true,
        };

        run_query_smoke(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert!(markdown.contains("## Readback Verification"));
        assert!(markdown.contains("| Checked Queries | 5 |"));
        assert!(markdown.contains("| Mismatches | 0 |"));
    }

    #[test]
    fn run_query_smoke_verifies_delta_histogram_readbacks_after_projection() {
        let tempdir = segment_store_with_delta_histogram();
        let config = QuerySmokeConfig {
            segments_dir: tempdir.path().to_path_buf(),
            output: tempdir.path().join("query_smoke.md"),
            start_ms: 0,
            end_ms: 10_000,
            sample_limit_per_kind: 1,
            verify_readbacks: true,
        };

        run_query_smoke(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert!(markdown.contains("| Checked Queries | 4 |"));
        assert!(markdown.contains("| Mismatches | 0 |"));
    }

    fn segment_store_with_float_and_histogram() -> tempfile::TempDir {
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
        writer.flush().unwrap();

        tempdir
    }

    fn segment_store_with_delta_histogram() -> tempfile::TempDir {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10));
        let mut writer = SegmentWriter::new(config).unwrap();
        let metadata = TypedSampleMetadata {
            temporality: OtlpAggregationTemporality::Delta,
            ..TypedSampleMetadata::default()
        };

        writer
            .record_histogram_samples_ordered_with_label_visitor(
                SeriesRef::new(1),
                &[
                    (
                        1_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(2.0),
                            min: Some(2.0),
                            max: Some(2.0),
                            metadata,
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![0, 1],
                        },
                    ),
                    (
                        2_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(3.0),
                            min: Some(3.0),
                            max: Some(3.0),
                            metadata,
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![1, 0],
                        },
                    ),
                    (
                        3_000,
                        HistogramValue {
                            count: 1,
                            sum: Some(4.0),
                            min: Some(4.0),
                            max: Some(4.0),
                            metadata,
                            explicit_bounds: vec![1.0],
                            bucket_counts: vec![0, 1],
                        },
                    ),
                ],
                |visit| {
                    visit(METRIC_NAME_LABEL, "delta.request.duration");
                    visit("route", "/delta");
                },
            )
            .unwrap();
        writer.flush().unwrap();

        tempdir
    }
}
