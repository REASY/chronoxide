use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use chrono::Utc;
use chronoxide_core::promql::METRIC_NAME_LABEL;
use chronoxide_core::storage::chunk::ChunkKind;
use chronoxide_core::storage::segment::{
    SegmentStoreReader, SegmentStoreSmokeKindStats, SegmentStoreSmokeReport,
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
}

fn default_output_path(segments_dir: &Path) -> PathBuf {
    let parent = segments_dir
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let filename = format!("query_smoke_{}.md", Utc::now().format("%Y%m%d_%H%M%S"));
    parent.join(filename)
}

fn render_markdown(config: &QuerySmokeConfig, report: &SegmentStoreSmokeReport) -> String {
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

    markdown
}

fn run_query_smoke(config: &QuerySmokeConfig) -> io::Result<SegmentStoreSmokeReport> {
    let store = SegmentStoreReader::open(&config.segments_dir)?;
    let report =
        store.smoke_verify(config.start_ms, config.end_ms, config.sample_limit_per_kind)?;
    let markdown = render_markdown(config, &report);

    if let Some(parent) = config
        .output
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
    {
        fs::create_dir_all(parent)?;
    }
    fs::write(&config.output, markdown)?;

    Ok(report)
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
    use chronoxide_core::storage::head::{HistogramValue, TypedSampleMetadata};
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
        };

        let markdown = render_markdown(&config, &report);

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
        };

        let report = run_query_smoke(&config).unwrap();
        let markdown = fs::read_to_string(&config.output).unwrap();

        assert_eq!(report.totals.segments, 1);
        assert!(markdown.contains("request_duration"));
        assert!(markdown.contains("_bucket"));
        assert!(markdown.contains("## PromQL Readbacks"));
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
}
