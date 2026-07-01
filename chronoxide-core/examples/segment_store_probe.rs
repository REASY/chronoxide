use std::env;
use std::time::Instant;

use chronoxide_core::storage::chunk::ChunkKind;
use chronoxide_core::storage::segment::{
    QueryLimits, SegmentStoreReader, SegmentStoreSmokeKindStats, SegmentStoreSmokeReport,
    SegmentStoreSmokeSeries,
};

fn main() {
    let config = ProbeConfig::from_env_args();

    let started = Instant::now();
    let store = SegmentStoreReader::open(&config.segments_dir).expect("open segment store");
    println!("open {:?} path={}", started.elapsed(), config.segments_dir);

    let started = Instant::now();
    let metric_names = store
        .metric_names(config.start_ms, config.end_ms)
        .expect("metric names");
    println!(
        "metric_names {:?} count={}",
        started.elapsed(),
        metric_names.len()
    );

    let started = Instant::now();
    let label_names = store
        .label_names(config.start_ms, config.end_ms)
        .expect("label names");
    println!(
        "label_names {:?} count={}",
        started.elapsed(),
        label_names.len()
    );

    for label in &config.label_values {
        let started = Instant::now();
        let values = store
            .label_values(label, config.start_ms, config.end_ms)
            .expect("label values");
        println!(
            "label_values({label}) {:?} count={}",
            started.elapsed(),
            values.len()
        );
    }

    if config.smoke_verify {
        let started = Instant::now();
        let report = store
            .smoke_verify(
                config.start_ms,
                config.end_ms,
                config.smoke_sample_limit_per_kind,
            )
            .expect("smoke verify");
        println!("smoke_verify {:?}", started.elapsed());
        print_smoke_report(&report);
    }

    let query = (!config.skip_query)
        .then(|| {
            config
                .query
                .as_deref()
                .or(metric_names.first().map(String::as_str))
        })
        .flatten();
    if let Some(query) = query {
        let started = Instant::now();
        let execution = store.query_promql_with_limits(
            query,
            config.start_ms,
            config.end_ms,
            QueryLimits {
                max_matched_series: config.max_matched_series,
                max_chunk_reads: config.max_chunk_reads,
                max_bytes_read: config.max_bytes_read,
                max_samples_decoded: config.max_samples_decoded,
                max_regex_values_examined: config.max_regex_values_examined,
            },
        );
        println!(
            "query({query}) {:?} result={execution:?}",
            started.elapsed()
        );
    }
}

#[derive(Debug)]
struct ProbeConfig {
    segments_dir: String,
    start_ms: u64,
    end_ms: u64,
    label_values: Vec<String>,
    query: Option<String>,
    skip_query: bool,
    max_matched_series: Option<u64>,
    max_chunk_reads: Option<u64>,
    max_bytes_read: Option<u64>,
    max_samples_decoded: Option<u64>,
    max_regex_values_examined: Option<u64>,
    smoke_verify: bool,
    smoke_sample_limit_per_kind: usize,
}

impl ProbeConfig {
    fn from_env_args() -> Self {
        let mut args = env::args().skip(1);
        let segments_dir = args
            .next()
            .unwrap_or_else(|| "data/smoke/segments-001".to_string());
        let start_ms = args
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(0);
        let end_ms = args
            .next()
            .and_then(|value| value.parse::<u64>().ok())
            .unwrap_or(u64::MAX);

        let label_values = env::var("PROBE_LABEL_VALUES")
            .ok()
            .map(|labels| {
                labels
                    .split(',')
                    .map(str::trim)
                    .filter(|label| !label.is_empty())
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_else(|| {
                vec![
                    "__name__".to_string(),
                    "k8s.pod.name".to_string(),
                    "service.name".to_string(),
                ]
            });

        Self {
            segments_dir,
            start_ms,
            end_ms,
            label_values,
            query: env::var("PROBE_QUERY")
                .ok()
                .filter(|query| !query.is_empty()),
            skip_query: env_bool("PROBE_SKIP_QUERY"),
            max_matched_series: env_limit("PROBE_MAX_MATCHED_SERIES").or(Some(1)),
            max_chunk_reads: env_limit("PROBE_MAX_CHUNK_READS"),
            max_bytes_read: env_limit("PROBE_MAX_BYTES_READ"),
            max_samples_decoded: env_limit("PROBE_MAX_SAMPLES_DECODED"),
            max_regex_values_examined: env_limit("PROBE_MAX_REGEX_VALUES_EXAMINED"),
            smoke_verify: env_bool("PROBE_SMOKE_VERIFY"),
            smoke_sample_limit_per_kind: env_limit("PROBE_SMOKE_SAMPLE_LIMIT_PER_KIND")
                .and_then(|value| usize::try_from(value).ok())
                .unwrap_or(1),
        }
    }
}

fn print_smoke_report(report: &SegmentStoreSmokeReport) {
    println!(
        "smoke_totals segments={} datapoints={} series={} chunks={} chunk_bytes={}",
        report.totals.segments,
        report.totals.datapoints,
        report.totals.series,
        report.totals.chunks,
        report.totals.chunk_bytes
    );
    print_kind_stats("float", report.totals.by_kind.float);
    print_kind_stats("int64", report.totals.by_kind.int64);
    print_kind_stats("histogram", report.totals.by_kind.histogram);
    print_kind_stats(
        "exponential_histogram",
        report.totals.by_kind.exponential_histogram,
    );
    print_kind_stats("summary", report.totals.by_kind.summary);

    for sample in &report.sample_series {
        print_sample(sample);
    }

    for query in &report.queries {
        println!(
            "smoke_query kind={} result_series={} result_samples={} matched_series={} chunk_reads={} bytes_read={} samples_decoded={} query={}",
            kind_name(query.kind),
            query.result_series,
            query.result_samples,
            query.matched_series,
            query.chunk_reads,
            query.bytes_read,
            query.samples_decoded,
            query.query
        );
    }
}

fn print_kind_stats(name: &str, stats: SegmentStoreSmokeKindStats) {
    println!(
        "smoke_kind kind={} chunks={} chunk_bytes={}",
        name, stats.chunks, stats.chunk_bytes
    );
}

fn print_sample(sample: &SegmentStoreSmokeSeries) {
    println!(
        "smoke_sample kind={} segment={} series_ref={} series_id={} metric={} labels={} samples={} min_time_ms={} max_time_ms={} chunk_bytes={} bucket_le={} quantile={}",
        kind_name(sample.kind),
        sample.segment_id,
        sample.series_ref,
        sample.series_id,
        metric_name(sample),
        format_labels(&sample.labels),
        sample.samples,
        sample.min_time_ms,
        sample.max_time_ms,
        sample.chunk_bytes,
        sample.bucket_le.as_deref().unwrap_or(""),
        sample.quantile.as_deref().unwrap_or("")
    );
}

fn metric_name(sample: &SegmentStoreSmokeSeries) -> &str {
    sample
        .labels
        .iter()
        .find_map(|(key, value)| (key == "__name__").then_some(value.as_str()))
        .unwrap_or("")
}

fn format_labels(labels: &[(String, String)]) -> String {
    let mut out = String::from("{");
    for (idx, (key, value)) in labels.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        out.push_str(key);
        out.push_str("=\"");
        out.push_str(&value.replace('\\', "\\\\").replace('"', "\\\""));
        out.push('"');
    }
    out.push('}');
    out
}

fn kind_name(kind: ChunkKind) -> &'static str {
    match kind {
        ChunkKind::Float => "float",
        ChunkKind::Int64 => "int64",
        ChunkKind::Histogram => "histogram",
        ChunkKind::ExponentialHistogram => "exponential_histogram",
        ChunkKind::Summary => "summary",
    }
}

fn env_limit(name: &str) -> Option<u64> {
    env::var(name)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
}

fn env_bool(name: &str) -> bool {
    env::var(name)
        .ok()
        .is_some_and(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
}
