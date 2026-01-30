use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, LabelSetStoreError, SeriesRef, TmpLabel,
};
use chronoxide_core::otlp::{
    datapoint_time_ms, exponential_histogram_value, histogram_value, number_value, summary_value,
};
use chronoxide_core::otlp_capture::OtlpCaptureReader;
use chronoxide_core::otlp_labelset::{OtlpLabelSetInterner, intern_labelset};
use chronoxide_core::statistics::{
    DEFAULT_TDIGEST_BUFFER_CAPACITY, DEFAULT_TDIGEST_MAX_CENTROIDS, Stats,
};
use chronoxide_core::storage::head::{
    BytesByKind, FloatEncoding, HeadBuffer, HeadConfig, HeadWindow, IntEncoding, SampleValue,
    VarLenEncodingKind,
};
use clap::{Parser, ValueEnum};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogramDataPoint, HistogramDataPoint, NumberDataPoint, SummaryDataPoint,
};
use prost::Message;

type ExampleResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Clone, Copy, Debug, ValueEnum)]
enum Mode {
    Sample,
    Batch,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OutputFormat {
    Text,
    Markdown,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum LabelSetStoreKindArg {
    #[value(name = "flat_interned")]
    FlatInterned,
    #[value(name = "key_set_dict_encoded")]
    KeySetDictEncoded,
}

impl LabelSetStoreKindArg {
    fn as_str(self) -> &'static str {
        match self {
            Self::FlatInterned => "flat_interned",
            Self::KeySetDictEncoded => "key_set_dict_encoded",
        }
    }
}

enum LabelSetStoreWrapper {
    Flat(FlatInternedLabelSetStore<DefaultSymbolTable>),
    KeySet(KeySetDictEncodedLabelSetStore<DefaultSymbolTable>),
}

impl LabelSetStoreWrapper {
    fn new(kind: LabelSetStoreKindArg) -> Self {
        match kind {
            LabelSetStoreKindArg::FlatInterned => {
                Self::Flat(FlatInternedLabelSetStore::<DefaultSymbolTable>::default())
            }
            LabelSetStoreKindArg::KeySetDictEncoded => {
                Self::KeySet(KeySetDictEncodedLabelSetStore::<DefaultSymbolTable>::default())
            }
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::Flat(store) => store.len(),
            Self::KeySet(store) => store.len(),
        }
    }

    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        match self {
            Self::Flat(store) => store.intern(labels),
            Self::KeySet(store) => store.intern(labels),
        }
    }
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum FloatEncodingArg {
    #[value(name = "gorilla")]
    Gorilla,
    #[value(name = "elf")]
    Elf,
    #[value(name = "alp")]
    Alp,
    #[value(name = "alp_rd", alias = "alprd", alias = "alp-rd")]
    AlpRd,
    #[value(name = "alp_spiral", alias = "alp_spiraldb", alias = "alp_spiral_db")]
    AlpSpiral,
    #[value(
        name = "alp_rd_spiral",
        alias = "alp_spiral_rd",
        alias = "alp_rd_spiraldb",
        alias = "alp_rd_spiral_db"
    )]
    AlpRdSpiral,
    #[value(
        name = "chimp128_duckdb",
        alias = "chimp128",
        alias = "chimp128_duck_db"
    )]
    Chimp128DuckDB,
    #[value(name = "chimp128_baseline")]
    Chimp128Baseline,
    #[value(name = "raw")]
    Raw,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum IntEncodingArg {
    #[value(name = "delta_zigzag")]
    DeltaZigZag,
    #[value(name = "raw")]
    Raw,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum VarLenEncodingArg {
    #[value(name = "raw")]
    Raw,
    #[value(name = "schema")]
    Schema,
}

impl From<FloatEncodingArg> for FloatEncoding {
    fn from(value: FloatEncodingArg) -> Self {
        match value {
            FloatEncodingArg::Gorilla => Self::Gorilla,
            FloatEncodingArg::Elf => Self::Elf,
            FloatEncodingArg::Alp => Self::Alp,
            FloatEncodingArg::AlpRd => Self::AlpRd,
            FloatEncodingArg::AlpSpiral => Self::AlpSpiral,
            FloatEncodingArg::AlpRdSpiral => Self::AlpRdSpiral,
            FloatEncodingArg::Chimp128DuckDB => Self::Chimp128DuckDB,
            FloatEncodingArg::Chimp128Baseline => Self::Chimp128Baseline,
            FloatEncodingArg::Raw => Self::Raw,
        }
    }
}

impl From<IntEncodingArg> for IntEncoding {
    fn from(value: IntEncodingArg) -> Self {
        match value {
            IntEncodingArg::DeltaZigZag => Self::DeltaZigZag,
            IntEncodingArg::Raw => Self::Raw,
        }
    }
}

impl From<VarLenEncodingArg> for VarLenEncodingKind {
    fn from(value: VarLenEncodingArg) -> Self {
        match value {
            VarLenEncodingArg::Raw => Self::Raw,
            VarLenEncodingArg::Schema => Self::Schema,
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "headbuffer_replay",
    about = "Replay captured OTLP messages through HeadBuffer"
)]
struct Args {
    #[arg(short, long, value_name = "PATH")]
    capture_path: PathBuf,
    #[arg(long, value_enum, default_value_t = FloatEncodingArg::Gorilla)]
    float_encoding: FloatEncodingArg,
    #[arg(long, value_enum, default_value_t = IntEncodingArg::DeltaZigZag)]
    int_encoding: IntEncodingArg,
    #[arg(long, value_enum, default_value_t = VarLenEncodingArg::Raw)]
    varlen_encoding: VarLenEncodingArg,
    #[arg(long, value_enum, default_value_t = Mode::Sample)]
    mode: Mode,
    #[arg(long, value_enum, default_value_t = LabelSetStoreKindArg::FlatInterned)]
    labelset_store: LabelSetStoreKindArg,
    #[arg(long, value_delimiter = ',', num_args = 1.., value_name = "PARTITION")]
    partitions: Option<Vec<i32>>,
    #[arg(long = "stop-after-messages", alias = "stop-after")]
    stop_after_messages: Option<u64>,
    #[arg(long, value_enum, default_value_t = OutputFormat::Markdown)]
    output_format: OutputFormat,
}

#[derive(Default)]
struct Counters {
    messages: u64,
    datapoints_total: u64,
    datapoints_recorded: u64,
    float_samples: u64,
    int_samples: u64,
    histogram_samples: u64,
    exp_histogram_samples: u64,
    summary_samples: u64,
    raw_ts_bytes: u64,
    raw_value_bytes: u64,
    raw_bytes_by_kind_total: BytesByKind,
    raw_bytes_by_window: HashMap<u64, BytesByKind>,
    skipped_non_scalar: u64,
    decode_errors: u64,
    labelset_errors: u64,
}

struct HeadMetrics {
    call_latency: Stats<Duration>,
    batch_sizes: Stats<u64>,
    series_sample_counts: Stats<u64>,
    series_single_sample_count: u64,
    series_multi_sample_count: u64,
    head_time_ns: u128,
    head_calls: u64,
    head_samples: u64,
    windows_flushed: u64,
    encoded_bytes_total: u64,
    encoded_bytes_max: u64,
    encoded_payload_bytes_total: u64,
    encoded_payload_bytes_max: u64,
    encoded_series_total: u64,
    encoded_samples_total: u64,
    last_window_bytes: u64,
    last_window_payload_bytes: u64,
    last_window_series: u64,
    last_window_samples: u64,
    last_window_start_ms: Option<u64>,
    encoded_bytes_by_kind_total: BytesByKind,
    encoded_payload_bytes_by_kind_total: BytesByKind,
    last_window_bytes_by_kind: BytesByKind,
    last_window_payload_bytes_by_kind: BytesByKind,
    arena_capacity_total: u64,
    arena_used_total: u64,
    arena_slack_total: u64,
    arena_capacity_max: u64,
    arena_used_max: u64,
    arena_slack_max: u64,
    last_window_arena_capacity: u64,
    last_window_arena_used: u64,
    last_window_arena_slack: u64,
    last_window_arena_pages: u64,
}

impl HeadMetrics {
    fn new() -> Self {
        Self {
            call_latency: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            batch_sizes: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            series_sample_counts: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            series_single_sample_count: 0,
            series_multi_sample_count: 0,
            head_time_ns: 0,
            head_calls: 0,
            head_samples: 0,
            windows_flushed: 0,
            encoded_bytes_total: 0,
            encoded_bytes_max: 0,
            encoded_payload_bytes_total: 0,
            encoded_payload_bytes_max: 0,
            encoded_series_total: 0,
            encoded_samples_total: 0,
            last_window_bytes: 0,
            last_window_payload_bytes: 0,
            last_window_series: 0,
            last_window_samples: 0,
            last_window_start_ms: None,
            encoded_bytes_by_kind_total: BytesByKind::default(),
            encoded_payload_bytes_by_kind_total: BytesByKind::default(),
            last_window_bytes_by_kind: BytesByKind::default(),
            last_window_payload_bytes_by_kind: BytesByKind::default(),
            arena_capacity_total: 0,
            arena_used_total: 0,
            arena_slack_total: 0,
            arena_capacity_max: 0,
            arena_used_max: 0,
            arena_slack_max: 0,
            last_window_arena_capacity: 0,
            last_window_arena_used: 0,
            last_window_arena_slack: 0,
            last_window_arena_pages: 0,
        }
    }

    fn record_call(&mut self, elapsed: Duration, samples: usize, flushed_windows: usize) {
        self.call_latency.insert(elapsed);
        self.batch_sizes.insert(samples as u64);
        self.head_time_ns = self.head_time_ns.saturating_add(elapsed.as_nanos());
        self.head_calls = self.head_calls.saturating_add(1);
        self.head_samples = self.head_samples.saturating_add(samples as u64);
        self.windows_flushed = self.windows_flushed.saturating_add(flushed_windows as u64);
    }

    fn record_window(&mut self, window: &HeadWindow) {
        let bytes = window.estimated_bytes() as u64;
        let payload_bytes = window.payload_bytes() as u64;
        let bytes_by_kind = window.estimated_bytes_by_kind();
        let payload_bytes_by_kind = window.payload_bytes_by_kind();
        let series = window.series_len() as u64;
        let samples = window.datapoints;
        let arena_capacity = window.arena_capacity_bytes() as u64;
        let arena_used = window.arena_used_bytes() as u64;
        let arena_slack = window.arena_slack_bytes() as u64;
        let arena_pages = window.arena_page_count() as u64;

        for sample_count in window.series_sample_counts() {
            self.series_sample_counts.insert(sample_count);
            if sample_count <= 1 {
                self.series_single_sample_count = self.series_single_sample_count.saturating_add(1);
            } else {
                self.series_multi_sample_count = self.series_multi_sample_count.saturating_add(1);
            }
        }

        self.encoded_bytes_total = self.encoded_bytes_total.saturating_add(bytes);
        self.encoded_series_total = self.encoded_series_total.saturating_add(series);
        self.encoded_samples_total = self.encoded_samples_total.saturating_add(samples);
        self.encoded_bytes_max = self.encoded_bytes_max.max(bytes);
        self.encoded_payload_bytes_total = self
            .encoded_payload_bytes_total
            .saturating_add(payload_bytes);
        self.encoded_payload_bytes_max = self.encoded_payload_bytes_max.max(payload_bytes);
        add_bytes_by_kind_totals(&mut self.encoded_bytes_by_kind_total, bytes_by_kind);
        add_bytes_by_kind_totals(
            &mut self.encoded_payload_bytes_by_kind_total,
            payload_bytes_by_kind,
        );
        self.arena_capacity_total = self.arena_capacity_total.saturating_add(arena_capacity);
        self.arena_used_total = self.arena_used_total.saturating_add(arena_used);
        self.arena_slack_total = self.arena_slack_total.saturating_add(arena_slack);
        self.arena_capacity_max = self.arena_capacity_max.max(arena_capacity);
        self.arena_used_max = self.arena_used_max.max(arena_used);
        self.arena_slack_max = self.arena_slack_max.max(arena_slack);
        self.last_window_bytes = bytes;
        self.last_window_payload_bytes = payload_bytes;
        self.last_window_series = series;
        self.last_window_samples = samples;
        self.last_window_start_ms = Some(window.start_ms);
        self.last_window_bytes_by_kind = bytes_by_kind;
        self.last_window_payload_bytes_by_kind = payload_bytes_by_kind;
        self.last_window_arena_capacity = arena_capacity;
        self.last_window_arena_used = arena_used;
        self.last_window_arena_slack = arena_slack;
        self.last_window_arena_pages = arena_pages;
    }
}

struct BatchBuffer {
    per_series: HashMap<SeriesRef, Vec<(u64, SampleValue)>>,
}

impl BatchBuffer {
    fn new() -> Self {
        Self {
            per_series: HashMap::new(),
        }
    }

    fn push(&mut self, series: SeriesRef, ts_ms: u64, value: SampleValue) {
        self.per_series
            .entry(series)
            .or_insert_with(Vec::new)
            .push((ts_ms, value));
    }

    fn drain(
        &mut self,
    ) -> std::collections::hash_map::Drain<'_, SeriesRef, Vec<(u64, SampleValue)>> {
        self.per_series.drain()
    }
}

fn main() -> ExampleResult<()> {
    println!(
        "Running head ingestion example, PID: {}",
        std::process::id()
    );
    std::thread::sleep(Duration::from_secs(5));

    let args = Args::parse();
    let float_encoding: FloatEncoding = args.float_encoding.into();
    let int_encoding: IntEncoding = args.int_encoding.into();

    let mut reader = OtlpCaptureReader::open(&args.capture_path)?;
    let config = HeadConfig::new(Duration::from_secs(3600), float_encoding, int_encoding)
        .with_varlen_encoding(args.varlen_encoding.into());
    let window_duration_ms = config.window_duration.as_millis() as u64;
    let mut head = HeadBuffer::new(config)?;
    let mut labelsets = LabelSetStoreWrapper::new(args.labelset_store);
    let mut batch = BatchBuffer::new();
    let mut counters = Counters::default();
    let mut head_metrics = HeadMetrics::new();
    let partition_filter = args.partitions.as_deref();

    let start = Instant::now();
    loop {
        let Some(msg) = reader.next()? else {
            break;
        };
        if let Some(partitions) = partition_filter {
            if !partitions.contains(&msg.partition) {
                continue;
            }
        }

        counters.messages = counters.messages.saturating_add(1);

        let decoded = match ExportMetricsServiceRequest::decode(msg.payload.as_slice()) {
            Ok(decoded) => decoded,
            Err(_) => {
                counters.decode_errors = counters.decode_errors.saturating_add(1);
                continue;
            }
        };
        let fallback_ts_ms = if msg.timestamp_ms >= 0 {
            Some(msg.timestamp_ms)
        } else {
            None
        };

        match args.mode {
            Mode::Sample => ingest_request_sample(
                &mut head,
                &mut labelsets,
                &decoded,
                fallback_ts_ms,
                window_duration_ms,
                &mut counters,
                &mut head_metrics,
            )?,
            Mode::Batch => {
                ingest_request_collect(
                    &mut labelsets,
                    &decoded,
                    fallback_ts_ms,
                    window_duration_ms,
                    &mut counters,
                    |series, ts_ms, value| {
                        batch.push(series, ts_ms, value);
                        Ok(())
                    },
                )?;
                flush_batch(&mut head, &mut batch, &mut head_metrics)?;
            }
        }

        if let Some(stop_after) = args.stop_after_messages
            && counters.messages >= stop_after
        {
            break;
        }
    }

    if let Some(window) = head.drain() {
        head_metrics.windows_flushed = head_metrics.windows_flushed.saturating_add(1);
        head_metrics.record_window(&window);
    }

    let elapsed = start.elapsed();
    print_summary(
        &args,
        &counters,
        &head_metrics,
        args.labelset_store.as_str(),
        labelsets.len(),
        elapsed,
    );

    Ok(())
}

fn ingest_request_sample<'a>(
    head: &mut HeadBuffer,
    labelsets: &mut LabelSetStoreWrapper,
    req: &'a ExportMetricsServiceRequest,
    fallback_ts_ms: Option<i64>,
    window_duration_ms: u64,
    counters: &mut Counters,
    head_metrics: &mut HeadMetrics,
) -> ExampleResult<()> {
    ingest_request_collect(
        labelsets,
        req,
        fallback_ts_ms,
        window_duration_ms,
        counters,
        |series, ts_ms, value| {
            let call_start = Instant::now();
            let flushed = head.record_sample(series, ts_ms, value)?;
            let elapsed = call_start.elapsed();
            let flushed_windows = if flushed.is_some() { 1 } else { 0 };
            head_metrics.record_call(elapsed, 1, flushed_windows);
            if let Some(window) = &flushed {
                head_metrics.record_window(window);
            }
            Ok(())
        },
    )
}

fn ingest_request_collect<'a, F>(
    labelsets: &mut LabelSetStoreWrapper,
    req: &'a ExportMetricsServiceRequest,
    fallback_ts_ms: Option<i64>,
    window_duration_ms: u64,
    counters: &mut Counters,
    mut on_sample: F,
) -> ExampleResult<()>
where
    F: FnMut(SeriesRef, u64, SampleValue) -> ExampleResult<()>,
{
    let mut scratch_values: Vec<Box<str>> = Vec::new();
    let mut tmp_labels: Vec<TmpLabel<'a>> = Vec::new();

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
                    MetricData::Gauge(gauge) => ingest_number_datapoints(
                        labelsets,
                        resource_attrs,
                        metric_name,
                        &gauge.data_points,
                        &mut scratch_values,
                        &mut tmp_labels,
                        fallback_ts_ms,
                        window_duration_ms,
                        counters,
                        &mut on_sample,
                    )?,
                    MetricData::Sum(sum) => ingest_number_datapoints(
                        labelsets,
                        resource_attrs,
                        metric_name,
                        &sum.data_points,
                        &mut scratch_values,
                        &mut tmp_labels,
                        fallback_ts_ms,
                        window_duration_ms,
                        counters,
                        &mut on_sample,
                    )?,
                    MetricData::Histogram(hist) => ingest_histogram_datapoints(
                        labelsets,
                        resource_attrs,
                        metric_name,
                        &hist.data_points,
                        &mut scratch_values,
                        &mut tmp_labels,
                        fallback_ts_ms,
                        window_duration_ms,
                        counters,
                        &mut on_sample,
                    )?,
                    MetricData::ExponentialHistogram(hist) => ingest_exponential_histogram_points(
                        labelsets,
                        resource_attrs,
                        metric_name,
                        &hist.data_points,
                        &mut scratch_values,
                        &mut tmp_labels,
                        fallback_ts_ms,
                        window_duration_ms,
                        counters,
                        &mut on_sample,
                    )?,
                    MetricData::Summary(summary) => ingest_summary_points(
                        labelsets,
                        resource_attrs,
                        metric_name,
                        &summary.data_points,
                        &mut scratch_values,
                        &mut tmp_labels,
                        fallback_ts_ms,
                        window_duration_ms,
                        counters,
                        &mut on_sample,
                    )?,
                }
            }
        }
    }

    Ok(())
}

const RAW_TS_BYTES: u64 = 8;
const RAW_F64_BYTES: u64 = 8;
const RAW_U64_BYTES: u64 = 8;
const RAW_I32_BYTES: u64 = 4;

#[derive(Clone, Copy, Debug)]
enum RawKind {
    Float,
    Int,
    Histogram,
    ExponentialHistogram,
    Summary,
}

fn window_start_ms(timestamp_ms: u64, duration_ms: u64) -> u64 {
    timestamp_ms.saturating_sub(timestamp_ms % duration_ms)
}

fn add_bytes_by_kind(target: &mut BytesByKind, kind: RawKind, bytes: u64) {
    match kind {
        RawKind::Float => {
            target.float = target.float.saturating_add(bytes);
        }
        RawKind::Int => {
            target.int = target.int.saturating_add(bytes);
        }
        RawKind::Histogram => {
            target.histogram = target.histogram.saturating_add(bytes);
        }
        RawKind::ExponentialHistogram => {
            target.exponential_histogram = target.exponential_histogram.saturating_add(bytes);
        }
        RawKind::Summary => {
            target.summary = target.summary.saturating_add(bytes);
        }
    }
}

fn add_bytes_by_kind_totals(target: &mut BytesByKind, value: BytesByKind) {
    target.float = target.float.saturating_add(value.float);
    target.int = target.int.saturating_add(value.int);
    target.histogram = target.histogram.saturating_add(value.histogram);
    target.exponential_histogram = target
        .exponential_histogram
        .saturating_add(value.exponential_histogram);
    target.summary = target.summary.saturating_add(value.summary);
}

fn record_raw_baseline(
    counters: &mut Counters,
    window_start: u64,
    kind: RawKind,
    value_bytes: u64,
) {
    counters.raw_ts_bytes = counters.raw_ts_bytes.saturating_add(RAW_TS_BYTES);
    counters.raw_value_bytes = counters.raw_value_bytes.saturating_add(value_bytes);
    let total_bytes = RAW_TS_BYTES.saturating_add(value_bytes);
    add_bytes_by_kind(&mut counters.raw_bytes_by_kind_total, kind, total_bytes);
    let entry = counters
        .raw_bytes_by_window
        .entry(window_start)
        .or_default();
    add_bytes_by_kind(entry, kind, total_bytes);
}

fn raw_histogram_value_bytes(dp: &HistogramDataPoint) -> u64 {
    let mut bytes = RAW_U64_BYTES;
    if dp.sum.is_some() {
        bytes = bytes.saturating_add(RAW_F64_BYTES);
    }
    if dp.min.is_some() {
        bytes = bytes.saturating_add(RAW_F64_BYTES);
    }
    if dp.max.is_some() {
        bytes = bytes.saturating_add(RAW_F64_BYTES);
    }
    bytes = bytes.saturating_add((dp.explicit_bounds.len() as u64).saturating_mul(RAW_F64_BYTES));
    bytes = bytes.saturating_add((dp.bucket_counts.len() as u64).saturating_mul(RAW_U64_BYTES));
    bytes
}

fn raw_exponential_histogram_value_bytes(dp: &ExponentialHistogramDataPoint) -> u64 {
    let mut bytes = RAW_U64_BYTES;
    if dp.sum.is_some() {
        bytes = bytes.saturating_add(RAW_F64_BYTES);
    }
    if dp.min.is_some() {
        bytes = bytes.saturating_add(RAW_F64_BYTES);
    }
    if dp.max.is_some() {
        bytes = bytes.saturating_add(RAW_F64_BYTES);
    }
    bytes = bytes.saturating_add(RAW_I32_BYTES);
    bytes = bytes.saturating_add(RAW_U64_BYTES);
    if let Some(buckets) = dp.positive.as_ref() {
        bytes = bytes.saturating_add(RAW_I32_BYTES);
        bytes = bytes
            .saturating_add((buckets.bucket_counts.len() as u64).saturating_mul(RAW_U64_BYTES));
    }
    if let Some(buckets) = dp.negative.as_ref() {
        bytes = bytes.saturating_add(RAW_I32_BYTES);
        bytes = bytes
            .saturating_add((buckets.bucket_counts.len() as u64).saturating_mul(RAW_U64_BYTES));
    }
    bytes
}

fn raw_summary_value_bytes(dp: &SummaryDataPoint) -> u64 {
    let quantile_pairs = (dp.quantile_values.len() as u64).saturating_mul(2);
    RAW_U64_BYTES
        .saturating_add(RAW_F64_BYTES)
        .saturating_add(quantile_pairs.saturating_mul(RAW_F64_BYTES))
}

fn ingest_number_datapoints<'a, F>(
    labelsets: &mut LabelSetStoreWrapper,
    resource_attrs: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [NumberDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    fallback_ts_ms: Option<i64>,
    window_duration_ms: u64,
    counters: &mut Counters,
    on_sample: &mut F,
) -> ExampleResult<()>
where
    F: FnMut(SeriesRef, u64, SampleValue) -> ExampleResult<()>,
{
    for dp in points {
        counters.datapoints_total = counters.datapoints_total.saturating_add(1);
        let ts_ms = datapoint_time_ms(dp.time_unix_nano, fallback_ts_ms);
        let value = number_value(dp);
        let series = intern_labelset_with(
            labelsets,
            counters,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        );
        if let (Some(series), Some(ts_ms), Some(value)) = (series, ts_ms, value) {
            let window_start = window_start_ms(ts_ms, window_duration_ms);
            counters.datapoints_recorded = counters.datapoints_recorded.saturating_add(1);
            match &value {
                SampleValue::Float(_) => {
                    counters.float_samples = counters.float_samples.saturating_add(1);
                    record_raw_baseline(counters, window_start, RawKind::Float, RAW_F64_BYTES);
                }
                SampleValue::Int64(_) => {
                    counters.int_samples = counters.int_samples.saturating_add(1);
                    record_raw_baseline(counters, window_start, RawKind::Int, RAW_U64_BYTES);
                }
                SampleValue::Histogram(_) => {
                    counters.histogram_samples = counters.histogram_samples.saturating_add(1);
                }
                SampleValue::ExponentialHistogram(_) => {
                    counters.exp_histogram_samples =
                        counters.exp_histogram_samples.saturating_add(1);
                }
                SampleValue::Summary(_) => {
                    counters.summary_samples = counters.summary_samples.saturating_add(1);
                }
            }
            on_sample(series, ts_ms, value)?;
        }
    }
    Ok(())
}

fn ingest_histogram_datapoints<'a, F>(
    labelsets: &mut LabelSetStoreWrapper,
    resource_attrs: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [HistogramDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    fallback_ts_ms: Option<i64>,
    window_duration_ms: u64,
    counters: &mut Counters,
    on_sample: &mut F,
) -> ExampleResult<()>
where
    F: FnMut(SeriesRef, u64, SampleValue) -> ExampleResult<()>,
{
    for dp in points {
        counters.datapoints_total = counters.datapoints_total.saturating_add(1);
        let ts_ms = datapoint_time_ms(dp.time_unix_nano, fallback_ts_ms);
        let series = intern_labelset_with(
            labelsets,
            counters,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        );
        if let (Some(series), Some(ts_ms)) = (series, ts_ms) {
            let window_start = window_start_ms(ts_ms, window_duration_ms);
            let value = histogram_value(dp);
            counters.datapoints_recorded = counters.datapoints_recorded.saturating_add(1);
            counters.histogram_samples = counters.histogram_samples.saturating_add(1);
            record_raw_baseline(
                counters,
                window_start,
                RawKind::Histogram,
                raw_histogram_value_bytes(dp),
            );
            on_sample(series, ts_ms, value)?;
        }
    }
    Ok(())
}

fn ingest_exponential_histogram_points<'a, F>(
    labelsets: &mut LabelSetStoreWrapper,
    resource_attrs: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [ExponentialHistogramDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    fallback_ts_ms: Option<i64>,
    window_duration_ms: u64,
    counters: &mut Counters,
    on_sample: &mut F,
) -> ExampleResult<()>
where
    F: FnMut(SeriesRef, u64, SampleValue) -> ExampleResult<()>,
{
    for dp in points {
        counters.datapoints_total = counters.datapoints_total.saturating_add(1);
        let ts_ms = datapoint_time_ms(dp.time_unix_nano, fallback_ts_ms);
        let series = intern_labelset_with(
            labelsets,
            counters,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        );
        if let (Some(series), Some(ts_ms)) = (series, ts_ms) {
            let window_start = window_start_ms(ts_ms, window_duration_ms);
            let value = exponential_histogram_value(dp);
            counters.datapoints_recorded = counters.datapoints_recorded.saturating_add(1);
            counters.exp_histogram_samples = counters.exp_histogram_samples.saturating_add(1);
            record_raw_baseline(
                counters,
                window_start,
                RawKind::ExponentialHistogram,
                raw_exponential_histogram_value_bytes(dp),
            );
            on_sample(series, ts_ms, value)?;
        }
    }
    Ok(())
}

fn ingest_summary_points<'a, F>(
    labelsets: &mut LabelSetStoreWrapper,
    resource_attrs: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [SummaryDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    fallback_ts_ms: Option<i64>,
    window_duration_ms: u64,
    counters: &mut Counters,
    on_sample: &mut F,
) -> ExampleResult<()>
where
    F: FnMut(SeriesRef, u64, SampleValue) -> ExampleResult<()>,
{
    for dp in points {
        counters.datapoints_total = counters.datapoints_total.saturating_add(1);
        let ts_ms = datapoint_time_ms(dp.time_unix_nano, fallback_ts_ms);
        let series = intern_labelset_with(
            labelsets,
            counters,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        );
        if let (Some(series), Some(ts_ms)) = (series, ts_ms) {
            let window_start = window_start_ms(ts_ms, window_duration_ms);
            let value = summary_value(dp);
            counters.datapoints_recorded = counters.datapoints_recorded.saturating_add(1);
            counters.summary_samples = counters.summary_samples.saturating_add(1);
            record_raw_baseline(
                counters,
                window_start,
                RawKind::Summary,
                raw_summary_value_bytes(dp),
            );
            on_sample(series, ts_ms, value)?;
        }
    }
    Ok(())
}

fn flush_batch(
    head: &mut HeadBuffer,
    batch: &mut BatchBuffer,
    head_metrics: &mut HeadMetrics,
) -> ExampleResult<()> {
    for (series, samples) in batch.drain() {
        let call_start = Instant::now();
        let flushed = head.record_samples(series, &samples)?;
        let elapsed = call_start.elapsed();
        head_metrics.record_call(elapsed, samples.len(), flushed.len());
        for window in &flushed {
            head_metrics.record_window(window);
        }
    }
    Ok(())
}

struct ReplayLabelSetInterner<'a> {
    labelsets: &'a mut LabelSetStoreWrapper,
    counters: &'a mut Counters,
}

impl<'a> OtlpLabelSetInterner for ReplayLabelSetInterner<'a> {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {
        self.counters.skipped_non_scalar = self.counters.skipped_non_scalar.saturating_add(1);
    }

    fn on_intern_error(&mut self, _error: Self::Error) {
        self.counters.labelset_errors = self.counters.labelset_errors.saturating_add(1);
    }

    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, Self::Error> {
        self.labelsets.intern(labels)
    }
}

fn intern_labelset_with<'a>(
    labelsets: &mut LabelSetStoreWrapper,
    counters: &mut Counters,
    resource_attrs: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [opentelemetry_proto::tonic::common::v1::KeyValue],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) -> Option<SeriesRef> {
    let mut interner = ReplayLabelSetInterner {
        labelsets,
        counters,
    };
    intern_labelset(
        &mut interner,
        resource_attrs,
        metric_name,
        datapoint_attrs,
        scratch_values,
        tmp_labels,
    )
}

fn print_summary(
    args: &Args,
    counters: &Counters,
    head_metrics: &HeadMetrics,
    labelset_store: &str,
    series_count: usize,
    elapsed: Duration,
) {
    match args.output_format {
        OutputFormat::Text => print_summary_text(
            args,
            counters,
            head_metrics,
            labelset_store,
            series_count,
            elapsed,
        ),
        OutputFormat::Markdown => print_summary_markdown(
            args,
            counters,
            head_metrics,
            labelset_store,
            series_count,
            elapsed,
        ),
    }
}

fn print_summary_text(
    args: &Args,
    counters: &Counters,
    head_metrics: &HeadMetrics,
    labelset_store: &str,
    series_count: usize,
    elapsed: Duration,
) {
    let seconds = elapsed.as_secs_f64();
    let msg_rate = if seconds > 0.0 {
        counters.messages as f64 / seconds
    } else {
        0.0
    };
    let dp_rate = if seconds > 0.0 {
        counters.datapoints_recorded as f64 / seconds
    } else {
        0.0
    };

    println!("HeadBuffer replay");
    println!(
        "capture={} float_encoding={:?} int_encoding={:?} varlen_encoding={:?} mode={:?} labelset_store={} stop_after_messages={:?} partitions={:?}",
        args.capture_path.display(),
        args.float_encoding,
        args.int_encoding,
        args.varlen_encoding,
        args.mode,
        labelset_store,
        args.stop_after_messages,
        args.partitions
    );
    println!(
        "messages={} datapoints_total={} datapoints_recorded={} series={}",
        counters.messages, counters.datapoints_total, counters.datapoints_recorded, series_count
    );
    println!(
        "samples_float={} samples_int={} samples_histogram={} samples_exponential_histogram={} samples_summary={}",
        counters.float_samples,
        counters.int_samples,
        counters.histogram_samples,
        counters.exp_histogram_samples,
        counters.summary_samples
    );
    println!(
        "elapsed={:?} msg/s={:.2} dp/s={:.2}",
        elapsed, msg_rate, dp_rate
    );
    println!(
        "head_calls={} head_samples={} windows_flushed={}",
        head_metrics.head_calls, head_metrics.head_samples, head_metrics.windows_flushed
    );

    let avg_call = avg_duration(head_metrics.head_time_ns, head_metrics.head_calls);
    let avg_sample = avg_duration(head_metrics.head_time_ns, head_metrics.head_samples);
    println!(
        "head_time_total={} avg_per_call={} avg_per_sample={}",
        format_duration_ns(head_metrics.head_time_ns),
        avg_call,
        avg_sample
    );

    if let Some(dist) = head_metrics.call_latency.summarize() {
        println!("head_call_latency {}", dist);
    }
    if let Some(dist) = head_metrics.batch_sizes.summarize() {
        println!("batch_sizes {}", dist);
    }
    if let Some(dist) = head_metrics.series_sample_counts.summarize() {
        let series_total = head_metrics.series_sample_counts.count();
        let single = head_metrics.series_single_sample_count;
        let single_ratio = if series_total > 0 {
            single as f64 / series_total as f64
        } else {
            0.0
        };
        println!("series_sample_counts {}", dist);
        println!(
            "series_single_sample_count={} ratio={:.3} series_multi_sample_count={}",
            single, single_ratio, head_metrics.series_multi_sample_count
        );
    }
    let raw_total_bytes = counters
        .raw_ts_bytes
        .saturating_add(counters.raw_value_bytes);
    if counters.datapoints_recorded > 0 && raw_total_bytes > 0 {
        let raw_avg = avg_bytes_per_sample(raw_total_bytes, counters.datapoints_recorded);
        println!(
            "raw_bytes_total={} ({}) raw_bytes_per_sample={} raw_ts_bytes={} raw_value_bytes={}",
            raw_total_bytes,
            format_bytes(raw_total_bytes),
            raw_avg,
            counters.raw_ts_bytes,
            counters.raw_value_bytes
        );
        if counters.raw_bytes_by_kind_total.total() > 0 {
            print_bytes_by_kind("raw_bytes_by_kind_total", counters.raw_bytes_by_kind_total);
        }
        if let Some(start_ms) = head_metrics.last_window_start_ms {
            let raw_last = counters
                .raw_bytes_by_window
                .get(&start_ms)
                .copied()
                .unwrap_or_default();
            if raw_last.total() > 0 {
                print_bytes_by_kind("raw_bytes_by_kind_final_window", raw_last);
            }
        }
        println!(
            "note raw_bytes_* assumes 8-byte timestamps + raw values; encoded_payload_* uses varint timestamps and codec output"
        );
    }
    if head_metrics.encoded_bytes_total > 0 {
        let avg_bytes_total = avg_bytes_per_sample(
            head_metrics.encoded_bytes_total,
            head_metrics.encoded_samples_total,
        );
        let avg_payload_bytes_total = avg_bytes_per_sample(
            head_metrics.encoded_payload_bytes_total,
            head_metrics.encoded_samples_total,
        );
        let overhead_bytes_total = head_metrics
            .encoded_bytes_total
            .saturating_sub(head_metrics.encoded_payload_bytes_total);
        let avg_overhead_bytes_total =
            avg_bytes_per_sample(overhead_bytes_total, head_metrics.encoded_samples_total);
        println!(
            "estimated_bytes_total={} ({}) total_samples={} total_series={} avg_bytes_per_sample_total={}",
            head_metrics.encoded_bytes_total,
            format_bytes(head_metrics.encoded_bytes_total),
            head_metrics.encoded_samples_total,
            head_metrics.encoded_series_total,
            avg_bytes_total
        );
        println!(
            "encoded_payload_bytes_total={} ({}) avg_bytes_per_sample_payload={} overhead_bytes_total={} ({}) avg_bytes_per_sample_overhead={}",
            head_metrics.encoded_payload_bytes_total,
            format_bytes(head_metrics.encoded_payload_bytes_total),
            avg_payload_bytes_total,
            overhead_bytes_total,
            format_bytes(overhead_bytes_total),
            avg_overhead_bytes_total
        );
        let avg_bytes_last = avg_bytes_per_sample(
            head_metrics.last_window_bytes,
            head_metrics.last_window_samples,
        );
        let avg_payload_bytes_last = avg_bytes_per_sample(
            head_metrics.last_window_payload_bytes,
            head_metrics.last_window_samples,
        );
        let overhead_bytes_last = head_metrics
            .last_window_bytes
            .saturating_sub(head_metrics.last_window_payload_bytes);
        let avg_overhead_bytes_last =
            avg_bytes_per_sample(overhead_bytes_last, head_metrics.last_window_samples);
        println!(
            "estimated_bytes_final_window={} ({}) window_samples={} window_series={} avg_bytes_per_sample_window={}",
            head_metrics.last_window_bytes,
            format_bytes(head_metrics.last_window_bytes),
            head_metrics.last_window_samples,
            head_metrics.last_window_series,
            avg_bytes_last
        );
        println!(
            "encoded_payload_bytes_final_window={} ({}) avg_bytes_per_sample_payload={} overhead_bytes_final_window={} ({}) avg_bytes_per_sample_overhead={}",
            head_metrics.last_window_payload_bytes,
            format_bytes(head_metrics.last_window_payload_bytes),
            avg_payload_bytes_last,
            overhead_bytes_last,
            format_bytes(overhead_bytes_last),
            avg_overhead_bytes_last
        );
        if head_metrics.encoded_bytes_max > 0 {
            println!(
                "estimated_bytes_max_window={} ({})",
                head_metrics.encoded_bytes_max,
                format_bytes(head_metrics.encoded_bytes_max)
            );
        }
        if head_metrics.encoded_payload_bytes_max > 0 {
            println!(
                "encoded_payload_bytes_max_window={} ({})",
                head_metrics.encoded_payload_bytes_max,
                format_bytes(head_metrics.encoded_payload_bytes_max)
            );
        }
        if head_metrics.encoded_bytes_by_kind_total.total() > 0 {
            print_bytes_by_kind(
                "estimated_bytes_by_kind_total",
                head_metrics.encoded_bytes_by_kind_total,
            );
        }
        if head_metrics.encoded_payload_bytes_by_kind_total.total() > 0 {
            print_bytes_by_kind(
                "encoded_payload_bytes_by_kind_total",
                head_metrics.encoded_payload_bytes_by_kind_total,
            );
        }
        if head_metrics.last_window_bytes_by_kind.total() > 0 {
            print_bytes_by_kind(
                "estimated_bytes_by_kind_final_window",
                head_metrics.last_window_bytes_by_kind,
            );
        }
        if head_metrics.last_window_payload_bytes_by_kind.total() > 0 {
            print_bytes_by_kind(
                "encoded_payload_bytes_by_kind_final_window",
                head_metrics.last_window_payload_bytes_by_kind,
            );
        }
        if raw_total_bytes > 0 {
            let payload_ratio =
                head_metrics.encoded_payload_bytes_total as f64 / raw_total_bytes as f64;
            println!("encoded_payload_to_raw_ratio={payload_ratio:.3}");
        }
    }
    if head_metrics.arena_capacity_total > 0 {
        let avg_arena_capacity = avg_bytes_per_sample(
            head_metrics.arena_capacity_total,
            head_metrics.encoded_samples_total,
        );
        let avg_arena_used = avg_bytes_per_sample(
            head_metrics.arena_used_total,
            head_metrics.encoded_samples_total,
        );
        let avg_arena_slack = avg_bytes_per_sample(
            head_metrics.arena_slack_total,
            head_metrics.encoded_samples_total,
        );
        println!(
            "arena_capacity_total={} ({}) arena_used_total={} ({}) arena_slack_total={} ({}) avg_arena_capacity_per_sample={} avg_arena_used_per_sample={} avg_arena_slack_per_sample={}",
            head_metrics.arena_capacity_total,
            format_bytes(head_metrics.arena_capacity_total),
            head_metrics.arena_used_total,
            format_bytes(head_metrics.arena_used_total),
            head_metrics.arena_slack_total,
            format_bytes(head_metrics.arena_slack_total),
            avg_arena_capacity,
            avg_arena_used,
            avg_arena_slack
        );
        println!(
            "arena_final_window_capacity={} ({}) arena_final_window_used={} ({}) arena_final_window_slack={} ({}) arena_final_window_pages={}",
            head_metrics.last_window_arena_capacity,
            format_bytes(head_metrics.last_window_arena_capacity),
            head_metrics.last_window_arena_used,
            format_bytes(head_metrics.last_window_arena_used),
            head_metrics.last_window_arena_slack,
            format_bytes(head_metrics.last_window_arena_slack),
            head_metrics.last_window_arena_pages
        );
        if head_metrics.arena_capacity_max > 0 {
            println!(
                "arena_max_window_capacity={} ({}) arena_max_window_used={} ({}) arena_max_window_slack={} ({})",
                head_metrics.arena_capacity_max,
                format_bytes(head_metrics.arena_capacity_max),
                head_metrics.arena_used_max,
                format_bytes(head_metrics.arena_used_max),
                head_metrics.arena_slack_max,
                format_bytes(head_metrics.arena_slack_max)
            );
        }
    }
    if counters.decode_errors > 0 || counters.labelset_errors > 0 || counters.skipped_non_scalar > 0
    {
        println!(
            "errors decode_errors={} labelset_errors={} skipped_non_scalar={}",
            counters.decode_errors, counters.labelset_errors, counters.skipped_non_scalar
        );
    }
}

fn print_summary_markdown(
    args: &Args,
    counters: &Counters,
    head_metrics: &HeadMetrics,
    labelset_store: &str,
    series_count: usize,
    elapsed: Duration,
) {
    let seconds = elapsed.as_secs_f64();
    let msg_rate = if seconds > 0.0 {
        counters.messages as f64 / seconds
    } else {
        0.0
    };
    let dp_rate = if seconds > 0.0 {
        counters.datapoints_recorded as f64 / seconds
    } else {
        0.0
    };
    let avg_call = avg_duration(head_metrics.head_time_ns, head_metrics.head_calls);
    let avg_sample = avg_duration(head_metrics.head_time_ns, head_metrics.head_samples);

    println!("# HeadBuffer replay\n");
    print_markdown_kv_table(
        "Config",
        vec![
            ("capture", args.capture_path.display().to_string()),
            ("float_encoding", format!("{:?}", args.float_encoding)),
            ("int_encoding", format!("{:?}", args.int_encoding)),
            ("varlen_encoding", format!("{:?}", args.varlen_encoding)),
            ("mode", format!("{:?}", args.mode)),
            ("labelset_store", labelset_store.to_string()),
            (
                "stop_after_messages",
                format!("{:?}", args.stop_after_messages),
            ),
            ("partitions", format!("{:?}", args.partitions)),
        ],
    );
    print_markdown_kv_table(
        "Counts",
        vec![
            ("messages", counters.messages.to_string()),
            ("datapoints_total", counters.datapoints_total.to_string()),
            (
                "datapoints_recorded",
                counters.datapoints_recorded.to_string(),
            ),
            ("series", series_count.to_string()),
        ],
    );
    print_markdown_kv_table(
        "Samples",
        vec![
            ("samples_float", counters.float_samples.to_string()),
            ("samples_int", counters.int_samples.to_string()),
            ("samples_histogram", counters.histogram_samples.to_string()),
            (
                "samples_exponential_histogram",
                counters.exp_histogram_samples.to_string(),
            ),
            ("samples_summary", counters.summary_samples.to_string()),
        ],
    );
    print_markdown_kv_table(
        "Throughput",
        vec![
            ("elapsed", format!("{elapsed:?}")),
            ("msg/s", format!("{msg_rate:.2}")),
            ("dp/s", format!("{dp_rate:.2}")),
        ],
    );
    print_markdown_kv_table(
        "Head Buffer",
        vec![
            ("head_calls", head_metrics.head_calls.to_string()),
            ("head_samples", head_metrics.head_samples.to_string()),
            ("windows_flushed", head_metrics.windows_flushed.to_string()),
            (
                "head_time_total",
                format_duration_ns(head_metrics.head_time_ns),
            ),
            ("avg_per_call", avg_call),
            ("avg_per_sample", avg_sample),
        ],
    );

    let mut dist_rows = Vec::new();
    if let Some(dist) = head_metrics.call_latency.summarize() {
        dist_rows.push(dist.to_markdown_row("head_call_latency"));
    }
    if let Some(dist) = head_metrics.batch_sizes.summarize() {
        dist_rows.push(dist.to_markdown_row("batch_sizes"));
    }
    if let Some(dist) = head_metrics.series_sample_counts.summarize() {
        dist_rows.push(dist.to_markdown_row("series_sample_counts"));
    }
    print_markdown_dist_table("Distributions", dist_rows);

    if head_metrics.series_sample_counts.count() > 0 {
        let series_total = head_metrics.series_sample_counts.count();
        let single = head_metrics.series_single_sample_count;
        let single_ratio = if series_total > 0 {
            single as f64 / series_total as f64
        } else {
            0.0
        };
        print_markdown_kv_table(
            "Series Density",
            vec![
                ("series_single_sample_count", single.to_string()),
                ("series_single_sample_ratio", format!("{single_ratio:.3}")),
                (
                    "series_multi_sample_count",
                    head_metrics.series_multi_sample_count.to_string(),
                ),
            ],
        );
    }

    let raw_total_bytes = counters
        .raw_ts_bytes
        .saturating_add(counters.raw_value_bytes);
    if counters.datapoints_recorded > 0 && raw_total_bytes > 0 {
        let raw_avg = avg_bytes_per_sample(raw_total_bytes, counters.datapoints_recorded);
        print_markdown_kv_table(
            "Raw Bytes",
            vec![
                (
                    "raw_bytes_total",
                    format!("{raw_total_bytes} ({})", format_bytes(raw_total_bytes)),
                ),
                ("raw_bytes_per_sample", raw_avg),
                (
                    "raw_ts_bytes",
                    format!(
                        "{} ({})",
                        counters.raw_ts_bytes,
                        format_bytes(counters.raw_ts_bytes)
                    ),
                ),
                (
                    "raw_value_bytes",
                    format!(
                        "{} ({})",
                        counters.raw_value_bytes,
                        format_bytes(counters.raw_value_bytes)
                    ),
                ),
            ],
        );
        if counters.raw_bytes_by_kind_total.total() > 0 {
            print_markdown_bytes_by_kind(
                "Raw Bytes by Kind (Total)",
                counters.raw_bytes_by_kind_total,
            );
        }
        if let Some(start_ms) = head_metrics.last_window_start_ms {
            let raw_last = counters
                .raw_bytes_by_window
                .get(&start_ms)
                .copied()
                .unwrap_or_default();
            if raw_last.total() > 0 {
                print_markdown_bytes_by_kind("Raw Bytes by Kind (Final Window)", raw_last);
            }
        }
        println!(
            "> note raw_bytes_* assumes 8-byte timestamps + raw values; encoded_payload_* uses varint timestamps and codec output\n"
        );
    }

    if head_metrics.encoded_bytes_total > 0 {
        let avg_bytes_total = avg_bytes_per_sample(
            head_metrics.encoded_bytes_total,
            head_metrics.encoded_samples_total,
        );
        let avg_payload_bytes_total = avg_bytes_per_sample(
            head_metrics.encoded_payload_bytes_total,
            head_metrics.encoded_samples_total,
        );
        let overhead_bytes_total = head_metrics
            .encoded_bytes_total
            .saturating_sub(head_metrics.encoded_payload_bytes_total);
        let avg_overhead_bytes_total =
            avg_bytes_per_sample(overhead_bytes_total, head_metrics.encoded_samples_total);
        print_markdown_kv_table(
            "Encoded Totals",
            vec![
                (
                    "total_samples",
                    head_metrics.encoded_samples_total.to_string(),
                ),
                (
                    "total_series",
                    head_metrics.encoded_series_total.to_string(),
                ),
                (
                    "estimated_bytes_total",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        head_metrics.encoded_bytes_total,
                        format_bytes(head_metrics.encoded_bytes_total),
                        avg_bytes_total
                    ),
                ),
                (
                    "encoded_payload_bytes_total",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        head_metrics.encoded_payload_bytes_total,
                        format_bytes(head_metrics.encoded_payload_bytes_total),
                        avg_payload_bytes_total
                    ),
                ),
                (
                    "overhead_bytes_total",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        overhead_bytes_total,
                        format_bytes(overhead_bytes_total),
                        avg_overhead_bytes_total
                    ),
                ),
            ],
        );

        let avg_bytes_last = avg_bytes_per_sample(
            head_metrics.last_window_bytes,
            head_metrics.last_window_samples,
        );
        let avg_payload_bytes_last = avg_bytes_per_sample(
            head_metrics.last_window_payload_bytes,
            head_metrics.last_window_samples,
        );
        let overhead_bytes_last = head_metrics
            .last_window_bytes
            .saturating_sub(head_metrics.last_window_payload_bytes);
        let avg_overhead_bytes_last =
            avg_bytes_per_sample(overhead_bytes_last, head_metrics.last_window_samples);
        print_markdown_kv_table(
            "Encoded Final Window",
            vec![
                (
                    "estimated_bytes_final_window",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        head_metrics.last_window_bytes,
                        format_bytes(head_metrics.last_window_bytes),
                        avg_bytes_last
                    ),
                ),
                (
                    "encoded_payload_bytes_final_window",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        head_metrics.last_window_payload_bytes,
                        format_bytes(head_metrics.last_window_payload_bytes),
                        avg_payload_bytes_last
                    ),
                ),
                (
                    "overhead_bytes_final_window",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        overhead_bytes_last,
                        format_bytes(overhead_bytes_last),
                        avg_overhead_bytes_last
                    ),
                ),
                (
                    "window_samples",
                    head_metrics.last_window_samples.to_string(),
                ),
                ("window_series", head_metrics.last_window_series.to_string()),
            ],
        );

        if head_metrics.encoded_bytes_max > 0 || head_metrics.encoded_payload_bytes_max > 0 {
            print_markdown_kv_table(
                "Encoded Max Window",
                vec![
                    (
                        "estimated_bytes_max_window",
                        format!(
                            "{} ({})",
                            head_metrics.encoded_bytes_max,
                            format_bytes(head_metrics.encoded_bytes_max)
                        ),
                    ),
                    (
                        "encoded_payload_bytes_max_window",
                        format!(
                            "{} ({})",
                            head_metrics.encoded_payload_bytes_max,
                            format_bytes(head_metrics.encoded_payload_bytes_max)
                        ),
                    ),
                ],
            );
        }

        if head_metrics.encoded_bytes_by_kind_total.total() > 0 {
            print_markdown_bytes_by_kind(
                "Estimated Bytes by Kind (Total)",
                head_metrics.encoded_bytes_by_kind_total,
            );
        }
        if head_metrics.encoded_payload_bytes_by_kind_total.total() > 0 {
            print_markdown_bytes_by_kind(
                "Encoded Payload Bytes by Kind (Total)",
                head_metrics.encoded_payload_bytes_by_kind_total,
            );
        }
        if head_metrics.last_window_bytes_by_kind.total() > 0 {
            print_markdown_bytes_by_kind(
                "Estimated Bytes by Kind (Final Window)",
                head_metrics.last_window_bytes_by_kind,
            );
        }
        if head_metrics.last_window_payload_bytes_by_kind.total() > 0 {
            print_markdown_bytes_by_kind(
                "Encoded Payload Bytes by Kind (Final Window)",
                head_metrics.last_window_payload_bytes_by_kind,
            );
        }

        if raw_total_bytes > 0 {
            let payload_ratio =
                head_metrics.encoded_payload_bytes_total as f64 / raw_total_bytes as f64;
            print_markdown_kv_table(
                "Compression",
                vec![(
                    "encoded_payload_to_raw_ratio",
                    format!("{payload_ratio:.3}"),
                )],
            );
        }
    }

    if head_metrics.arena_capacity_total > 0 {
        let avg_arena_capacity = avg_bytes_per_sample(
            head_metrics.arena_capacity_total,
            head_metrics.encoded_samples_total,
        );
        let avg_arena_used = avg_bytes_per_sample(
            head_metrics.arena_used_total,
            head_metrics.encoded_samples_total,
        );
        let avg_arena_slack = avg_bytes_per_sample(
            head_metrics.arena_slack_total,
            head_metrics.encoded_samples_total,
        );
        print_markdown_kv_table(
            "Arena Totals",
            vec![
                (
                    "arena_capacity_total",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        head_metrics.arena_capacity_total,
                        format_bytes(head_metrics.arena_capacity_total),
                        avg_arena_capacity
                    ),
                ),
                (
                    "arena_used_total",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        head_metrics.arena_used_total,
                        format_bytes(head_metrics.arena_used_total),
                        avg_arena_used
                    ),
                ),
                (
                    "arena_slack_total",
                    format!(
                        "{} ({}) avg_per_sample={}",
                        head_metrics.arena_slack_total,
                        format_bytes(head_metrics.arena_slack_total),
                        avg_arena_slack
                    ),
                ),
            ],
        );
        print_markdown_kv_table(
            "Arena Final Window",
            vec![
                (
                    "arena_final_window_capacity",
                    format!(
                        "{} ({})",
                        head_metrics.last_window_arena_capacity,
                        format_bytes(head_metrics.last_window_arena_capacity)
                    ),
                ),
                (
                    "arena_final_window_used",
                    format!(
                        "{} ({})",
                        head_metrics.last_window_arena_used,
                        format_bytes(head_metrics.last_window_arena_used)
                    ),
                ),
                (
                    "arena_final_window_slack",
                    format!(
                        "{} ({})",
                        head_metrics.last_window_arena_slack,
                        format_bytes(head_metrics.last_window_arena_slack)
                    ),
                ),
                (
                    "arena_final_window_pages",
                    head_metrics.last_window_arena_pages.to_string(),
                ),
            ],
        );
        if head_metrics.arena_capacity_max > 0 {
            print_markdown_kv_table(
                "Arena Max Window",
                vec![
                    (
                        "arena_max_window_capacity",
                        format!(
                            "{} ({})",
                            head_metrics.arena_capacity_max,
                            format_bytes(head_metrics.arena_capacity_max)
                        ),
                    ),
                    (
                        "arena_max_window_used",
                        format!(
                            "{} ({})",
                            head_metrics.arena_used_max,
                            format_bytes(head_metrics.arena_used_max)
                        ),
                    ),
                    (
                        "arena_max_window_slack",
                        format!(
                            "{} ({})",
                            head_metrics.arena_slack_max,
                            format_bytes(head_metrics.arena_slack_max)
                        ),
                    ),
                ],
            );
        }
    }

    if counters.decode_errors > 0 || counters.labelset_errors > 0 || counters.skipped_non_scalar > 0
    {
        print_markdown_kv_table(
            "Errors",
            vec![
                ("decode_errors", counters.decode_errors.to_string()),
                ("labelset_errors", counters.labelset_errors.to_string()),
                (
                    "skipped_non_scalar",
                    counters.skipped_non_scalar.to_string(),
                ),
            ],
        );
    }
}

fn avg_duration(total_ns: u128, denom: u64) -> String {
    if denom == 0 {
        return "n/a".to_string();
    }
    let nanos = total_ns / denom as u128;
    let nanos = nanos.min(u128::from(u64::MAX)) as u64;
    format!("{:?}", Duration::from_nanos(nanos))
}

fn format_duration_ns(total_ns: u128) -> String {
    let nanos = total_ns.min(u128::from(u64::MAX)) as u64;
    format!("{:?}", Duration::from_nanos(nanos))
}

fn print_bytes_by_kind(label: &str, bytes: BytesByKind) {
    println!(
        "{label} float={} ({}) int={} ({}) histogram={} ({}) exponential_histogram={} ({}) summary={} ({})",
        bytes.float,
        format_bytes(bytes.float),
        bytes.int,
        format_bytes(bytes.int),
        bytes.histogram,
        format_bytes(bytes.histogram),
        bytes.exponential_histogram,
        format_bytes(bytes.exponential_histogram),
        bytes.summary,
        format_bytes(bytes.summary)
    );
}

fn print_markdown_kv_table(title: &str, rows: Vec<(&str, String)>) {
    if rows.is_empty() {
        return;
    }
    println!("## {title}");
    println!("| metric | value |");
    println!("| --- | --- |");
    for (metric, value) in rows {
        println!("| {metric} | {value} |");
    }
    println!();
}

fn print_markdown_bytes_by_kind(title: &str, bytes: BytesByKind) {
    println!("## {title}");
    println!("| kind | bytes | human |");
    println!("| --- | --- | --- |");
    println!(
        "| float | {} | {} |",
        bytes.float,
        format_bytes(bytes.float)
    );
    println!("| int | {} | {} |", bytes.int, format_bytes(bytes.int));
    println!(
        "| histogram | {} | {} |",
        bytes.histogram,
        format_bytes(bytes.histogram)
    );
    println!(
        "| exponential_histogram | {} | {} |",
        bytes.exponential_histogram,
        format_bytes(bytes.exponential_histogram)
    );
    println!(
        "| summary | {} | {} |",
        bytes.summary,
        format_bytes(bytes.summary)
    );
    println!();
}

fn print_markdown_dist_table(title: &str, rows: Vec<String>) {
    if rows.is_empty() {
        return;
    }
    println!("## {title}");
    println!("| metric | n | mean | stddev | min | max | p50 | p75 | p95 | p99 |");
    println!("| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |");
    for row in rows {
        print!("{row}");
    }
    println!();
}

fn avg_bytes_per_sample(bytes: u64, samples: u64) -> String {
    if samples == 0 {
        return "n/a".to_string();
    }
    let avg = bytes as f64 / samples as f64;
    format!("{avg:.2}")
}

fn format_bytes(bytes: u64) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * 1024.0;
    const GIB: f64 = 1024.0 * 1024.0 * 1024.0;

    let value = bytes as f64;
    if value >= GIB {
        format!("{:.2} GiB", value / GIB)
    } else if value >= MIB {
        format!("{:.2} MiB", value / MIB)
    } else if value >= KIB {
        format!("{:.2} KiB", value / KIB)
    } else {
        format!("{bytes} B")
    }
}
