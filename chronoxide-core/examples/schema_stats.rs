use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;

use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, LabelSetStoreError, SeriesRef, TmpLabel,
};
use chronoxide_core::otlp::datapoint_time_ms;
use chronoxide_core::otlp_capture::OtlpCaptureReader;
use chronoxide_core::otlp_labelset::{CanonicalLabelSet, OtlpLabelSetInterner, intern_labelset};
use chronoxide_core::statistics::{
    DEFAULT_TDIGEST_BUFFER_CAPACITY, DEFAULT_TDIGEST_MAX_CENTROIDS, DistU64, Stats,
};
use chronoxide_core::storage::encoding::{encode_zigzag_i64, varint_len};
use clap::{Parser, ValueEnum};
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::metrics::v1::metric::Data as MetricData;
use opentelemetry_proto::tonic::metrics::v1::{
    ExponentialHistogramDataPoint, HistogramDataPoint, SummaryDataPoint,
};
use prost::Message;
use smallvec::SmallVec;

type ExampleResult<T> = Result<T, Box<dyn std::error::Error>>;

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

    fn intern(&mut self, labels: &[KeyValueRef<'_>]) -> Result<SeriesRef, LabelSetStoreError> {
        match self {
            Self::Flat(store) => store.intern(labels),
            Self::KeySet(store) => store.intern(labels),
        }
    }
}

#[derive(Parser, Debug)]
#[command(
    name = "schema_stats",
    about = "Analyze OTLP schema stability for histogram, exponential histogram, and summary"
)]
struct Args {
    #[arg(short, long, value_name = "PATH")]
    capture_path: PathBuf,
    #[arg(long, value_enum, default_value_t = LabelSetStoreKindArg::FlatInterned)]
    labelset_store: LabelSetStoreKindArg,
    #[arg(long, default_value_t = 1024)]
    block_size: u32,
    #[arg(long = "stop-after-messages", alias = "stop-after")]
    stop_after_messages: Option<u64>,
    #[arg(long)]
    partition: Option<i32>,
}

#[derive(Default)]
struct Counters {
    messages: u64,
    datapoints_total: u64,
    datapoints_tracked: u64,
    decode_errors: u64,
    labelset_errors: u64,
    skipped_non_scalar: u64,
}

#[derive(Default)]
struct ScaleStats {
    count: u64,
    min: i32,
    max: i32,
    sum: i64,
}

impl ScaleStats {
    fn record(&mut self, value: i32) {
        if self.count == 0 {
            self.min = value;
            self.max = value;
        } else {
            if value < self.min {
                self.min = value;
            }
            if value > self.max {
                self.max = value;
            }
        }
        self.sum += value as i64;
        self.count += 1;
    }

    fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum as f64 / self.count as f64
        }
    }
}

struct SeriesState {
    last_schema: u64,
    block_schema: u64,
    block_samples: u32,
    samples: u64,
    changes: u64,
    has_multiple: bool,
    schemas: SmallVec<[u64; 2]>,
}

struct SchemaTracker {
    block_size: u32,
    series: HashMap<SeriesRef, SeriesState>,
    samples_total: u64,
    series_total: u64,
    series_with_changes: u64,
    changes_total: u64,
    schema_bytes_per_sample_total: u64,
    schema_bytes_per_block_total: u64,
    schema_bytes_per_series_total: u64,
    blocks_total: u64,
}

impl SchemaTracker {
    fn new(block_size: u32) -> Self {
        Self {
            block_size,
            series: HashMap::new(),
            samples_total: 0,
            series_total: 0,
            series_with_changes: 0,
            changes_total: 0,
            schema_bytes_per_sample_total: 0,
            schema_bytes_per_block_total: 0,
            schema_bytes_per_series_total: 0,
            blocks_total: 0,
        }
    }

    fn record_sample(&mut self, series: SeriesRef, schema_hash: u64, schema_bytes: u64) {
        self.samples_total = self.samples_total.saturating_add(1);
        self.schema_bytes_per_sample_total = self
            .schema_bytes_per_sample_total
            .saturating_add(schema_bytes);

        match self.series.get_mut(&series) {
            None => {
                let mut schemas = SmallVec::new();
                schemas.push(schema_hash);
                self.series.insert(
                    series,
                    SeriesState {
                        last_schema: schema_hash,
                        block_schema: schema_hash,
                        block_samples: 1,
                        samples: 1,
                        changes: 0,
                        has_multiple: false,
                        schemas,
                    },
                );
                self.series_total = self.series_total.saturating_add(1);
                self.schema_bytes_per_series_total = self
                    .schema_bytes_per_series_total
                    .saturating_add(schema_bytes);
                self.schema_bytes_per_block_total = self
                    .schema_bytes_per_block_total
                    .saturating_add(schema_bytes);
                self.blocks_total = self.blocks_total.saturating_add(1);
            }
            Some(state) => {
                state.samples = state.samples.saturating_add(1);
                if schema_hash != state.last_schema {
                    state.changes = state.changes.saturating_add(1);
                    state.last_schema = schema_hash;
                    self.changes_total = self.changes_total.saturating_add(1);
                    if !state.has_multiple {
                        state.has_multiple = true;
                        self.series_with_changes = self.series_with_changes.saturating_add(1);
                    }
                }

                if !state.schemas.iter().any(|hash| *hash == schema_hash) {
                    state.schemas.push(schema_hash);
                    self.schema_bytes_per_series_total = self
                        .schema_bytes_per_series_total
                        .saturating_add(schema_bytes);
                }

                if state.block_samples >= self.block_size || schema_hash != state.block_schema {
                    state.block_schema = schema_hash;
                    state.block_samples = 0;
                    self.schema_bytes_per_block_total = self
                        .schema_bytes_per_block_total
                        .saturating_add(schema_bytes);
                    self.blocks_total = self.blocks_total.saturating_add(1);
                }
                state.block_samples = state.block_samples.saturating_add(1);
            }
        }
    }

    fn summarize(&self) -> SchemaSummary {
        let mut changes = Stats::<u64>::new_tdigest(
            DEFAULT_TDIGEST_MAX_CENTROIDS,
            DEFAULT_TDIGEST_BUFFER_CAPACITY,
        );
        let mut distinct = Stats::<u64>::new_tdigest(
            DEFAULT_TDIGEST_MAX_CENTROIDS,
            DEFAULT_TDIGEST_BUFFER_CAPACITY,
        );
        let mut samples_per_run = Stats::<u64>::new_tdigest(
            DEFAULT_TDIGEST_MAX_CENTROIDS,
            DEFAULT_TDIGEST_BUFFER_CAPACITY,
        );

        for state in self.series.values() {
            changes.insert(state.changes);
            distinct.insert(state.schemas.len() as u64);
            let runs = state.changes.saturating_add(1);
            let per_run = if runs > 0 { state.samples / runs } else { 0 };
            samples_per_run.insert(per_run);
        }

        SchemaSummary {
            changes: changes.summarize(),
            distinct: distinct.summarize(),
            samples_per_run: samples_per_run.summarize(),
        }
    }
}

struct SchemaSummary {
    changes: Option<DistU64>,
    distinct: Option<DistU64>,
    samples_per_run: Option<DistU64>,
}

struct HistogramTracker {
    base: SchemaTracker,
    bounds_len: Stats<u64>,
    bucket_len: Stats<u64>,
}

impl HistogramTracker {
    fn new(block_size: u32) -> Self {
        Self {
            base: SchemaTracker::new(block_size),
            bounds_len: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            bucket_len: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
        }
    }
}

struct ExponentialHistogramTracker {
    base: SchemaTracker,
    pos_len: Stats<u64>,
    neg_len: Stats<u64>,
    scale: ScaleStats,
}

impl ExponentialHistogramTracker {
    fn new(block_size: u32) -> Self {
        Self {
            base: SchemaTracker::new(block_size),
            pos_len: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            neg_len: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
            scale: ScaleStats::default(),
        }
    }
}

struct SummaryTracker {
    base: SchemaTracker,
    quantile_len: Stats<u64>,
}

impl SummaryTracker {
    fn new(block_size: u32) -> Self {
        Self {
            base: SchemaTracker::new(block_size),
            quantile_len: Stats::new_tdigest(
                DEFAULT_TDIGEST_MAX_CENTROIDS,
                DEFAULT_TDIGEST_BUFFER_CAPACITY,
            ),
        }
    }
}

fn main() -> ExampleResult<()> {
    let args = Args::parse();
    let mut reader = OtlpCaptureReader::open(&args.capture_path)?;
    let mut labelsets = LabelSetStoreWrapper::new(args.labelset_store);
    let mut counters = Counters::default();
    let mut hist = HistogramTracker::new(args.block_size);
    let mut exphist = ExponentialHistogramTracker::new(args.block_size);
    let mut summary = SummaryTracker::new(args.block_size);

    loop {
        let Some(msg) = reader.next()? else {
            break;
        };
        if let Some(partition) = args.partition {
            if msg.partition != partition {
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
        process_request(
            &mut labelsets,
            &decoded,
            &mut counters,
            &mut hist,
            &mut exphist,
            &mut summary,
        );

        if let Some(stop_after) = args.stop_after_messages
            && counters.messages >= stop_after
        {
            break;
        }
    }

    print_summary(&args, &counters, &hist, &exphist, &summary);
    Ok(())
}

fn process_request(
    labelsets: &mut LabelSetStoreWrapper,
    req: &ExportMetricsServiceRequest,
    counters: &mut Counters,
    hist: &mut HistogramTracker,
    exphist: &mut ExponentialHistogramTracker,
    summary: &mut SummaryTracker,
) {
    let mut scratch_values: Vec<Box<str>> = Vec::new();
    let mut tmp_labels: Vec<TmpLabel<'_>> = Vec::new();

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
                    MetricData::Histogram(histogram) => {
                        for dp in &histogram.data_points {
                            counters.datapoints_total = counters.datapoints_total.saturating_add(1);
                            if datapoint_time_ms(dp.time_unix_nano).is_none() {
                                continue;
                            }
                            let series = intern_labelset_with(
                                labelsets,
                                counters,
                                resource_attrs,
                                metric_name,
                                &dp.attributes,
                                &mut scratch_values,
                                &mut tmp_labels,
                            );
                            if let Some(series) = series {
                                counters.datapoints_tracked =
                                    counters.datapoints_tracked.saturating_add(1);
                                record_histogram(hist, series, dp);
                            }
                        }
                    }
                    MetricData::ExponentialHistogram(histogram) => {
                        for dp in &histogram.data_points {
                            counters.datapoints_total = counters.datapoints_total.saturating_add(1);
                            if datapoint_time_ms(dp.time_unix_nano).is_none() {
                                continue;
                            }
                            let series = intern_labelset_with(
                                labelsets,
                                counters,
                                resource_attrs,
                                metric_name,
                                &dp.attributes,
                                &mut scratch_values,
                                &mut tmp_labels,
                            );
                            if let Some(series) = series {
                                counters.datapoints_tracked =
                                    counters.datapoints_tracked.saturating_add(1);
                                record_exponential_histogram(exphist, series, dp);
                            }
                        }
                    }
                    MetricData::Summary(summary_data) => {
                        for dp in &summary_data.data_points {
                            counters.datapoints_total = counters.datapoints_total.saturating_add(1);
                            if datapoint_time_ms(dp.time_unix_nano).is_none() {
                                continue;
                            }
                            let series = intern_labelset_with(
                                labelsets,
                                counters,
                                resource_attrs,
                                metric_name,
                                &dp.attributes,
                                &mut scratch_values,
                                &mut tmp_labels,
                            );
                            if let Some(series) = series {
                                counters.datapoints_tracked =
                                    counters.datapoints_tracked.saturating_add(1);
                                record_summary(summary, series, dp);
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

struct SchemaStatsInterner<'a> {
    labelsets: &'a mut LabelSetStoreWrapper,
    counters: &'a mut Counters,
}

impl<'a> OtlpLabelSetInterner for SchemaStatsInterner<'a> {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {
        self.counters.skipped_non_scalar = self.counters.skipped_non_scalar.saturating_add(1);
    }

    fn on_intern_error(&mut self, _error: Self::Error) {
        self.counters.labelset_errors = self.counters.labelset_errors.saturating_add(1);
    }

    fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
        let labels = labels.iter().collect::<Vec<_>>();
        self.labelsets.intern(labels.as_slice())
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
    let mut interner = SchemaStatsInterner {
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

fn record_histogram(tracker: &mut HistogramTracker, series: SeriesRef, dp: &HistogramDataPoint) {
    let bounds_len = dp.explicit_bounds.len();
    let bucket_len = dp.bucket_counts.len();
    let schema_hash = hash_f64_slice(&dp.explicit_bounds);
    let schema_bytes = histogram_schema_bytes(bounds_len as u64, bucket_len as u64);
    tracker
        .base
        .record_sample(series, schema_hash, schema_bytes);
    tracker.bounds_len.insert(bounds_len as u64);
    tracker.bucket_len.insert(bucket_len as u64);
}

fn record_exponential_histogram(
    tracker: &mut ExponentialHistogramTracker,
    series: SeriesRef,
    dp: &ExponentialHistogramDataPoint,
) {
    let pos_len = dp.positive.as_ref().map_or(0, |b| b.bucket_counts.len());
    let neg_len = dp.negative.as_ref().map_or(0, |b| b.bucket_counts.len());
    let schema_hash = hash_exphist_schema(dp.scale, pos_len as u64, neg_len as u64);
    let schema_bytes = exphist_schema_bytes(dp.scale, pos_len as u64, neg_len as u64);
    tracker
        .base
        .record_sample(series, schema_hash, schema_bytes);
    tracker.pos_len.insert(pos_len as u64);
    tracker.neg_len.insert(neg_len as u64);
    tracker.scale.record(dp.scale);
}

fn record_summary(tracker: &mut SummaryTracker, series: SeriesRef, dp: &SummaryDataPoint) {
    let quantile_len = dp.quantile_values.len();
    let schema_hash = hash_summary_schema(&dp.quantile_values);
    let schema_bytes = summary_schema_bytes(quantile_len as u64);
    tracker
        .base
        .record_sample(series, schema_hash, schema_bytes);
    tracker.quantile_len.insert(quantile_len as u64);
}

fn hash_f64_slice(values: &[f64]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    values.len().hash(&mut hasher);
    for value in values {
        value.to_bits().hash(&mut hasher);
    }
    hasher.finish()
}

fn hash_summary_schema(
    quantiles: &[opentelemetry_proto::tonic::metrics::v1::summary_data_point::ValueAtQuantile],
) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    quantiles.len().hash(&mut hasher);
    for quantile in quantiles {
        quantile.quantile.to_bits().hash(&mut hasher);
    }
    hasher.finish()
}

fn hash_exphist_schema(scale: i32, pos_len: u64, neg_len: u64) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    scale.hash(&mut hasher);
    pos_len.hash(&mut hasher);
    neg_len.hash(&mut hasher);
    hasher.finish()
}

fn histogram_schema_bytes(bounds_len: u64, bucket_len: u64) -> u64 {
    (varint_len(bounds_len) as u64)
        .saturating_add(bounds_len.saturating_mul(8))
        .saturating_add(varint_len(bucket_len) as u64)
}

fn exphist_schema_bytes(scale: i32, pos_len: u64, neg_len: u64) -> u64 {
    let scale_bytes = varint_len(encode_zigzag_i64(scale as i64)) as u64;
    scale_bytes
        .saturating_add(varint_len(pos_len) as u64)
        .saturating_add(varint_len(neg_len) as u64)
}

fn summary_schema_bytes(quantile_len: u64) -> u64 {
    (varint_len(quantile_len) as u64).saturating_add(quantile_len.saturating_mul(8))
}

fn print_summary(
    args: &Args,
    counters: &Counters,
    hist: &HistogramTracker,
    exphist: &ExponentialHistogramTracker,
    summary: &SummaryTracker,
) {
    println!("Schema stability report");
    println!(
        "capture={} labelset_store={} block_size={} stop_after_messages={:?} partition={:?}",
        args.capture_path.display(),
        args.labelset_store.as_str(),
        args.block_size,
        args.stop_after_messages,
        args.partition
    );
    println!(
        "messages={} datapoints_total={} datapoints_tracked={}",
        counters.messages, counters.datapoints_total, counters.datapoints_tracked
    );
    if counters.decode_errors > 0 || counters.labelset_errors > 0 || counters.skipped_non_scalar > 0
    {
        println!(
            "errors decode_errors={} labelset_errors={} skipped_non_scalar={}",
            counters.decode_errors, counters.labelset_errors, counters.skipped_non_scalar
        );
    }

    println!("Histogram schema definition: explicit_bounds values + bucket_counts length");
    println!("Histogram distinct_schemas_per_series = unique schema hashes per series");
    print_tracker_report("Histogram", &hist.base, || {
        if let Some(dist) = hist.bounds_len.summarize() {
            println!("bounds_len {}", dist);
        }
        if let Some(dist) = hist.bucket_len.summarize() {
            println!("bucket_len {}", dist);
        }
    });

    println!(
        "ExponentialHistogram schema definition: scale + positive/negative bucket lengths (offset not included)"
    );
    println!("ExponentialHistogram distinct_schemas_per_series = unique schema hashes per series");
    print_tracker_report("ExponentialHistogram", &exphist.base, || {
        if let Some(dist) = exphist.pos_len.summarize() {
            println!("positive_bucket_len {}", dist);
        }
        if let Some(dist) = exphist.neg_len.summarize() {
            println!("negative_bucket_len {}", dist);
        }
        if exphist.scale.count > 0 {
            println!(
                "scale count={} mean={:.2} min={} max={}",
                exphist.scale.count,
                exphist.scale.mean(),
                exphist.scale.min,
                exphist.scale.max
            );
        }
    });

    println!("Summary schema definition: quantile list (quantile positions only)");
    println!("Summary distinct_schemas_per_series = unique schema hashes per series");
    print_tracker_report("Summary", &summary.base, || {
        if let Some(dist) = summary.quantile_len.summarize() {
            println!("quantile_len {}", dist);
        }
    });
}

fn print_tracker_report<F>(name: &str, tracker: &SchemaTracker, extra: F)
where
    F: FnOnce(),
{
    println!("{name} schema");
    println!(
        "series={} samples={} blocks={}",
        tracker.series_total, tracker.samples_total, tracker.blocks_total
    );
    if tracker.series_total > 0 {
        let pct = tracker.series_with_changes as f64 / tracker.series_total as f64 * 100.0;
        println!(
            "series_with_schema_changes={} ({:.2}%) schema_changes_total={}",
            tracker.series_with_changes, pct, tracker.changes_total
        );
    }
    let avg_samples_per_series = if tracker.series_total > 0 {
        tracker.samples_total as f64 / tracker.series_total as f64
    } else {
        0.0
    };
    let runs_total = tracker.changes_total.saturating_add(tracker.series_total);
    let avg_samples_per_run = if runs_total > 0 {
        tracker.samples_total as f64 / runs_total as f64
    } else {
        0.0
    };
    println!(
        "avg_samples_per_series={:.2} avg_samples_per_run={:.2}",
        avg_samples_per_series, avg_samples_per_run
    );

    println!(
        "schema_bytes_per_sample_total={} ({})",
        tracker.schema_bytes_per_sample_total,
        format_bytes(tracker.schema_bytes_per_sample_total)
    );
    if tracker.schema_bytes_per_sample_total > 0 {
        let block_ratio = tracker.schema_bytes_per_block_total as f64
            / tracker.schema_bytes_per_sample_total as f64;
        let series_ratio = tracker.schema_bytes_per_series_total as f64
            / tracker.schema_bytes_per_sample_total as f64;
        println!(
            "schema_bytes_per_block_total={} ({}) ratio={:.3}",
            tracker.schema_bytes_per_block_total,
            format_bytes(tracker.schema_bytes_per_block_total),
            block_ratio
        );
        println!(
            "schema_bytes_per_series_total={} ({}) ratio={:.3}",
            tracker.schema_bytes_per_series_total,
            format_bytes(tracker.schema_bytes_per_series_total),
            series_ratio
        );
    }

    let summary = tracker.summarize();
    if let Some(dist) = summary.changes {
        println!("schema_changes_per_series {}", dist);
    }
    if let Some(dist) = summary.distinct {
        println!("distinct_schemas_per_series {}", dist);
    }
    if let Some(dist) = summary.samples_per_run {
        println!("samples_per_run {}", dist);
    }

    extra();
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
