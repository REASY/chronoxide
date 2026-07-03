use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::io;
use std::sync::Mutex;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{debug, warn};

use crate::labels::{LabelSetStore, METRIC_NAME_LABEL, SeriesRef};
use crate::promql::{canonicalize_labelset, series_id};
use crate::storage::arena::BlockArena;
use crate::storage::block::{
    Block, BlockBuilder, BlockCodec, FloatAlpCodec, FloatAlpRdCodec, FloatAlpRdSpiralCodec,
    FloatAlpSpiralCodec, FloatChimp128BaselineDeferredCodec, FloatChimp128DuckDBDeferredCodec,
    FloatElfCodec, FloatGorillaCodec, FloatRawCodec, IntDeltaCodec, IntRawCodec,
};
use crate::storage::encoding::{
    SchemaVarLenCodec, SchemaVarLenEncoding, VarLenCodec, VarLenEncoding, decode_varint,
    decode_zigzag_i64, encode_varint, encode_zigzag_i64,
};
use crate::storage::segment::{
    MetadataAccumulator, NormalizedMatcher, QueryBudget, SegmentProjection, SegmentQueryResult,
    SegmentSelector, compile_label_matchers, compile_promql_regex, labels_match_compiled,
    promql_projection_metric_name_matches, segment_series_id,
};

#[derive(Debug, Clone)]
pub struct HeadConfig {
    pub window_duration: Duration,
    pub block_size: usize,
    pub float_encoding: FloatEncoding,
    pub int_encoding: IntEncoding,
    pub varlen_encoding: VarLenEncodingKind,
    pub out_of_order_time_window: Duration,
}

impl HeadConfig {
    pub fn new(
        window_duration: Duration,
        float_encoding: FloatEncoding,
        int_encoding: IntEncoding,
    ) -> Self {
        Self {
            window_duration,
            block_size: 1024,
            float_encoding,
            int_encoding,
            varlen_encoding: VarLenEncodingKind::Raw,
            out_of_order_time_window: Duration::ZERO,
        }
    }

    pub fn with_block_size(
        window_duration: Duration,
        block_size: usize,
        float_encoding: FloatEncoding,
        int_encoding: IntEncoding,
    ) -> Self {
        Self {
            window_duration,
            block_size,
            float_encoding,
            int_encoding,
            varlen_encoding: VarLenEncodingKind::Raw,
            out_of_order_time_window: Duration::ZERO,
        }
    }

    pub fn with_varlen_encoding(mut self, varlen_encoding: VarLenEncodingKind) -> Self {
        self.varlen_encoding = varlen_encoding;
        self
    }

    pub fn with_out_of_order_time_window(mut self, window: Duration) -> Self {
        self.out_of_order_time_window = window;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum FloatEncoding {
    Raw,
    Gorilla,
    #[serde(rename = "elf")]
    Elf,
    Alp,
    #[serde(rename = "alp_rd", alias = "alprd", alias = "alp-rd")]
    AlpRd,
    #[serde(rename = "alp_spiral", alias = "alp_spiraldb", alias = "alp_spiral_db")]
    AlpSpiral,
    #[serde(
        rename = "alp_rd_spiral",
        alias = "alp_spiral_rd",
        alias = "alp_rd_spiraldb",
        alias = "alp_rd_spiral_db"
    )]
    AlpRdSpiral,
    #[serde(
        rename = "chimp128_duckdb",
        alias = "chimp128",
        alias = "chimp128_duck_db"
    )]
    Chimp128DuckDB,
    Chimp128Baseline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum IntEncoding {
    Raw,
    DeltaZigZag,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum VarLenEncodingKind {
    Raw,
    Schema,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SampleValue {
    Float(f64),
    Int64(i64),
    Histogram(HistogramValue),
    ExponentialHistogram(ExponentialHistogramValue),
    Summary(SummaryValue),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SampleKind {
    Float,
    Int64,
    Histogram,
    ExponentialHistogram,
    Summary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NumberMetricKind {
    Gauge,
    Sum,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct BytesByKind {
    pub float: u64,
    pub float_gauge: u64,
    pub float_sum: u64,
    pub int: u64,
    pub int_gauge: u64,
    pub int_sum: u64,
    pub histogram: u64,
    pub exponential_histogram: u64,
    pub summary: u64,
}

impl BytesByKind {
    pub fn total(&self) -> u64 {
        self.float
            .saturating_add(self.int)
            .saturating_add(self.histogram)
            .saturating_add(self.exponential_histogram)
            .saturating_add(self.summary)
    }
}

const DEFAULT_HEAD_ARENA_PAGE_BYTES: usize = 4 * 1024 * 1024;

impl SampleValue {
    fn kind(&self) -> SampleKind {
        match self {
            Self::Float(_) => SampleKind::Float,
            Self::Int64(_) => SampleKind::Int64,
            Self::Histogram(_) => SampleKind::Histogram,
            Self::ExponentialHistogram(_) => SampleKind::ExponentialHistogram,
            Self::Summary(_) => SampleKind::Summary,
        }
    }
}

pub const OTLP_FLAG_NO_RECORDED_VALUE: u32 = 1;
pub const PROMETHEUS_STALE_NAN_BITS: u64 = 0x7ff0_0000_0000_0002;

pub fn prometheus_stale_nan() -> f64 {
    f64::from_bits(PROMETHEUS_STALE_NAN_BITS)
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum OtlpAggregationTemporality {
    #[default]
    Unspecified = 0,
    Delta = 1,
    Cumulative = 2,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum CounterResetHint {
    #[default]
    Unknown = 0,
    CounterReset = 1,
    NotCounterReset = 2,
    GaugeType = 3,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TypedSampleMetadata {
    pub start_time_ms: Option<u64>,
    pub flags: u32,
    pub temporality: OtlpAggregationTemporality,
    pub reset_hint: CounterResetHint,
}

impl TypedSampleMetadata {
    pub fn is_stale(self) -> bool {
        self.flags & OTLP_FLAG_NO_RECORDED_VALUE != 0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramValue {
    pub count: u64,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub metadata: TypedSampleMetadata,
    pub explicit_bounds: Vec<f64>,
    pub bucket_counts: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExponentialHistogramValue {
    pub count: u64,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
    pub scale: i32,
    pub zero_threshold: f64,
    pub zero_count: u64,
    pub metadata: TypedSampleMetadata,
    pub positive: ExponentialHistogramBuckets,
    pub negative: ExponentialHistogramBuckets,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExponentialHistogramBuckets {
    pub offset: i32,
    pub counts: Vec<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExponentialHistogramScalePolicy {
    Keep,
    DownscaleToMaxScale(i32),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExponentialHistogramMergeError {
    TargetScaleHigherThanSource {
        source_scale: i32,
        target_scale: i32,
    },
    ScaleDeltaTooLarge,
    BucketIndexOverflow,
    BucketCountOverflow,
    BucketSpanTooWide,
    ZeroThresholdMismatch,
}

impl fmt::Display for ExponentialHistogramMergeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TargetScaleHigherThanSource {
                source_scale,
                target_scale,
            } => write!(
                f,
                "cannot downscale exponential histogram from scale {source_scale} to higher scale {target_scale}"
            ),
            Self::ScaleDeltaTooLarge => write!(f, "exponential histogram scale delta is too large"),
            Self::BucketIndexOverflow => write!(f, "exponential histogram bucket index overflow"),
            Self::BucketCountOverflow => write!(f, "exponential histogram bucket count overflow"),
            Self::BucketSpanTooWide => write!(f, "exponential histogram bucket span is too wide"),
            Self::ZeroThresholdMismatch => write!(
                f,
                "cannot merge exponential histograms with different zero thresholds"
            ),
        }
    }
}

impl std::error::Error for ExponentialHistogramMergeError {}

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryValue {
    pub count: u64,
    pub sum: f64,
    pub metadata: TypedSampleMetadata,
    pub quantiles: Vec<SummaryQuantileValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryQuantileValue {
    pub quantile: f64,
    pub value: f64,
}

impl VarLenEncoding for HistogramValue {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_typed_metadata(self.metadata, out);
        encode_varint(self.count, out);
        encode_opt_f64(self.sum, out);
        encode_opt_f64(self.min, out);
        encode_opt_f64(self.max, out);
        encode_varint(self.explicit_bounds.len() as u64, out);
        for bound in &self.explicit_bounds {
            encode_f64(*bound, out);
        }
        encode_varint(self.bucket_counts.len() as u64, out);
        for count in &self.bucket_counts {
            encode_varint(*count, out);
        }
        Ok(())
    }

    fn decode_from(buf: &[u8]) -> io::Result<Self> {
        let mut cursor = 0usize;
        let metadata = decode_typed_metadata(buf, &mut cursor)?;
        let count = decode_varint(buf, &mut cursor)?;
        let sum = decode_opt_f64(buf, &mut cursor)?;
        let min = decode_opt_f64(buf, &mut cursor)?;
        let max = decode_opt_f64(buf, &mut cursor)?;
        let bounds_len = decode_len(buf, &mut cursor)?;
        let mut explicit_bounds = Vec::with_capacity(bounds_len);
        for _ in 0..bounds_len {
            explicit_bounds.push(decode_f64(buf, &mut cursor)?);
        }
        let counts_len = decode_len(buf, &mut cursor)?;
        let mut bucket_counts = Vec::with_capacity(counts_len);
        for _ in 0..counts_len {
            bucket_counts.push(decode_varint(buf, &mut cursor)?);
        }
        ensure_consumed(buf, cursor)?;
        Ok(Self {
            count,
            sum,
            min,
            max,
            metadata,
            explicit_bounds,
            bucket_counts,
        })
    }
}

impl VarLenEncoding for ExponentialHistogramValue {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_typed_metadata(self.metadata, out);
        encode_varint(self.count, out);
        encode_opt_f64(self.sum, out);
        encode_opt_f64(self.min, out);
        encode_opt_f64(self.max, out);
        encode_varint(encode_zigzag_i64(self.scale as i64), out);
        encode_f64(self.zero_threshold, out);
        encode_varint(self.zero_count, out);
        encode_buckets(&self.positive, out);
        encode_buckets(&self.negative, out);
        Ok(())
    }

    fn decode_from(buf: &[u8]) -> io::Result<Self> {
        let mut cursor = 0usize;
        let metadata = decode_typed_metadata(buf, &mut cursor)?;
        let count = decode_varint(buf, &mut cursor)?;
        let sum = decode_opt_f64(buf, &mut cursor)?;
        let min = decode_opt_f64(buf, &mut cursor)?;
        let max = decode_opt_f64(buf, &mut cursor)?;
        let scale = decode_i32(buf, &mut cursor)?;
        let zero_threshold = decode_f64(buf, &mut cursor)?;
        let zero_count = decode_varint(buf, &mut cursor)?;
        let positive = decode_buckets(buf, &mut cursor)?;
        let negative = decode_buckets(buf, &mut cursor)?;
        ensure_consumed(buf, cursor)?;
        Ok(Self {
            count,
            sum,
            min,
            max,
            scale,
            zero_threshold,
            zero_count,
            metadata,
            positive,
            negative,
        })
    }
}

impl VarLenEncoding for SummaryValue {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_typed_metadata(self.metadata, out);
        encode_varint(self.count, out);
        encode_f64(self.sum, out);
        encode_varint(self.quantiles.len() as u64, out);
        for quantile in &self.quantiles {
            encode_f64(quantile.quantile, out);
            encode_f64(quantile.value, out);
        }
        Ok(())
    }

    fn decode_from(buf: &[u8]) -> io::Result<Self> {
        let mut cursor = 0usize;
        let metadata = decode_typed_metadata(buf, &mut cursor)?;
        let count = decode_varint(buf, &mut cursor)?;
        let sum = decode_f64(buf, &mut cursor)?;
        let quantile_len = decode_len(buf, &mut cursor)?;
        let mut quantiles = Vec::with_capacity(quantile_len);
        for _ in 0..quantile_len {
            let quantile = decode_f64(buf, &mut cursor)?;
            let value = decode_f64(buf, &mut cursor)?;
            quantiles.push(SummaryQuantileValue { quantile, value });
        }
        ensure_consumed(buf, cursor)?;
        Ok(Self {
            count,
            sum,
            metadata,
            quantiles,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct HistogramSchema {
    explicit_bounds: Vec<f64>,
    bucket_len: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ExponentialHistogramSchema {
    scale: i32,
    zero_threshold: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SummarySchema {
    quantiles: Vec<f64>,
}

impl SchemaVarLenEncoding for HistogramValue {
    type Schema = HistogramSchema;

    fn encode_schema_from_value(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_varint(self.explicit_bounds.len() as u64, out);
        for bound in &self.explicit_bounds {
            encode_f64(*bound, out);
        }
        encode_varint(self.bucket_counts.len() as u64, out);
        Ok(())
    }

    fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
        let bounds_len = decode_len(buf, cursor)?;
        let mut explicit_bounds = Vec::with_capacity(bounds_len);
        for _ in 0..bounds_len {
            explicit_bounds.push(decode_f64(buf, cursor)?);
        }
        let bucket_len = decode_len(buf, cursor)?;
        Ok(Self::Schema {
            explicit_bounds,
            bucket_len,
        })
    }

    fn encode_value_with_schema(&self, schema: &Self::Schema, out: &mut Vec<u8>) -> io::Result<()> {
        if self.bucket_counts.len() != schema.bucket_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "histogram bucket length mismatch",
            ));
        }
        encode_typed_metadata(self.metadata, out);
        encode_varint(self.count, out);
        encode_opt_f64(self.sum, out);
        encode_opt_f64(self.min, out);
        encode_opt_f64(self.max, out);
        for count in &self.bucket_counts {
            encode_varint(*count, out);
        }
        Ok(())
    }

    fn decode_value_with_schema(
        schema: &Self::Schema,
        buf: &[u8],
        cursor: &mut usize,
    ) -> io::Result<Self> {
        let metadata = decode_typed_metadata(buf, cursor)?;
        let count = decode_varint(buf, cursor)?;
        let sum = decode_opt_f64(buf, cursor)?;
        let min = decode_opt_f64(buf, cursor)?;
        let max = decode_opt_f64(buf, cursor)?;
        let mut bucket_counts = Vec::with_capacity(schema.bucket_len);
        for _ in 0..schema.bucket_len {
            bucket_counts.push(decode_varint(buf, cursor)?);
        }
        Ok(Self {
            count,
            sum,
            min,
            max,
            metadata,
            explicit_bounds: schema.explicit_bounds.clone(),
            bucket_counts,
        })
    }
}

impl SchemaVarLenEncoding for ExponentialHistogramValue {
    type Schema = ExponentialHistogramSchema;

    fn encode_schema_from_value(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_varint(encode_zigzag_i64(self.scale as i64), out);
        encode_f64(self.zero_threshold, out);
        Ok(())
    }

    fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
        let scale = decode_i32(buf, cursor)?;
        let zero_threshold = decode_f64(buf, cursor)?;
        Ok(Self::Schema {
            scale,
            zero_threshold,
        })
    }

    fn encode_value_with_schema(&self, schema: &Self::Schema, out: &mut Vec<u8>) -> io::Result<()> {
        if self.scale != schema.scale
            || self.zero_threshold.to_bits() != schema.zero_threshold.to_bits()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exponential histogram schema mismatch",
            ));
        }
        encode_typed_metadata(self.metadata, out);
        encode_varint(self.count, out);
        encode_opt_f64(self.sum, out);
        encode_opt_f64(self.min, out);
        encode_opt_f64(self.max, out);
        encode_varint(self.zero_count, out);
        encode_varint(encode_zigzag_i64(self.positive.offset as i64), out);
        encode_varint(self.positive.counts.len() as u64, out);
        for count in &self.positive.counts {
            encode_varint(*count, out);
        }
        encode_varint(encode_zigzag_i64(self.negative.offset as i64), out);
        encode_varint(self.negative.counts.len() as u64, out);
        for count in &self.negative.counts {
            encode_varint(*count, out);
        }
        Ok(())
    }

    fn decode_value_with_schema(
        schema: &Self::Schema,
        buf: &[u8],
        cursor: &mut usize,
    ) -> io::Result<Self> {
        let metadata = decode_typed_metadata(buf, cursor)?;
        let count = decode_varint(buf, cursor)?;
        let sum = decode_opt_f64(buf, cursor)?;
        let min = decode_opt_f64(buf, cursor)?;
        let max = decode_opt_f64(buf, cursor)?;
        let zero_count = decode_varint(buf, cursor)?;
        let positive_offset = decode_i32(buf, cursor)?;
        let positive_len = decode_len(buf, cursor)?;
        let mut positive_counts = Vec::with_capacity(positive_len);
        for _ in 0..positive_len {
            positive_counts.push(decode_varint(buf, cursor)?);
        }
        let negative_offset = decode_i32(buf, cursor)?;
        let negative_len = decode_len(buf, cursor)?;
        let mut negative_counts = Vec::with_capacity(negative_len);
        for _ in 0..negative_len {
            negative_counts.push(decode_varint(buf, cursor)?);
        }
        Ok(Self {
            count,
            sum,
            min,
            max,
            scale: schema.scale,
            zero_threshold: schema.zero_threshold,
            zero_count,
            metadata,
            positive: ExponentialHistogramBuckets {
                offset: positive_offset,
                counts: positive_counts,
            },
            negative: ExponentialHistogramBuckets {
                offset: negative_offset,
                counts: negative_counts,
            },
        })
    }
}

impl SchemaVarLenEncoding for SummaryValue {
    type Schema = SummarySchema;

    fn encode_schema_from_value(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_varint(self.quantiles.len() as u64, out);
        for quantile in &self.quantiles {
            encode_f64(quantile.quantile, out);
        }
        Ok(())
    }

    fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
        let len = decode_len(buf, cursor)?;
        let mut quantiles = Vec::with_capacity(len);
        for _ in 0..len {
            quantiles.push(decode_f64(buf, cursor)?);
        }
        Ok(Self::Schema { quantiles })
    }

    fn encode_value_with_schema(&self, schema: &Self::Schema, out: &mut Vec<u8>) -> io::Result<()> {
        if self.quantiles.len() != schema.quantiles.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "summary quantile length mismatch",
            ));
        }
        for (idx, quantile) in self.quantiles.iter().enumerate() {
            if quantile.quantile.to_bits() != schema.quantiles[idx].to_bits() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "summary quantile schema mismatch",
                ));
            }
        }
        encode_typed_metadata(self.metadata, out);
        encode_varint(self.count, out);
        encode_f64(self.sum, out);
        for quantile in &self.quantiles {
            encode_f64(quantile.value, out);
        }
        Ok(())
    }

    fn decode_value_with_schema(
        schema: &Self::Schema,
        buf: &[u8],
        cursor: &mut usize,
    ) -> io::Result<Self> {
        let metadata = decode_typed_metadata(buf, cursor)?;
        let count = decode_varint(buf, cursor)?;
        let sum = decode_f64(buf, cursor)?;
        let mut quantiles = Vec::with_capacity(schema.quantiles.len());
        for quantile in &schema.quantiles {
            let value = decode_f64(buf, cursor)?;
            quantiles.push(SummaryQuantileValue {
                quantile: *quantile,
                value,
            });
        }
        Ok(Self {
            count,
            sum,
            metadata,
            quantiles,
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SeriesSamples {
    Float {
        encoding: FloatEncoding,
        samples: Vec<(u64, f64)>,
    },
    Int64 {
        encoding: IntEncoding,
        samples: Vec<(u64, i64)>,
    },
    Histogram {
        samples: Vec<(u64, HistogramValue)>,
    },
    ExponentialHistogram {
        samples: Vec<(u64, ExponentialHistogramValue)>,
    },
    Summary {
        samples: Vec<(u64, SummaryValue)>,
    },
}

pub trait SeriesLabelResolver {
    fn len(&self) -> usize;
    fn visit_labelset(&self, series: SeriesRef, visitor: &mut dyn FnMut(&str, &str));
}

impl<T> SeriesLabelResolver for T
where
    T: LabelSetStore,
{
    fn len(&self) -> usize {
        LabelSetStore::len(self)
    }

    fn visit_labelset(&self, series: SeriesRef, visitor: &mut dyn FnMut(&str, &str)) {
        LabelSetStore::visit_labelset(self, series, |key, value| visitor(key, value));
    }
}

impl SeriesSamples {
    fn is_empty(&self) -> bool {
        match self {
            Self::Float { samples, .. } => samples.is_empty(),
            Self::Int64 { samples, .. } => samples.is_empty(),
            Self::Histogram { samples } => samples.is_empty(),
            Self::ExponentialHistogram { samples } => samples.is_empty(),
            Self::Summary { samples } => samples.is_empty(),
        }
    }
}

#[derive(Debug)]
pub struct HeadWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    series: HashMap<SeriesRef, EncodedSeries>,
    pub datapoints: u64,
    arena: BlockArena,
}

impl HeadWindow {
    fn new(start_ms: u64, end_ms: u64) -> Self {
        Self {
            start_ms,
            end_ms,
            series: HashMap::new(),
            datapoints: 0,
            arena: BlockArena::new(DEFAULT_HEAD_ARENA_PAGE_BYTES),
        }
    }

    pub fn into_series_samples(self) -> io::Result<Vec<(SeriesRef, SeriesSamples)>> {
        let mut window = self;
        window.seal_all_series();
        let HeadWindow { series, arena, .. } = window;
        let mut decoded = Vec::with_capacity(series.len());
        for (series, encoded) in series {
            let series_estimated_bytes = encoded.estimated_bytes();
            if series_estimated_bytes > 1000 {
                debug!(
                    "Head series sealing series={} value_kind={:?} codec={} samples={} estimated_bytes={}",
                    series.get(),
                    encoded.kind(),
                    encoded.codec_name(),
                    encoded.sample_count(),
                    series_estimated_bytes
                );
            }
            let samples = encoded.into_samples(&arena)?;
            decoded.push((series, samples));
        }
        Ok(decoded)
    }

    pub fn estimated_bytes(&self) -> usize {
        self.series.values().fold(0usize, |acc, encoded| {
            acc.saturating_add(encoded.estimated_bytes())
        })
    }

    pub fn estimated_bytes_by_kind(&self) -> BytesByKind {
        self.bytes_by_kind(|encoded| encoded.estimated_bytes(), |_| None)
    }

    pub fn estimated_bytes_by_kind_with_number_kind<F>(&self, number_kind: F) -> BytesByKind
    where
        F: FnMut(SeriesRef) -> Option<NumberMetricKind>,
    {
        self.bytes_by_kind(|encoded| encoded.estimated_bytes(), number_kind)
    }

    pub fn payload_bytes(&self) -> usize {
        self.series.values().fold(0usize, |acc, encoded| {
            acc.saturating_add(encoded.payload_bytes())
        })
    }

    pub fn payload_bytes_by_kind(&self) -> BytesByKind {
        self.bytes_by_kind(|encoded| encoded.payload_bytes(), |_| None)
    }

    pub fn payload_bytes_by_kind_with_number_kind<F>(&self, number_kind: F) -> BytesByKind
    where
        F: FnMut(SeriesRef) -> Option<NumberMetricKind>,
    {
        self.bytes_by_kind(|encoded| encoded.payload_bytes(), number_kind)
    }

    pub fn series_len(&self) -> usize {
        self.series.len()
    }

    pub fn series_sample_counts(&self) -> impl Iterator<Item = u64> + '_ {
        self.series.values().map(|encoded| encoded.sample_count())
    }

    pub fn series_block_counts(&self) -> impl Iterator<Item = usize> + '_ {
        self.series.values().map(|encoded| encoded.block_count())
    }

    pub fn for_each_block_sample<F>(&self, mut f: F)
    where
        F: FnMut(u64),
    {
        for encoded in self.series.values() {
            encoded.for_each_block_sample(&mut f);
        }
    }

    pub fn arena_capacity_bytes(&self) -> usize {
        self.arena.total_capacity_bytes()
    }

    pub fn arena_used_bytes(&self) -> usize {
        self.arena.total_used_bytes()
    }

    pub fn arena_slack_bytes(&self) -> usize {
        self.arena.slack_bytes()
    }

    pub fn arena_page_count(&self) -> usize {
        self.arena.page_count()
    }

    fn seal_all_series(&mut self) {
        for encoded in self.series.values_mut() {
            encoded.seal(&mut self.arena);
        }
    }

    fn bytes_by_kind<F, G>(&self, mut bytes_fn: F, mut number_kind: G) -> BytesByKind
    where
        F: FnMut(&EncodedSeries) -> usize,
        G: FnMut(SeriesRef) -> Option<NumberMetricKind>,
    {
        let mut bytes = BytesByKind::default();
        for (series, encoded) in &self.series {
            let value = bytes_fn(encoded) as u64;
            match encoded.kind() {
                SampleKind::Float => {
                    bytes.float = bytes.float.saturating_add(value);
                    match number_kind(*series) {
                        Some(NumberMetricKind::Gauge) => {
                            bytes.float_gauge = bytes.float_gauge.saturating_add(value);
                        }
                        Some(NumberMetricKind::Sum) => {
                            bytes.float_sum = bytes.float_sum.saturating_add(value);
                        }
                        None => {}
                    }
                }
                SampleKind::Int64 => {
                    bytes.int = bytes.int.saturating_add(value);
                    match number_kind(*series) {
                        Some(NumberMetricKind::Gauge) => {
                            bytes.int_gauge = bytes.int_gauge.saturating_add(value);
                        }
                        Some(NumberMetricKind::Sum) => {
                            bytes.int_sum = bytes.int_sum.saturating_add(value);
                        }
                        None => {}
                    }
                }
                SampleKind::Histogram => {
                    bytes.histogram = bytes.histogram.saturating_add(value);
                }
                SampleKind::ExponentialHistogram => {
                    bytes.exponential_histogram = bytes.exponential_histogram.saturating_add(value);
                }
                SampleKind::Summary => {
                    bytes.summary = bytes.summary.saturating_add(value);
                }
            }
        }
        bytes
    }

    pub fn series_samples_in_range(
        &self,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<(SeriesRef, SeriesSamples)>> {
        if end_ms <= start_ms {
            return Ok(Vec::new());
        }

        let mut decoded = Vec::new();
        for (series, encoded) in &self.series {
            let samples = encoded.samples_in_range(&self.arena, start_ms, end_ms)?;
            if !samples.is_empty() {
                decoded.push((*series, samples));
            }
        }
        Ok(decoded)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct HeadSelectorIndexKey {
    start_ms: u64,
    end_ms: u64,
    datapoints: u64,
    series_len: usize,
    label_resolver_len: usize,
}

impl HeadSelectorIndexKey {
    fn new(window: &HeadWindow, label_resolver_len: usize) -> Self {
        Self {
            start_ms: window.start_ms,
            end_ms: window.end_ms,
            datapoints: window.datapoints,
            series_len: window.series.len(),
            label_resolver_len,
        }
    }
}

#[derive(Debug, Clone)]
struct CachedHeadSelectorIndex {
    key: HeadSelectorIndexKey,
    index: HeadSelectorIndex,
}

#[derive(Debug, Clone)]
struct HeadIndexedSeries {
    series_id: u64,
    labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
struct HeadSelectorIndex {
    all_series: Vec<SeriesRef>,
    series: BTreeMap<SeriesRef, HeadIndexedSeries>,
    postings: BTreeMap<(String, String), Vec<SeriesRef>>,
    label_values: BTreeMap<String, Vec<String>>,
}

impl HeadSelectorIndex {
    fn build<R>(window: &HeadWindow, labels: &R) -> io::Result<Self>
    where
        R: SeriesLabelResolver,
    {
        let mut all_series: Vec<_> = window.series.keys().copied().collect();
        all_series.sort_unstable();

        let mut series = BTreeMap::new();
        let mut postings: BTreeMap<(String, String), Vec<SeriesRef>> = BTreeMap::new();
        let mut label_values: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        let mut indexed_series = Vec::with_capacity(all_series.len());

        for series_ref in all_series {
            let Some((series_id_value, canonical_labels)) =
                canonical_head_labelset(labels, series_ref)
            else {
                continue;
            };

            for (name, value) in &canonical_labels {
                postings
                    .entry((name.clone(), value.clone()))
                    .or_default()
                    .push(series_ref);
                label_values
                    .entry(name.clone())
                    .or_default()
                    .insert(value.clone());
            }

            indexed_series.push(series_ref);
            series.insert(
                series_ref,
                HeadIndexedSeries {
                    series_id: series_id_value,
                    labels: canonical_labels,
                },
            );
        }

        Ok(Self {
            all_series: indexed_series,
            series,
            postings,
            label_values: label_values
                .into_iter()
                .map(|(name, values)| (name, values.into_iter().collect()))
                .collect(),
        })
    }

    fn series(&self, series: &SeriesRef) -> Option<&HeadIndexedSeries> {
        self.series.get(series)
    }

    fn matching_series(
        &self,
        matchers: &[NormalizedMatcher],
        budget: &mut QueryBudget,
        match_promql_projection_names: bool,
    ) -> io::Result<Vec<SeriesRef>> {
        let mut candidates: Option<Vec<SeriesRef>> = None;
        for matcher in matchers {
            let positive = match matcher {
                NormalizedMatcher::Eq { name, value } => Some(self.exact_postings(name, value)),
                NormalizedMatcher::Regex { name, pattern } => Some(self.regex_postings(
                    name,
                    pattern,
                    budget,
                    match_promql_projection_names && name == METRIC_NAME_LABEL,
                )?),
                NormalizedMatcher::NotEq { .. } | NormalizedMatcher::NotRegex { .. } => None,
            };

            if let Some(positive) = positive {
                if positive.is_empty() {
                    return Ok(Vec::new());
                }
                candidates = Some(match candidates {
                    Some(existing) => intersect_series_refs(&existing, &positive),
                    None => positive,
                });
            }
        }

        let mut candidate_refs = candidates.unwrap_or_else(|| self.all_series.clone());
        for matcher in matchers {
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let posting = self.exact_postings(name, value);
                    if !posting.is_empty() {
                        candidate_refs = subtract_series_refs(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = self.regex_postings(name, pattern, budget, false)?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_series_refs(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        Ok(candidate_refs)
    }

    fn exact_postings(&self, name: &str, value: &str) -> Vec<SeriesRef> {
        self.postings
            .get(&(name.to_string(), value.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    fn regex_postings(
        &self,
        name: &str,
        pattern: &str,
        budget: &mut QueryBudget,
        match_promql_projection_names: bool,
    ) -> io::Result<Vec<SeriesRef>> {
        let regex = compile_promql_regex(pattern)
            .map_err(|err| io::Error::new(io::ErrorKind::InvalidInput, err))?;
        let Some(values) = self.label_values.get(name) else {
            return Ok(Vec::new());
        };

        let mut out = Vec::new();
        for value in values {
            budget.observe_regex_value()?;
            let matches = if match_promql_projection_names {
                promql_projection_metric_name_matches(value, &regex)
            } else {
                regex.is_match(value)
            };
            if !matches {
                continue;
            }
            if let Some(posting) = self.postings.get(&(name.to_string(), value.clone())) {
                out = union_series_refs(&out, posting);
            }
        }

        Ok(out)
    }
}

pub struct HeadBuffer {
    config: HeadConfig,
    window: Option<HeadWindow>,
    ooo_windows: BTreeMap<(u64, u64), HeadWindow>,
    last_timestamps: HashMap<SeriesRef, u64>,
    selector_index: Mutex<Option<CachedHeadSelectorIndex>>,
}

impl HeadBuffer {
    pub fn new(config: HeadConfig) -> io::Result<Self> {
        let _ = Self::window_duration_ms(&config)?;
        let _ = Self::out_of_order_time_window_ms(&config)?;
        Self::validate_block_size(&config)?;
        Ok(Self {
            config,
            window: None,
            ooo_windows: BTreeMap::new(),
            last_timestamps: HashMap::new(),
            selector_index: Mutex::new(None),
        })
    }

    pub fn record_sample(
        &mut self,
        series: SeriesRef,
        timestamp_ms: u64,
        value: SampleValue,
    ) -> io::Result<Option<HeadWindow>> {
        let mut flushed = self.record_samples(series, &[(timestamp_ms, value)])?;
        Ok(flushed.pop())
    }

    pub fn record_samples(
        &mut self,
        series: SeriesRef,
        samples: &[(u64, SampleValue)],
    ) -> io::Result<Vec<HeadWindow>> {
        let duration_ms = Self::window_duration_ms(&self.config)?;
        let mut flushed = Vec::new();

        for (ts, value) in samples {
            self.validate_sample_timestamp(series, *ts)?;
            let (start_ms, end_ms) = window_for(*ts, duration_ms);
            let route_to_ooo = self.should_route_to_ooo_window(series, *ts);

            let accepted = if route_to_ooo {
                let window = self
                    .ooo_windows
                    .entry((start_ms, end_ms))
                    .or_insert_with(|| HeadWindow::new(start_ms, end_ms));
                Self::push_sample_to_window(&self.config, window, series, *ts, value)?
            } else {
                let rotate = match &self.window {
                    None => true,
                    Some(window) => *ts >= window.end_ms,
                };

                if rotate {
                    if let Some(mut window) = self.window.take() {
                        window.seal_all_series();
                        flushed.push(window);
                    }
                    self.window = Some(HeadWindow::new(start_ms, end_ms));
                }

                let Some(window) = self.window.as_mut() else {
                    continue;
                };
                Self::push_sample_to_window(&self.config, window, series, *ts, value)?
            };

            if accepted {
                self.record_accepted_timestamp(series, *ts);
                self.clear_selector_index_cache();
            }
        }

        Ok(flushed)
    }

    pub fn drain(&mut self) -> Option<HeadWindow> {
        self.clear_selector_index_cache();
        if let Some(mut window) = self.window.take() {
            window.seal_all_series();
            Some(window)
        } else {
            None
        }
    }

    pub fn drain_windows(&mut self) -> Vec<HeadWindow> {
        self.clear_selector_index_cache();
        let mut windows = Vec::new();
        for (_range, mut window) in std::mem::take(&mut self.ooo_windows) {
            window.seal_all_series();
            windows.push(window);
        }
        if let Some(mut window) = self.window.take() {
            window.seal_all_series();
            windows.push(window);
        }
        windows.sort_by_key(|window| (window.start_ms, window.end_ms));
        windows
    }

    pub fn window_range(&self) -> Option<(u64, u64)> {
        self.window.as_ref().map(|w| (w.start_ms, w.end_ms))
    }

    pub fn query_selector<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::unlimited();
        self.query_selector_with_budget(labels, selector, start_ms, end_ms, &mut budget)
    }

    pub(crate) fn query_selector_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let matchers = selector.normalized_matchers();
        let mut results = Vec::new();
        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            results.extend(self.query_window_selector_with_budget(
                labels,
                window,
                &matchers,
                selector.projection(),
                start_ms,
                end_ms,
                budget,
            )?);
        }

        Ok(merge_head_query_results(results))
    }

    fn query_window_selector_with_budget<R>(
        &self,
        labels: &R,
        window: &HeadWindow,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        let index = self.selector_index(labels, window)?;
        let candidate_series = index.matching_series(
            &matchers,
            budget,
            matches!(projection, SegmentProjection::AllPromql { .. }),
        )?;
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => Some(compile_label_matchers(matchers)?),
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::SummaryQuantile { .. } => None,
        };
        let mut results = Vec::new();
        let range_end_ms = end_ms.saturating_add(1);

        for series in candidate_series {
            let Some(encoded) = window.series.get(&series) else {
                continue;
            };
            let Some(indexed) = index.series(&series) else {
                continue;
            };
            budget.observe_matched_series(indexed.series_id)?;

            let samples = encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?;
            match (projection, samples) {
                (
                    SegmentProjection::None | SegmentProjection::AllPromql { .. },
                    SeriesSamples::Float { samples, .. },
                ) => {
                    budget.observe_samples_decoded(samples.len() as u64)?;
                    if samples.is_empty() {
                        continue;
                    }
                    if projected_label_filter
                        .as_ref()
                        .is_some_and(|filter| !labels_match_compiled(&indexed.labels, filter))
                    {
                        continue;
                    }

                    results.push(SegmentQueryResult::with_samples(
                        indexed.series_id,
                        indexed.labels.clone(),
                        samples,
                    ));
                }
                (
                    SegmentProjection::None | SegmentProjection::AllPromql { .. },
                    SeriesSamples::Int64 { samples, .. },
                ) => {
                    budget.observe_samples_decoded(samples.len() as u64)?;
                    if samples.is_empty() {
                        continue;
                    }
                    if projected_label_filter
                        .as_ref()
                        .is_some_and(|filter| !labels_match_compiled(&indexed.labels, filter))
                    {
                        continue;
                    }

                    results.push(SegmentQueryResult::with_samples(
                        indexed.series_id,
                        indexed.labels.clone(),
                        samples
                            .into_iter()
                            .map(|(timestamp_ms, value)| (timestamp_ms, value as f64))
                            .collect(),
                    ));
                }
                (SegmentProjection::None, _) => {}
                (projection, samples) => {
                    let decoded_count = series_samples_len(&samples);
                    let mut projected = project_head_series_samples(
                        projection,
                        &indexed.labels,
                        samples,
                        start_ms,
                        end_ms,
                    );
                    budget.observe_samples_decoded(decoded_count as u64)?;
                    if let Some(filter) = &projected_label_filter {
                        projected.retain(|result| labels_match_compiled(&result.labels, filter));
                    }
                    results.append(&mut projected);
                }
            }
        }

        results.sort_by_key(|result| result.series_id);
        Ok(results)
    }

    pub fn metric_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values<R>(
        &self,
        labels: &R,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        let label_name = if label_name == METRIC_NAME_LABEL {
            METRIC_NAME_LABEL.to_string()
        } else {
            crate::promql::normalize_label_name(label_name)
        };
        Ok(metadata.label_values(&label_name))
    }

    pub(crate) fn collect_metadata<R>(
        &self,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(());
        }

        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            Self::collect_window_metadata(labels, window, start_ms, end_ms, metadata)?;
        }

        Ok(())
    }

    fn collect_window_metadata<R>(
        labels: &R,
        window: &HeadWindow,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        let range_end_ms = end_ms.saturating_add(1);
        for (series, encoded) in &window.series {
            let samples = encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?;
            if samples.is_empty() {
                continue;
            }
            let Some((_, canonical_labels)) = canonical_head_labelset(labels, *series) else {
                continue;
            };
            metadata.add_labelset(&canonical_labels);
        }

        Ok(())
    }

    fn query_windows(&self) -> Vec<&HeadWindow> {
        let mut windows: Vec<(u8, &HeadWindow)> = Vec::new();
        if let Some(window) = &self.window {
            windows.push((0, window));
        }
        for window in self.ooo_windows.values() {
            windows.push((1, window));
        }
        windows.sort_by_key(|(lane_precedence, window)| {
            (window.start_ms, window.end_ms, *lane_precedence)
        });
        windows.into_iter().map(|(_, window)| window).collect()
    }

    fn window_overlaps_range(window: &HeadWindow, start_ms: u64, end_ms: u64) -> bool {
        window.end_ms > start_ms && window.start_ms <= end_ms
    }

    fn selector_index<R>(&self, labels: &R, window: &HeadWindow) -> io::Result<HeadSelectorIndex>
    where
        R: SeriesLabelResolver,
    {
        let key = HeadSelectorIndexKey::new(window, labels.len());
        {
            let cache = self
                .selector_index
                .lock()
                .map_err(|_| io::Error::other("head selector index cache lock poisoned"))?;
            if let Some(cached) = cache.as_ref()
                && cached.key == key
            {
                return Ok(cached.index.clone());
            }
        }

        let index = HeadSelectorIndex::build(window, labels)?;
        let mut cache = self
            .selector_index
            .lock()
            .map_err(|_| io::Error::other("head selector index cache lock poisoned"))?;
        *cache = Some(CachedHeadSelectorIndex {
            key,
            index: index.clone(),
        });
        Ok(index)
    }

    fn clear_selector_index_cache(&mut self) {
        if let Ok(cache) = self.selector_index.get_mut() {
            *cache = None;
        }
    }

    fn should_route_to_ooo_window(&self, series: SeriesRef, timestamp_ms: u64) -> bool {
        if self
            .last_timestamps
            .get(&series)
            .is_some_and(|last_timestamp_ms| timestamp_ms < *last_timestamp_ms)
        {
            return true;
        }
        self.window
            .as_ref()
            .is_some_and(|window| timestamp_ms < window.start_ms)
    }

    fn push_sample_to_window(
        config: &HeadConfig,
        window: &mut HeadWindow,
        series: SeriesRef,
        timestamp_ms: u64,
        value: &SampleValue,
    ) -> io::Result<bool> {
        let base_ms = window.start_ms;
        let block_size = config.block_size;
        let encoding = match value.kind() {
            SampleKind::Float => SeriesEncoding::Float(config.float_encoding),
            SampleKind::Int64 => SeriesEncoding::Int(config.int_encoding),
            SampleKind::Histogram => SeriesEncoding::Histogram(config.varlen_encoding),
            SampleKind::ExponentialHistogram => {
                SeriesEncoding::ExponentialHistogram(config.varlen_encoding)
            }
            SampleKind::Summary => SeriesEncoding::Summary(config.varlen_encoding),
        };
        match window.series.entry(series) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                let mut encoded = EncodedSeries::new(encoding);
                encoded.push_sample(
                    series,
                    base_ms,
                    timestamp_ms,
                    value.clone(),
                    block_size,
                    &mut window.arena,
                )?;
                entry.insert(encoded);
            }
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                if entry.get().kind() != value.kind() {
                    warn!(
                        "Head series type mismatch series={} expected={:?} got={:?}; dropping sample",
                        series.get(),
                        entry.get().kind(),
                        value.kind()
                    );
                    return Ok(false);
                }
                entry.get_mut().push_sample(
                    series,
                    base_ms,
                    timestamp_ms,
                    value.clone(),
                    block_size,
                    &mut window.arena,
                )?;
            }
        }
        window.datapoints = window.datapoints.saturating_add(1);
        Ok(true)
    }

    fn window_duration_ms(config: &HeadConfig) -> io::Result<u64> {
        let ms = config.window_duration.as_millis();
        if ms == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "window_duration must be > 0",
            ));
        }
        if ms > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "window_duration is too large",
            ));
        }
        Ok(ms as u64)
    }

    fn out_of_order_time_window_ms(config: &HeadConfig) -> io::Result<u64> {
        let ms = config.out_of_order_time_window.as_millis();
        if ms > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "out_of_order_time_window is too large",
            ));
        }
        Ok(ms as u64)
    }

    fn validate_sample_timestamp(&self, series: SeriesRef, timestamp_ms: u64) -> io::Result<()> {
        let Some(last_timestamp_ms) = self.last_timestamps.get(&series).copied() else {
            return Ok(());
        };
        if timestamp_ms >= last_timestamp_ms {
            return Ok(());
        }

        let window_ms = Self::out_of_order_time_window_ms(&self.config)?;
        let lower_bound_ms = last_timestamp_ms.saturating_sub(window_ms);
        if timestamp_ms < lower_bound_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sample is outside out_of_order_time_window",
            ));
        }
        Ok(())
    }

    fn record_accepted_timestamp(&mut self, series: SeriesRef, timestamp_ms: u64) {
        self.last_timestamps
            .entry(series)
            .and_modify(|last| *last = (*last).max(timestamp_ms))
            .or_insert(timestamp_ms);
    }

    fn validate_block_size(config: &HeadConfig) -> io::Result<()> {
        if config.block_size == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_size must be > 0",
            ));
        }
        if config.block_size > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "block_size is too large",
            ));
        }
        Ok(())
    }
}

fn canonical_head_labelset<R>(labels: &R, series: SeriesRef) -> Option<(u64, Vec<(String, String)>)>
where
    R: SeriesLabelResolver,
{
    if series.get() as usize >= labels.len() {
        return None;
    }

    let mut metric_name = String::new();
    let mut attributes = Vec::new();
    labels.visit_labelset(series, &mut |key, value| {
        if key == METRIC_NAME_LABEL {
            metric_name = value.to_string();
        } else {
            attributes.push((key.to_string(), value.to_string()));
        }
    });

    let attribute_refs: Vec<(&str, &str)> = attributes
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let canonical = canonicalize_labelset(&metric_name, &attribute_refs);
    let id = series_id(&canonical);
    let labels = canonical
        .labels()
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();

    Some((id, labels))
}

fn intersect_series_refs(left: &[SeriesRef], right: &[SeriesRef]) -> Vec<SeriesRef> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() && ri < right.len() {
        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => li += 1,
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

fn union_series_refs(left: &[SeriesRef], right: &[SeriesRef]) -> Vec<SeriesRef> {
    let mut out = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() || ri < right.len() {
        if li >= left.len() {
            out.extend_from_slice(&right[ri..]);
            break;
        }
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(right[ri]);
                ri += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

fn subtract_series_refs(left: &[SeriesRef], right: &[SeriesRef]) -> Vec<SeriesRef> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() {
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

fn merge_head_query_results(results: Vec<SegmentQueryResult>) -> Vec<SegmentQueryResult> {
    let mut merged: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
    for result in results {
        let entry = merged
            .entry(result.series_id)
            .or_insert_with(|| SegmentQueryResult::new(result.series_id, result.labels.clone()));
        entry.extend_from(result);
    }

    let mut results: Vec<_> = merged.into_values().collect();
    for result in &mut results {
        result.dedupe_samples_keep_last();
    }
    results
}

fn series_samples_len(samples: &SeriesSamples) -> usize {
    match samples {
        SeriesSamples::Float { samples, .. } => samples.len(),
        SeriesSamples::Int64 { samples, .. } => samples.len(),
        SeriesSamples::Histogram { samples } => samples.len(),
        SeriesSamples::ExponentialHistogram { samples } => samples.len(),
        SeriesSamples::Summary { samples } => samples.len(),
    }
}

fn project_head_series_samples(
    projection: &SegmentProjection,
    base_labels: &[(String, String)],
    samples: SeriesSamples,
    start_ms: u64,
    end_ms: u64,
) -> Vec<SegmentQueryResult> {
    let metric_name = base_labels
        .iter()
        .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then_some(value.as_str()))
        .unwrap_or_default();
    let mut projected = BTreeMap::new();

    match (projection, samples) {
        (SegmentProjection::AllPromql { .. }, SeriesSamples::Histogram { samples }) => {
            project_head_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            project_head_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            for result in project_head_series_samples(
                &SegmentProjection::HistogramBucket {
                    le: None,
                    exponential_histogram_boundaries: Vec::new(),
                },
                base_labels,
                SeriesSamples::Histogram { samples },
                start_ms,
                end_ms,
            ) {
                projected.insert(result.series_id, result);
            }
        }
        (
            SegmentProjection::AllPromql {
                exponential_histogram_boundaries,
            },
            SeriesSamples::ExponentialHistogram { samples },
        ) => {
            project_head_exponential_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            project_head_exponential_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            for result in project_head_series_samples(
                &SegmentProjection::HistogramBucket {
                    le: None,
                    exponential_histogram_boundaries: exponential_histogram_boundaries.clone(),
                },
                base_labels,
                SeriesSamples::ExponentialHistogram { samples },
                start_ms,
                end_ms,
            ) {
                projected.insert(result.series_id, result);
            }
        }
        (SegmentProjection::AllPromql { .. }, SeriesSamples::Summary { samples }) => {
            project_head_summary_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            project_head_summary_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples.clone(),
                start_ms,
                end_ms,
            );
            for result in project_head_series_samples(
                &SegmentProjection::SummaryQuantile { quantile: None },
                base_labels,
                SeriesSamples::Summary { samples },
                start_ms,
                end_ms,
            ) {
                projected.insert(result.series_id, result);
            }
        }
        (SegmentProjection::Count, SeriesSamples::Histogram { samples }) => {
            project_head_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Count, SeriesSamples::ExponentialHistogram { samples }) => {
            project_head_exponential_histogram_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Count, SeriesSamples::Summary { samples }) => {
            project_head_summary_count_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Sum, SeriesSamples::Histogram { samples }) => {
            project_head_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Sum, SeriesSamples::ExponentialHistogram { samples }) => {
            project_head_exponential_histogram_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::Sum, SeriesSamples::Summary { samples }) => {
            project_head_summary_sum_samples(
                &mut projected,
                base_labels,
                metric_name,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::HistogramBucket { le, .. }, SeriesSamples::Histogram { samples }) => {
            let mut delta_accumulators = BTreeMap::new();
            for (ts, value) in samples {
                if ts < start_ms || ts > end_ms {
                    continue;
                }
                let mut cumulative = 0u64;
                for (idx, bound) in value.explicit_bounds.iter().enumerate() {
                    cumulative = cumulative
                        .saturating_add(value.bucket_counts.get(idx).copied().unwrap_or(0));
                    let le_value = format_promql_float_label(*bound);
                    if le.as_deref().is_none_or(|filter| filter == le_value) {
                        let projected_value = project_head_histogram_bucket_value(
                            value.metadata,
                            cumulative,
                            &le_value,
                            &mut delta_accumulators,
                        );
                        let labels = projected_head_labels(
                            base_labels,
                            metric_name,
                            "_bucket",
                            Some(("le", le_value)),
                        );
                        push_head_projected_sample_with_counter_reset_hint(
                            &mut projected,
                            labels,
                            ts,
                            projected_value,
                            value.metadata.reset_hint,
                        );
                    }
                }
                if le.as_deref().is_none_or(|filter| filter == "+Inf") {
                    let projected_value = project_head_histogram_bucket_value(
                        value.metadata,
                        value.count,
                        "+Inf",
                        &mut delta_accumulators,
                    );
                    let labels = projected_head_labels(
                        base_labels,
                        metric_name,
                        "_bucket",
                        Some(("le", "+Inf".to_string())),
                    );
                    push_head_projected_sample_with_counter_reset_hint(
                        &mut projected,
                        labels,
                        ts,
                        projected_value,
                        value.metadata.reset_hint,
                    );
                }
            }
        }
        (
            SegmentProjection::HistogramBucket {
                le,
                exponential_histogram_boundaries,
            },
            SeriesSamples::ExponentialHistogram { samples },
        ) => {
            project_head_exponential_histogram_bucket_samples(
                &mut projected,
                base_labels,
                metric_name,
                le.as_deref(),
                exponential_histogram_boundaries,
                samples,
                start_ms,
                end_ms,
            );
        }
        (SegmentProjection::SummaryQuantile { quantile }, SeriesSamples::Summary { samples }) => {
            for (ts, value) in samples {
                if ts < start_ms || ts > end_ms {
                    continue;
                }
                for quantile_value in value.quantiles {
                    let label = format_promql_float_label(quantile_value.quantile);
                    if quantile.as_deref().is_some_and(|filter| filter != label) {
                        continue;
                    }
                    let labels = projected_head_labels(
                        base_labels,
                        metric_name,
                        "",
                        Some(("quantile", label)),
                    );
                    let projected_value = if value.metadata.is_stale() {
                        prometheus_stale_nan()
                    } else {
                        quantile_value.value
                    };
                    push_head_projected_sample(&mut projected, labels, ts, projected_value);
                }
            }
        }
        _ => {}
    }

    projected.into_values().collect()
}

fn project_head_histogram_count_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, HistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_u64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_count",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
}

fn project_head_exponential_histogram_count_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, ExponentialHistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_u64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_count",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
}

fn project_head_summary_count_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, SummaryValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_u64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_count",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.count)),
        start_ms,
        end_ms,
    );
}

fn project_head_histogram_sum_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, HistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_optional_f64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_sum",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.sum)),
        start_ms,
        end_ms,
    );
}

fn project_head_exponential_histogram_sum_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, ExponentialHistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_optional_f64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_sum",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, value.sum)),
        start_ms,
        end_ms,
    );
}

fn project_head_summary_sum_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    values: Vec<(u64, SummaryValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    project_head_typed_optional_f64_counter_samples(
        out,
        base_labels,
        metric_name,
        "_sum",
        values
            .into_iter()
            .map(|(ts, value)| (ts, value.metadata, Some(value.sum))),
        start_ms,
        end_ms,
    );
}

fn project_head_typed_u64_counter_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    metric_suffix: &str,
    values: impl IntoIterator<Item = (u64, TypedSampleMetadata, u64)>,
    start_ms: u64,
    end_ms: u64,
) {
    let labels = projected_head_labels(base_labels, metric_name, metric_suffix, None);
    let mut delta_accumulator = 0u64;
    for (ts, metadata, raw) in values {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            prometheus_stale_nan()
        } else if metadata.temporality == OtlpAggregationTemporality::Delta {
            delta_accumulator = delta_accumulator.saturating_add(raw);
            delta_accumulator as f64
        } else {
            raw as f64
        };
        push_head_projected_sample_with_counter_reset_hint(
            out,
            labels.clone(),
            ts,
            value,
            metadata.reset_hint,
        );
    }
}

fn project_head_typed_optional_f64_counter_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    metric_suffix: &str,
    values: impl IntoIterator<Item = (u64, TypedSampleMetadata, Option<f64>)>,
    start_ms: u64,
    end_ms: u64,
) {
    let labels = projected_head_labels(base_labels, metric_name, metric_suffix, None);
    let mut delta_accumulator = 0.0f64;
    for (ts, metadata, raw) in values {
        if ts < start_ms || ts > end_ms {
            continue;
        }
        let value = if metadata.is_stale() {
            prometheus_stale_nan()
        } else if let Some(raw) = raw {
            if metadata.temporality == OtlpAggregationTemporality::Delta {
                delta_accumulator += raw;
                delta_accumulator
            } else {
                raw
            }
        } else {
            continue;
        };
        push_head_projected_sample_with_counter_reset_hint(
            out,
            labels.clone(),
            ts,
            value,
            metadata.reset_hint,
        );
    }
}

fn project_head_histogram_bucket_value(
    metadata: TypedSampleMetadata,
    raw: u64,
    le: &str,
    delta_accumulators: &mut BTreeMap<String, u64>,
) -> f64 {
    if metadata.is_stale() {
        return prometheus_stale_nan();
    }
    if metadata.temporality == OtlpAggregationTemporality::Delta {
        let accumulator = delta_accumulators.entry(le.to_string()).or_insert(0);
        *accumulator = accumulator.saturating_add(raw);
        *accumulator as f64
    } else {
        raw as f64
    }
}

fn project_head_exponential_histogram_bucket_samples(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    base_labels: &[(String, String)],
    metric_name: &str,
    le_filter: Option<&str>,
    boundaries: &[f64],
    values: Vec<(u64, ExponentialHistogramValue)>,
    start_ms: u64,
    end_ms: u64,
) {
    let mut delta_accumulators: BTreeMap<String, u64> = BTreeMap::new();
    for (ts, value) in values {
        if ts < start_ms || ts > end_ms {
            continue;
        }

        for boundary in boundaries {
            let le = format_promql_float_label(*boundary);
            if le_filter.is_none_or(|filter| filter == le) {
                let raw = exponential_histogram_projected_bucket_count(&value, *boundary);
                let projected = project_head_histogram_bucket_value(
                    value.metadata,
                    raw,
                    &le,
                    &mut delta_accumulators,
                );
                let labels =
                    projected_head_labels(base_labels, metric_name, "_bucket", Some(("le", le)));
                push_head_projected_sample_with_counter_reset_hint(
                    out,
                    labels,
                    ts,
                    projected,
                    value.metadata.reset_hint,
                );
            }
        }

        if le_filter.is_none_or(|filter| filter == "+Inf") {
            let projected = project_head_histogram_bucket_value(
                value.metadata,
                value.count,
                "+Inf",
                &mut delta_accumulators,
            );
            let labels = projected_head_labels(
                base_labels,
                metric_name,
                "_bucket",
                Some(("le", "+Inf".to_string())),
            );
            push_head_projected_sample_with_counter_reset_hint(
                out,
                labels,
                ts,
                projected,
                value.metadata.reset_hint,
            );
        }
    }
}

pub(crate) fn exponential_histogram_projected_bucket_count(
    value: &ExponentialHistogramValue,
    le: f64,
) -> u64 {
    if le.is_infinite() && le.is_sign_positive() {
        return value.count;
    }

    let base = exponential_histogram_base(value.scale);
    let negative = exponential_histogram_negative_bucket_count_le(&value.negative, base, le);
    let zero = if le >= value.zero_threshold {
        value.zero_count
    } else {
        0
    };
    let positive = exponential_histogram_positive_bucket_count_le(&value.positive, base, le);
    negative
        .saturating_add(zero)
        .saturating_add(positive)
        .min(value.count)
}

pub fn downscale_exponential_histogram(
    value: &ExponentialHistogramValue,
    target_scale: i32,
) -> Result<ExponentialHistogramValue, ExponentialHistogramMergeError> {
    if target_scale > value.scale {
        return Err(
            ExponentialHistogramMergeError::TargetScaleHigherThanSource {
                source_scale: value.scale,
                target_scale,
            },
        );
    }

    Ok(ExponentialHistogramValue {
        scale: target_scale,
        positive: exponential_histogram_bucket_map_to_buckets(
            downscale_exponential_histogram_buckets_to_map(
                &value.positive,
                value.scale,
                target_scale,
            )?,
        )?,
        negative: exponential_histogram_bucket_map_to_buckets(
            downscale_exponential_histogram_buckets_to_map(
                &value.negative,
                value.scale,
                target_scale,
            )?,
        )?,
        ..value.clone()
    })
}

pub fn merge_exponential_histograms(
    values: &[ExponentialHistogramValue],
    scale_policy: ExponentialHistogramScalePolicy,
) -> Result<Option<ExponentialHistogramValue>, ExponentialHistogramMergeError> {
    let Some(first) = values.first() else {
        return Ok(None);
    };

    let target_scale = values
        .iter()
        .map(|value| value.scale)
        .min()
        .unwrap_or(first.scale);
    let target_scale = match scale_policy {
        ExponentialHistogramScalePolicy::Keep => target_scale,
        ExponentialHistogramScalePolicy::DownscaleToMaxScale(max_scale) => {
            target_scale.min(max_scale)
        }
    };

    let zero_threshold_bits = first.zero_threshold.to_bits();
    let mut count = 0u64;
    let mut zero_count = 0u64;
    let mut sum = 0.0f64;
    let mut all_sums_present = true;
    let mut min = None;
    let mut max = None;
    let mut positive = BTreeMap::new();
    let mut negative = BTreeMap::new();

    for value in values {
        if value.zero_threshold.to_bits() != zero_threshold_bits {
            return Err(ExponentialHistogramMergeError::ZeroThresholdMismatch);
        }

        count = count
            .checked_add(value.count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;
        zero_count = zero_count
            .checked_add(value.zero_count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;

        if let Some(value_sum) = value.sum {
            sum += value_sum;
        } else {
            all_sums_present = false;
        }

        min = merge_optional_min(min, value.min);
        max = merge_optional_max(max, value.max);

        add_exponential_histogram_bucket_maps(
            &mut positive,
            downscale_exponential_histogram_buckets_to_map(
                &value.positive,
                value.scale,
                target_scale,
            )?,
        )?;
        add_exponential_histogram_bucket_maps(
            &mut negative,
            downscale_exponential_histogram_buckets_to_map(
                &value.negative,
                value.scale,
                target_scale,
            )?,
        )?;
    }

    Ok(Some(ExponentialHistogramValue {
        count,
        sum: all_sums_present.then_some(sum),
        min,
        max,
        scale: target_scale,
        zero_threshold: first.zero_threshold,
        zero_count,
        metadata: first.metadata,
        positive: exponential_histogram_bucket_map_to_buckets(positive)?,
        negative: exponential_histogram_bucket_map_to_buckets(negative)?,
    }))
}

pub fn downscale_exponential_histogram_buckets_to_map(
    buckets: &ExponentialHistogramBuckets,
    source_scale: i32,
    target_scale: i32,
) -> Result<BTreeMap<i32, u64>, ExponentialHistogramMergeError> {
    if target_scale > source_scale {
        return Err(
            ExponentialHistogramMergeError::TargetScaleHigherThanSource {
                source_scale,
                target_scale,
            },
        );
    }
    let shift = source_scale
        .checked_sub(target_scale)
        .ok_or(ExponentialHistogramMergeError::ScaleDeltaTooLarge)?;
    let divisor = 1i64
        .checked_shl(
            u32::try_from(shift).map_err(|_| ExponentialHistogramMergeError::ScaleDeltaTooLarge)?,
        )
        .ok_or(ExponentialHistogramMergeError::ScaleDeltaTooLarge)?;

    let mut map = BTreeMap::new();
    for (idx, count) in buckets.counts.iter().copied().enumerate() {
        let source_index = i64::from(buckets.offset)
            .checked_add(
                i64::try_from(idx)
                    .map_err(|_| ExponentialHistogramMergeError::BucketIndexOverflow)?,
            )
            .ok_or(ExponentialHistogramMergeError::BucketIndexOverflow)?;
        let target_index = floor_div_i64(source_index, divisor);
        let target_index = i32::try_from(target_index)
            .map_err(|_| ExponentialHistogramMergeError::BucketIndexOverflow)?;
        let entry = map.entry(target_index).or_insert(0u64);
        *entry = entry
            .checked_add(count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;
    }
    Ok(map)
}

fn add_exponential_histogram_bucket_maps(
    out: &mut BTreeMap<i32, u64>,
    input: BTreeMap<i32, u64>,
) -> Result<(), ExponentialHistogramMergeError> {
    for (index, count) in input {
        let entry = out.entry(index).or_insert(0);
        *entry = entry
            .checked_add(count)
            .ok_or(ExponentialHistogramMergeError::BucketCountOverflow)?;
    }
    Ok(())
}

fn exponential_histogram_bucket_map_to_buckets(
    map: BTreeMap<i32, u64>,
) -> Result<ExponentialHistogramBuckets, ExponentialHistogramMergeError> {
    let Some((&offset, _)) = map.first_key_value() else {
        return Ok(ExponentialHistogramBuckets {
            offset: 0,
            counts: Vec::new(),
        });
    };
    let Some((&last, _)) = map.last_key_value() else {
        unreachable!("non-empty BTreeMap has a last key");
    };
    let span = i64::from(last)
        .checked_sub(i64::from(offset))
        .and_then(|span| span.checked_add(1))
        .ok_or(ExponentialHistogramMergeError::BucketSpanTooWide)?;
    let span =
        usize::try_from(span).map_err(|_| ExponentialHistogramMergeError::BucketSpanTooWide)?;
    let mut counts = vec![0u64; span];
    for (index, count) in map {
        let idx = usize::try_from(i64::from(index) - i64::from(offset))
            .map_err(|_| ExponentialHistogramMergeError::BucketIndexOverflow)?;
        counts[idx] = count;
    }
    Ok(ExponentialHistogramBuckets { offset, counts })
}

fn merge_optional_min(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn merge_optional_max(left: Option<f64>, right: Option<f64>) -> Option<f64> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.max(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

fn floor_div_i64(value: i64, divisor: i64) -> i64 {
    debug_assert!(divisor > 0);
    let quotient = value / divisor;
    let remainder = value % divisor;
    if remainder != 0 && value < 0 {
        quotient - 1
    } else {
        quotient
    }
}

fn exponential_histogram_base(scale: i32) -> f64 {
    2.0f64.powf(2.0f64.powi(-scale))
}

fn exponential_histogram_positive_bucket_count_le(
    buckets: &ExponentialHistogramBuckets,
    base: f64,
    le: f64,
) -> u64 {
    buckets
        .counts
        .iter()
        .enumerate()
        .filter_map(|(idx, count)| {
            let bucket_index = buckets
                .offset
                .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
            let upper = base.powi(bucket_index.saturating_add(1));
            (upper <= le).then_some(*count)
        })
        .fold(0u64, u64::saturating_add)
}

fn exponential_histogram_negative_bucket_count_le(
    buckets: &ExponentialHistogramBuckets,
    base: f64,
    le: f64,
) -> u64 {
    buckets
        .counts
        .iter()
        .enumerate()
        .filter_map(|(idx, count)| {
            let bucket_index = buckets
                .offset
                .saturating_add(i32::try_from(idx).unwrap_or(i32::MAX));
            let upper = -base.powi(bucket_index);
            (upper <= le).then_some(*count)
        })
        .fold(0u64, u64::saturating_add)
}

fn projected_head_labels(
    base_labels: &[(String, String)],
    metric_name: &str,
    metric_suffix: &str,
    extra_label: Option<(&str, String)>,
) -> Vec<(String, String)> {
    let mut labels = Vec::with_capacity(base_labels.len() + usize::from(extra_label.is_some()));
    let mut metric_seen = false;
    let extra_key = extra_label.as_ref().map(|(key, _)| *key);
    for (key, value) in base_labels {
        if key == METRIC_NAME_LABEL {
            labels.push((key.clone(), format!("{metric_name}{metric_suffix}")));
            metric_seen = true;
        } else if extra_key != Some(key.as_str()) {
            labels.push((key.clone(), value.clone()));
        }
    }
    if !metric_seen {
        labels.push((
            METRIC_NAME_LABEL.to_string(),
            format!("{metric_name}{metric_suffix}"),
        ));
    }
    if let Some((key, value)) = extra_label {
        labels.push((key.to_string(), value));
    }
    labels.sort_by(|left, right| left.0.cmp(&right.0));
    labels
}

fn push_head_projected_sample(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    labels: Vec<(String, String)>,
    timestamp_ms: u64,
    value: f64,
) {
    let series_id = segment_series_id(&labels);
    let entry = out
        .entry(series_id)
        .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
    entry.push_sample(timestamp_ms, value);
}

fn push_head_projected_sample_with_counter_reset_hint(
    out: &mut BTreeMap<u64, SegmentQueryResult>,
    labels: Vec<(String, String)>,
    timestamp_ms: u64,
    value: f64,
    reset_hint: CounterResetHint,
) {
    let series_id = segment_series_id(&labels);
    let entry = out
        .entry(series_id)
        .or_insert_with(|| SegmentQueryResult::new(series_id, labels));
    entry.push_sample_with_counter_reset_hint(timestamp_ms, value, reset_hint);
}

fn format_promql_float_label(value: f64) -> String {
    if value.is_infinite() && value.is_sign_positive() {
        "+Inf".to_string()
    } else {
        value.to_string()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SeriesEncoding {
    Float(FloatEncoding),
    Int(IntEncoding),
    Histogram(VarLenEncodingKind),
    ExponentialHistogram(VarLenEncodingKind),
    Summary(VarLenEncodingKind),
}

type HistogramRawCodec = VarLenCodec<HistogramValue>;
type HistogramSchemaCodec = SchemaVarLenCodec<HistogramValue>;
type ExponentialHistogramRawCodec = VarLenCodec<ExponentialHistogramValue>;
type ExponentialHistogramSchemaCodec = SchemaVarLenCodec<ExponentialHistogramValue>;
type SummaryRawCodec = VarLenCodec<SummaryValue>;
type SummarySchemaCodec = SchemaVarLenCodec<SummaryValue>;

#[derive(Debug)]
enum EncodedSeries {
    FloatRaw(Series<FloatRawCodec>),
    IntRaw(Series<IntRawCodec>),
    FloatGorilla(Series<FloatGorillaCodec>),
    FloatElf(Series<FloatElfCodec>),
    FloatAlp(Series<FloatAlpCodec>),
    FloatAlpRd(Series<FloatAlpRdCodec>),
    FloatAlpSpiral(Series<FloatAlpSpiralCodec>),
    FloatAlpRdSpiral(Series<FloatAlpRdSpiralCodec>),
    FloatChimp128DuckDB(Series<FloatChimp128DuckDBDeferredCodec>),
    FloatChimp128Baseline(Series<FloatChimp128BaselineDeferredCodec>),
    IntDelta(Series<IntDeltaCodec>),
    Histogram(Series<HistogramRawCodec>),
    HistogramSchema(Series<HistogramSchemaCodec>),
    ExponentialHistogram(Series<ExponentialHistogramRawCodec>),
    ExponentialHistogramSchema(Series<ExponentialHistogramSchemaCodec>),
    Summary(Series<SummaryRawCodec>),
    SummarySchema(Series<SummarySchemaCodec>),
}

impl EncodedSeries {
    fn new(encoding: SeriesEncoding) -> Self {
        match encoding {
            SeriesEncoding::Float(FloatEncoding::Gorilla) => Self::FloatGorilla(Series::new()),
            SeriesEncoding::Float(FloatEncoding::Elf) => Self::FloatElf(Series::new()),
            SeriesEncoding::Float(FloatEncoding::Alp) => Self::FloatAlp(Series::new()),
            SeriesEncoding::Float(FloatEncoding::AlpRd) => Self::FloatAlpRd(Series::new()),
            SeriesEncoding::Float(FloatEncoding::AlpSpiral) => Self::FloatAlpSpiral(Series::new()),
            SeriesEncoding::Float(FloatEncoding::AlpRdSpiral) => {
                Self::FloatAlpRdSpiral(Series::new())
            }
            SeriesEncoding::Float(FloatEncoding::Chimp128DuckDB) => {
                Self::FloatChimp128DuckDB(Series::new())
            }
            SeriesEncoding::Float(FloatEncoding::Chimp128Baseline) => {
                Self::FloatChimp128Baseline(Series::new())
            }
            SeriesEncoding::Float(FloatEncoding::Raw) => Self::FloatRaw(Series::new()),
            SeriesEncoding::Int(IntEncoding::DeltaZigZag) => Self::IntDelta(Series::new()),
            SeriesEncoding::Int(IntEncoding::Raw) => Self::IntRaw(Series::new()),
            SeriesEncoding::Histogram(VarLenEncodingKind::Raw) => Self::Histogram(Series::new()),
            SeriesEncoding::Histogram(VarLenEncodingKind::Schema) => {
                Self::HistogramSchema(Series::new())
            }
            SeriesEncoding::ExponentialHistogram(VarLenEncodingKind::Raw) => {
                Self::ExponentialHistogram(Series::new())
            }
            SeriesEncoding::ExponentialHistogram(VarLenEncodingKind::Schema) => {
                Self::ExponentialHistogramSchema(Series::new())
            }
            SeriesEncoding::Summary(VarLenEncodingKind::Raw) => Self::Summary(Series::new()),
            SeriesEncoding::Summary(VarLenEncodingKind::Schema) => {
                Self::SummarySchema(Series::new())
            }
        }
    }

    fn kind(&self) -> SampleKind {
        match self {
            Self::FloatGorilla(_)
            | Self::FloatElf(_)
            | Self::FloatAlp(_)
            | Self::FloatAlpRd(_)
            | Self::FloatAlpSpiral(_)
            | Self::FloatAlpRdSpiral(_)
            | Self::FloatChimp128DuckDB(_)
            | Self::FloatChimp128Baseline(_)
            | Self::FloatRaw(_) => SampleKind::Float,
            Self::IntDelta(_) | Self::IntRaw(_) => SampleKind::Int64,
            Self::Histogram(_) | Self::HistogramSchema(_) => SampleKind::Histogram,
            Self::ExponentialHistogram(_) | Self::ExponentialHistogramSchema(_) => {
                SampleKind::ExponentialHistogram
            }
            Self::Summary(_) | Self::SummarySchema(_) => SampleKind::Summary,
        }
    }

    fn codec_name(&self) -> &'static str {
        match self {
            Self::FloatGorilla(series) => series.codec_name(),
            Self::FloatElf(series) => series.codec_name(),
            Self::FloatAlp(series) => series.codec_name(),
            Self::FloatAlpRd(series) => series.codec_name(),
            Self::FloatAlpSpiral(series) => series.codec_name(),
            Self::FloatAlpRdSpiral(series) => series.codec_name(),
            Self::FloatChimp128DuckDB(series) => series.codec_name(),
            Self::FloatChimp128Baseline(series) => series.codec_name(),
            Self::FloatRaw(series) => series.codec_name(),
            Self::IntDelta(series) => series.codec_name(),
            Self::IntRaw(series) => series.codec_name(),
            Self::Histogram(series) => series.codec_name(),
            Self::HistogramSchema(series) => series.codec_name(),
            Self::ExponentialHistogram(series) => series.codec_name(),
            Self::ExponentialHistogramSchema(series) => series.codec_name(),
            Self::Summary(series) => series.codec_name(),
            Self::SummarySchema(series) => series.codec_name(),
        }
    }

    fn sample_count(&self) -> u64 {
        match self {
            Self::FloatGorilla(series) => series.sample_count(),
            Self::FloatElf(series) => series.sample_count(),
            Self::FloatAlp(series) => series.sample_count(),
            Self::FloatAlpRd(series) => series.sample_count(),
            Self::FloatAlpSpiral(series) => series.sample_count(),
            Self::FloatAlpRdSpiral(series) => series.sample_count(),
            Self::FloatChimp128DuckDB(series) => series.sample_count(),
            Self::FloatChimp128Baseline(series) => series.sample_count(),
            Self::FloatRaw(series) => series.sample_count(),
            Self::IntDelta(series) => series.sample_count(),
            Self::IntRaw(series) => series.sample_count(),
            Self::Histogram(series) => series.sample_count(),
            Self::HistogramSchema(series) => series.sample_count(),
            Self::ExponentialHistogram(series) => series.sample_count(),
            Self::ExponentialHistogramSchema(series) => series.sample_count(),
            Self::Summary(series) => series.sample_count(),
            Self::SummarySchema(series) => series.sample_count(),
        }
    }

    fn block_count(&self) -> usize {
        match self {
            Self::FloatGorilla(series) => series.block_count(),
            Self::FloatElf(series) => series.block_count(),
            Self::FloatAlp(series) => series.block_count(),
            Self::FloatAlpRd(series) => series.block_count(),
            Self::FloatAlpSpiral(series) => series.block_count(),
            Self::FloatAlpRdSpiral(series) => series.block_count(),
            Self::FloatChimp128DuckDB(series) => series.block_count(),
            Self::FloatChimp128Baseline(series) => series.block_count(),
            Self::FloatRaw(series) => series.block_count(),
            Self::IntDelta(series) => series.block_count(),
            Self::IntRaw(series) => series.block_count(),
            Self::Histogram(series) => series.block_count(),
            Self::HistogramSchema(series) => series.block_count(),
            Self::ExponentialHistogram(series) => series.block_count(),
            Self::ExponentialHistogramSchema(series) => series.block_count(),
            Self::Summary(series) => series.block_count(),
            Self::SummarySchema(series) => series.block_count(),
        }
    }

    fn for_each_block_sample<F>(&self, f: &mut F)
    where
        F: FnMut(u64),
    {
        match self {
            Self::FloatGorilla(series) => series.for_each_block_sample(f),
            Self::FloatElf(series) => series.for_each_block_sample(f),
            Self::FloatAlp(series) => series.for_each_block_sample(f),
            Self::FloatAlpRd(series) => series.for_each_block_sample(f),
            Self::FloatAlpSpiral(series) => series.for_each_block_sample(f),
            Self::FloatAlpRdSpiral(series) => series.for_each_block_sample(f),
            Self::FloatChimp128DuckDB(series) => series.for_each_block_sample(f),
            Self::FloatChimp128Baseline(series) => series.for_each_block_sample(f),
            Self::FloatRaw(series) => series.for_each_block_sample(f),
            Self::IntDelta(series) => series.for_each_block_sample(f),
            Self::IntRaw(series) => series.for_each_block_sample(f),
            Self::Histogram(series) => series.for_each_block_sample(f),
            Self::HistogramSchema(series) => series.for_each_block_sample(f),
            Self::ExponentialHistogram(series) => series.for_each_block_sample(f),
            Self::ExponentialHistogramSchema(series) => series.for_each_block_sample(f),
            Self::Summary(series) => series.for_each_block_sample(f),
            Self::SummarySchema(series) => series.for_each_block_sample(f),
        }
    }

    fn estimated_bytes(&self) -> usize {
        match self {
            Self::FloatGorilla(series) => series.estimated_bytes(),
            Self::FloatElf(series) => series.estimated_bytes(),
            Self::FloatAlp(series) => series.estimated_bytes(),
            Self::FloatAlpRd(series) => series.estimated_bytes(),
            Self::FloatAlpSpiral(series) => series.estimated_bytes(),
            Self::FloatAlpRdSpiral(series) => series.estimated_bytes(),
            Self::FloatChimp128DuckDB(series) => series.estimated_bytes(),
            Self::FloatChimp128Baseline(series) => series.estimated_bytes(),
            Self::FloatRaw(series) => series.estimated_bytes(),
            Self::IntDelta(series) => series.estimated_bytes(),
            Self::IntRaw(series) => series.estimated_bytes(),
            Self::Histogram(series) => series.estimated_bytes(),
            Self::HistogramSchema(series) => series.estimated_bytes(),
            Self::ExponentialHistogram(series) => series.estimated_bytes(),
            Self::ExponentialHistogramSchema(series) => series.estimated_bytes(),
            Self::Summary(series) => series.estimated_bytes(),
            Self::SummarySchema(series) => series.estimated_bytes(),
        }
    }

    fn payload_bytes(&self) -> usize {
        match self {
            Self::FloatGorilla(series) => series.payload_bytes(),
            Self::FloatElf(series) => series.payload_bytes(),
            Self::FloatAlp(series) => series.payload_bytes(),
            Self::FloatAlpRd(series) => series.payload_bytes(),
            Self::FloatAlpSpiral(series) => series.payload_bytes(),
            Self::FloatAlpRdSpiral(series) => series.payload_bytes(),
            Self::FloatChimp128DuckDB(series) => series.payload_bytes(),
            Self::FloatChimp128Baseline(series) => series.payload_bytes(),
            Self::FloatRaw(series) => series.payload_bytes(),
            Self::IntDelta(series) => series.payload_bytes(),
            Self::IntRaw(series) => series.payload_bytes(),
            Self::Histogram(series) => series.payload_bytes(),
            Self::HistogramSchema(series) => series.payload_bytes(),
            Self::ExponentialHistogram(series) => series.payload_bytes(),
            Self::ExponentialHistogramSchema(series) => series.payload_bytes(),
            Self::Summary(series) => series.payload_bytes(),
            Self::SummarySchema(series) => series.payload_bytes(),
        }
    }

    fn seal(&mut self, arena: &mut BlockArena) {
        match self {
            Self::FloatGorilla(series) => series.seal_current(arena),
            Self::FloatElf(series) => series.seal_current(arena),
            Self::FloatAlp(series) => series.seal_current(arena),
            Self::FloatAlpRd(series) => series.seal_current(arena),
            Self::FloatAlpSpiral(series) => series.seal_current(arena),
            Self::FloatAlpRdSpiral(series) => series.seal_current(arena),
            Self::FloatChimp128DuckDB(series) => series.seal_current(arena),
            Self::FloatChimp128Baseline(series) => series.seal_current(arena),
            Self::FloatRaw(series) => series.seal_current(arena),
            Self::IntDelta(series) => series.seal_current(arena),
            Self::IntRaw(series) => series.seal_current(arena),
            Self::Histogram(series) => series.seal_current(arena),
            Self::HistogramSchema(series) => series.seal_current(arena),
            Self::ExponentialHistogram(series) => series.seal_current(arena),
            Self::ExponentialHistogramSchema(series) => series.seal_current(arena),
            Self::Summary(series) => series.seal_current(arena),
            Self::SummarySchema(series) => series.seal_current(arena),
        }
    }

    fn push_sample(
        &mut self,
        series: SeriesRef,
        base_ms: u64,
        timestamp_ms: u64,
        value: SampleValue,
        block_size: usize,
        arena: &mut BlockArena,
    ) -> io::Result<()> {
        match self {
            Self::FloatGorilla(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatElf(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatAlp(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatAlpRd(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatAlpSpiral(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatAlpRdSpiral(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatChimp128DuckDB(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatChimp128Baseline(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::FloatRaw(series_buf) => match value {
                SampleValue::Float(value) => series_buf.push_sample(
                    series,
                    "float",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::IntDelta(series_buf) => match value {
                SampleValue::Int64(value) => series_buf.push_sample(
                    series,
                    "int64",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "int series received non-int sample",
                )),
            },
            Self::IntRaw(series_buf) => match value {
                SampleValue::Int64(value) => series_buf.push_sample(
                    series,
                    "int64",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                SampleValue::Float(_) => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "int series received float sample",
                )),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "int series received non-int sample",
                )),
            },
            Self::Histogram(series_buf) => match value {
                SampleValue::Histogram(value) => series_buf.push_sample(
                    series,
                    "histogram",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "histogram series received non-histogram sample",
                )),
            },
            Self::HistogramSchema(series_buf) => match value {
                SampleValue::Histogram(value) => series_buf.push_sample(
                    series,
                    "histogram_schema",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "histogram series received non-histogram sample",
                )),
            },
            Self::ExponentialHistogram(series_buf) => match value {
                SampleValue::ExponentialHistogram(value) => series_buf.push_sample(
                    series,
                    "exponential_histogram",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "exponential histogram series received non-histogram sample",
                )),
            },
            Self::ExponentialHistogramSchema(series_buf) => match value {
                SampleValue::ExponentialHistogram(value) => series_buf.push_sample(
                    series,
                    "exponential_histogram_schema",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "exponential histogram series received non-histogram sample",
                )),
            },
            Self::Summary(series_buf) => match value {
                SampleValue::Summary(value) => series_buf.push_sample(
                    series,
                    "summary",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "summary series received non-summary sample",
                )),
            },
            Self::SummarySchema(series_buf) => match value {
                SampleValue::Summary(value) => series_buf.push_sample(
                    series,
                    "summary_schema",
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                    arena,
                ),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "summary series received non-summary sample",
                )),
            },
        }
    }

    fn into_samples(self, arena: &BlockArena) -> io::Result<SeriesSamples> {
        match self {
            Self::FloatGorilla(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatElf(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Elf,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatAlp(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Alp,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatAlpRd(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::AlpRd,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatAlpSpiral(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::AlpSpiral,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatAlpRdSpiral(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::AlpRdSpiral,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatChimp128DuckDB(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Chimp128DuckDB,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatChimp128Baseline(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Chimp128Baseline,
                samples: series.into_samples(arena)?,
            }),
            Self::FloatRaw(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Raw,
                samples: series.into_samples(arena)?,
            }),
            Self::IntDelta(series) => Ok(SeriesSamples::Int64 {
                encoding: IntEncoding::DeltaZigZag,
                samples: series.into_samples(arena)?,
            }),
            Self::IntRaw(series) => Ok(SeriesSamples::Int64 {
                encoding: IntEncoding::Raw,
                samples: series.into_samples(arena)?,
            }),
            Self::Histogram(series) => Ok(SeriesSamples::Histogram {
                samples: series.into_samples(arena)?,
            }),
            Self::HistogramSchema(series) => Ok(SeriesSamples::Histogram {
                samples: series.into_samples(arena)?,
            }),
            Self::ExponentialHistogram(series) => Ok(SeriesSamples::ExponentialHistogram {
                samples: series.into_samples(arena)?,
            }),
            Self::ExponentialHistogramSchema(series) => Ok(SeriesSamples::ExponentialHistogram {
                samples: series.into_samples(arena)?,
            }),
            Self::Summary(series) => Ok(SeriesSamples::Summary {
                samples: series.into_samples(arena)?,
            }),
            Self::SummarySchema(series) => Ok(SeriesSamples::Summary {
                samples: series.into_samples(arena)?,
            }),
        }
    }

    fn samples_in_range(
        &self,
        arena: &BlockArena,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<SeriesSamples> {
        match self {
            Self::FloatGorilla(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatElf(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Elf,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatAlp(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Alp,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatAlpRd(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::AlpRd,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatAlpSpiral(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::AlpSpiral,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatAlpRdSpiral(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::AlpRdSpiral,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatChimp128DuckDB(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Chimp128DuckDB,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatChimp128Baseline(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Chimp128Baseline,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::FloatRaw(series) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Raw,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::IntDelta(series) => Ok(SeriesSamples::Int64 {
                encoding: IntEncoding::DeltaZigZag,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::IntRaw(series) => Ok(SeriesSamples::Int64 {
                encoding: IntEncoding::Raw,
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::Histogram(series) => Ok(SeriesSamples::Histogram {
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::HistogramSchema(series) => Ok(SeriesSamples::Histogram {
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::ExponentialHistogram(series) => Ok(SeriesSamples::ExponentialHistogram {
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::ExponentialHistogramSchema(series) => Ok(SeriesSamples::ExponentialHistogram {
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::Summary(series) => Ok(SeriesSamples::Summary {
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
            Self::SummarySchema(series) => Ok(SeriesSamples::Summary {
                samples: series.samples_in_range(arena, start_ms, end_ms)?,
            }),
        }
    }
}

fn encode_f64(value: f64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn decode_f64(buf: &[u8], cursor: &mut usize) -> io::Result<f64> {
    if cursor.saturating_add(8) > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short f64"));
    }
    let value = f64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

fn encode_opt_f64(value: Option<f64>, out: &mut Vec<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            encode_f64(value, out);
        }
        None => out.push(0),
    }
}

fn decode_opt_f64(buf: &[u8], cursor: &mut usize) -> io::Result<Option<f64>> {
    if *cursor >= buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short option"));
    }
    let flag = buf[*cursor];
    *cursor += 1;
    match flag {
        0 => Ok(None),
        1 => Ok(Some(decode_f64(buf, cursor)?)),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid option flag",
        )),
    }
}

fn encode_typed_metadata(metadata: TypedSampleMetadata, out: &mut Vec<u8>) {
    encode_varint(u64::from(metadata.flags), out);
    encode_varint(metadata.temporality as u64, out);
    encode_varint(metadata.reset_hint as u64, out);
    match metadata.start_time_ms {
        Some(start_time_ms) => {
            out.push(1);
            encode_varint(start_time_ms, out);
        }
        None => out.push(0),
    }
}

fn decode_typed_metadata(buf: &[u8], cursor: &mut usize) -> io::Result<TypedSampleMetadata> {
    let flags = decode_varint(buf, cursor)?;
    let temporality = decode_temporality(decode_varint(buf, cursor)?)?;
    let reset_hint = decode_counter_reset_hint(decode_varint(buf, cursor)?)?;
    if *cursor >= buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "short start time option",
        ));
    }
    let start_time_ms = match buf[*cursor] {
        0 => {
            *cursor += 1;
            None
        }
        1 => {
            *cursor += 1;
            Some(decode_varint(buf, cursor)?)
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid start time option flag",
            ));
        }
    };
    Ok(TypedSampleMetadata {
        start_time_ms,
        flags: u32::try_from(flags)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "flags overflow"))?,
        temporality,
        reset_hint,
    })
}

fn decode_temporality(value: u64) -> io::Result<OtlpAggregationTemporality> {
    match value {
        0 => Ok(OtlpAggregationTemporality::Unspecified),
        1 => Ok(OtlpAggregationTemporality::Delta),
        2 => Ok(OtlpAggregationTemporality::Cumulative),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid aggregation temporality",
        )),
    }
}

fn decode_counter_reset_hint(value: u64) -> io::Result<CounterResetHint> {
    match value {
        0 => Ok(CounterResetHint::Unknown),
        1 => Ok(CounterResetHint::CounterReset),
        2 => Ok(CounterResetHint::NotCounterReset),
        3 => Ok(CounterResetHint::GaugeType),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid counter reset hint",
        )),
    }
}

fn decode_len(buf: &[u8], cursor: &mut usize) -> io::Result<usize> {
    let len = decode_varint(buf, cursor)?;
    usize::try_from(len).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length overflow"))
}

fn decode_i32(buf: &[u8], cursor: &mut usize) -> io::Result<i32> {
    let encoded = decode_varint(buf, cursor)?;
    let decoded = decode_zigzag_i64(encoded);
    i32::try_from(decoded).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "i32 overflow"))
}

fn encode_buckets(buckets: &ExponentialHistogramBuckets, out: &mut Vec<u8>) {
    encode_varint(encode_zigzag_i64(buckets.offset as i64), out);
    encode_varint(buckets.counts.len() as u64, out);
    for count in &buckets.counts {
        encode_varint(*count, out);
    }
}

fn decode_buckets(buf: &[u8], cursor: &mut usize) -> io::Result<ExponentialHistogramBuckets> {
    let offset = decode_i32(buf, cursor)?;
    let len = decode_len(buf, cursor)?;
    let mut counts = Vec::with_capacity(len);
    for _ in 0..len {
        counts.push(decode_varint(buf, cursor)?);
    }
    Ok(ExponentialHistogramBuckets { offset, counts })
}

fn ensure_consumed(buf: &[u8], cursor: usize) -> io::Result<()> {
    if cursor != buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "value buffer has trailing bytes",
        ));
    }
    Ok(())
}

#[derive(Debug)]
struct Series<C: BlockCodec> {
    blocks: SmallVec<[Block<C>; 1]>,
    current: Option<Box<BlockBuilder<C>>>,
    samples: u64,
}

impl<C: BlockCodec> Series<C> {
    fn new() -> Self {
        Self {
            blocks: SmallVec::new(),
            current: None,
            samples: 0,
        }
    }

    fn push_sample(
        &mut self,
        series: SeriesRef,
        value_kind: &'static str,
        base_ms: u64,
        timestamp_ms: u64,
        value: C::Value,
        block_size: usize,
        arena: &mut BlockArena,
    ) -> io::Result<()> {
        if timestamp_ms < base_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "timestamp precedes window start",
            ));
        }
        let current_full = self
            .current
            .as_ref()
            .is_some_and(|block| block.is_full(block_size));

        if current_full {
            if let Some(block) = self.current.as_ref() {
                let duration = Duration::from_millis(block.max_ts() - block.min_ts());
                let estimated_bytes = block.payload_bytes();
                let series_estimated_bytes = self.estimated_bytes();
                debug!(
                    "Head block completed series={} value_kind={} codec={} block_size={} min_ts={} max_ts={}, duration={:?} samples={} estimated_bytes={} series_estimated_bytes={} -> new block start_ts={}",
                    series.get(),
                    value_kind,
                    self.codec_name(),
                    block_size,
                    block.min_ts(),
                    block.max_ts(),
                    duration,
                    block.sample_count(),
                    estimated_bytes,
                    series_estimated_bytes,
                    timestamp_ms
                );
            }
            if let Some(block) = self.current.take() {
                self.blocks.push(block.seal(arena));
            }
        }

        match self.current.as_mut() {
            Some(block) => {
                if !block.is_full(block_size) {
                    block.push_sample(timestamp_ms, value, block_size)?;
                }
            }
            None => {
                self.current = Some(Box::new(BlockBuilder::new(
                    base_ms,
                    timestamp_ms,
                    value,
                    block_size,
                )?));
            }
        }

        self.samples = self.samples.saturating_add(1);
        Ok(())
    }

    fn seal_current(&mut self, arena: &mut BlockArena) {
        if let Some(block) = self.current.take() {
            self.blocks.push(block.seal(arena));
        }
    }

    fn into_samples(self, arena: &BlockArena) -> io::Result<Vec<(u64, C::Value)>> {
        if self.samples == 0 {
            return Ok(Vec::new());
        }

        let count = usize::try_from(self.samples).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidData, "series sample count overflow")
        })?;

        let mut out = Vec::with_capacity(count);
        debug_assert!(self.current.is_none(), "series has unsealed block");
        for block in self.blocks {
            out.extend(block.decode_samples(arena)?);
        }
        out.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
        Ok(out)
    }

    fn samples_in_range(
        &self,
        arena: &BlockArena,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<(u64, C::Value)>> {
        if end_ms <= start_ms || self.samples == 0 {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        for block in &self.blocks {
            if !block.overlaps(start_ms, end_ms) {
                continue;
            }
            for (ts, value) in block.decode_samples(arena)? {
                if ts >= start_ms && ts < end_ms {
                    out.push((ts, value));
                }
            }
        }
        if let Some(block) = &self.current
            && block.overlaps(start_ms, end_ms)
        {
            for (ts, value) in block.decode_samples()? {
                if ts >= start_ms && ts < end_ms {
                    out.push((ts, value));
                }
            }
        }
        out.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
        Ok(out)
    }

    fn codec_name(&self) -> &'static str {
        std::any::type_name::<C>()
    }

    fn sample_count(&self) -> u64 {
        self.samples
    }

    fn block_count(&self) -> usize {
        self.blocks
            .len()
            .saturating_add(self.current.as_ref().map_or(0, |_| 1))
    }

    fn for_each_block_sample<F>(&self, f: &mut F)
    where
        F: FnMut(u64),
    {
        for block in &self.blocks {
            f(u64::from(block.sample_count()));
        }
        if let Some(block) = &self.current {
            f(u64::from(block.sample_count()));
        }
    }

    fn estimated_bytes(&self) -> usize {
        let block_overhead = if self.blocks.spilled() {
            self.blocks
                .capacity()
                .saturating_mul(std::mem::size_of::<Block<C>>())
        } else {
            0
        };
        let current_overhead = self
            .current
            .as_ref()
            .map_or(0, |_| std::mem::size_of::<BlockBuilder<C>>());
        let block_data: usize = self
            .blocks
            .iter()
            .map(|block| block.estimated_bytes())
            .sum();
        let current_data = self
            .current
            .as_ref()
            .map_or(0, |block| block.payload_bytes());
        std::mem::size_of::<Self>()
            .saturating_add(block_overhead)
            .saturating_add(current_overhead)
            .saturating_add(block_data)
            .saturating_add(current_data)
    }

    fn payload_bytes(&self) -> usize {
        let sealed: usize = self.blocks.iter().fold(0usize, |acc, block| {
            acc.saturating_add(block.estimated_bytes())
        });
        let current = self
            .current
            .as_ref()
            .map_or(0, |block| block.payload_bytes());
        sealed.saturating_add(current)
    }
}

fn window_for(timestamp_ms: u64, duration_ms: u64) -> (u64, u64) {
    let start_ms = timestamp_ms.saturating_sub(timestamp_ms % duration_ms);
    (start_ms, start_ms.saturating_add(duration_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::labels::{
        DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore,
    };
    use crate::storage::arena::BlockArena;
    use crate::storage::block::{
        BlockBuilder, BlockCodec, FloatChimp128DuckDBDeferredCodec, FloatGorillaCodec,
        FloatRawCodec, IntDeltaCodec, IntRawCodec,
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

        let selector = SegmentSelector::with_metric(
            "cpu.usage",
            vec![LabelMatcher::eq("pod.name", "backend-1")],
        );
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

        let selector = SegmentSelector::with_metric(
            "cpu.usage",
            vec![LabelMatcher::eq("pod.name", "backend-1")],
        );
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

        let selector = SegmentSelector::with_metric(
            "cpu.usage",
            vec![LabelMatcher::eq("pod.name", "backend-1")],
        );
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

        let selector = SegmentSelector::with_metric(
            "cpu.usage",
            vec![LabelMatcher::eq("pod.name", "backend-1")],
        );
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
        let repeated = downscale_exponential_histogram(
            &downscale_exponential_histogram(&value, 1).unwrap(),
            0,
        )
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
}
