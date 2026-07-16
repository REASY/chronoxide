use super::*;

#[derive(Debug, Clone)]
pub struct HeadConfig {
    pub window_duration: Duration,
    pub block_size: usize,
    pub float_encoding: FloatEncoding,
    pub int_encoding: IntEncoding,
    pub varlen_encoding: VarLenEncodingKind,
    pub out_of_order_time_window: Duration,
    pub compact_numeric_series: bool,
    pub adaptive_series_table: bool,
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
            compact_numeric_series: true,
            adaptive_series_table: true,
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
            compact_numeric_series: true,
            adaptive_series_table: true,
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

    pub fn with_compact_numeric_series(mut self, enabled: bool) -> Self {
        self.compact_numeric_series = enabled;
        self
    }

    pub fn with_adaptive_series_table(mut self, enabled: bool) -> Self {
        self.adaptive_series_table = enabled;
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
pub(super) enum SampleKind {
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

/// Structural snapshot of a head window's series lookup table.
///
/// These counters are collected outside the ingest hot path when a window is
/// flushed. Capacities describe retained container allocation, not allocator
/// metadata or encoded sample payloads.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct HeadSeriesTableStats {
    pub adaptive: bool,
    pub series: usize,
    pub page_directory_len: usize,
    pub page_directory_capacity: usize,
    pub sparse_pages: usize,
    pub sparse_series: usize,
    pub sparse_capacity: usize,
    pub refs_above_paged_limit: usize,
    pub sparse_slot_capacity: usize,
    pub direct_pages: usize,
    pub direct_series: usize,
    pub direct_slot_index_bytes: usize,
    pub direct_reverse_slot_capacity: usize,
    pub direct_value_capacity: usize,
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

pub(super) const DEFAULT_HEAD_ARENA_PAGE_BYTES: usize = 4 * 1024 * 1024;

impl SampleValue {
    pub(super) fn kind(&self) -> SampleKind {
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

pub(crate) trait TypedCounterValue {
    fn metadata(&self) -> TypedSampleMetadata;
    fn count(&self) -> u64;
    fn sum(&self) -> Option<f64>;
}

macro_rules! impl_optional_sum_typed_counter_value {
    ($($value:ty),+ $(,)?) => {
        $(
            impl TypedCounterValue for $value {
                fn metadata(&self) -> TypedSampleMetadata { self.metadata }
                fn count(&self) -> u64 { self.count }
                fn sum(&self) -> Option<f64> { self.sum }
            }
        )+
    };
}

impl_optional_sum_typed_counter_value!(HistogramValue, ExponentialHistogramValue);

impl TypedCounterValue for SummaryValue {
    fn metadata(&self) -> TypedSampleMetadata {
        self.metadata
    }

    fn count(&self) -> u64 {
        self.count
    }

    fn sum(&self) -> Option<f64> {
        Some(self.sum)
    }
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

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

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
    pub(super) fn is_empty(&self) -> bool {
        match self {
            Self::Float { samples, .. } => samples.is_empty(),
            Self::Int64 { samples, .. } => samples.is_empty(),
            Self::Histogram { samples } => samples.is_empty(),
            Self::ExponentialHistogram { samples } => samples.is_empty(),
            Self::Summary { samples } => samples.is_empty(),
        }
    }
}
