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
    pub adaptive_last_timestamp_table: bool,
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
            adaptive_last_timestamp_table: true,
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
            adaptive_last_timestamp_table: true,
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

    pub fn with_adaptive_last_timestamp_table(mut self, enabled: bool) -> Self {
        self.adaptive_last_timestamp_table = enabled;
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SampleKind {
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
/// The snapshot itself is O(1): structural counters are maintained as the
/// table changes, and no keys, pages, or occupancy vectors are scanned when a
/// flushed window is recorded. Capacities describe retained container
/// allocation, not allocator metadata or encoded sample payloads.
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

/// Structural snapshot of the long-lived per-partition last-timestamp table.
///
/// The snapshot itself is O(1): structural counters are maintained on accepted
/// inserts, and no table keys, pages, or occupancy bitmaps are scanned when the
/// periodic ingest report reads them. Capacities and `paged_allocated_bytes`
/// exclude allocator metadata.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct LastTimestampTableStats {
    pub adaptive: bool,
    pub series: usize,
    pub page_directory_len: usize,
    pub page_directory_capacity: usize,
    pub sparse_pages: usize,
    pub sparse_series: usize,
    pub sparse_capacity: usize,
    pub refs_above_paged_limit: usize,
    pub dense_pages: usize,
    pub dense_series: usize,
    pub paged_allocated_bytes: usize,
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
pub(super) const LIVE_HEAD_ARENA_INITIAL_PAGE_BYTES: usize = 16 * 1024;

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

    /// Validates typed shapes before label, reset, or head state is mutated.
    ///
    /// Block codecs repeat these checks defensively at their byte boundary.
    pub fn validate_for_storage(&self) -> io::Result<()> {
        match self {
            Self::Float(_) | Self::Int64(_) => Ok(()),
            Self::Histogram(value) => validate_histogram_shape(
                &value.explicit_bounds,
                &value.bucket_counts,
                value.count,
                io::ErrorKind::InvalidInput,
            ),
            Self::ExponentialHistogram(value) => validate_exponential_histogram_shape(
                value.zero_count,
                &value.positive.counts,
                &value.negative.counts,
                value.count,
                io::ErrorKind::InvalidInput,
            ),
            Self::Summary(value) => validate_summary_quantile_positions(
                value.quantiles.iter().map(|value| value.quantile),
                io::ErrorKind::InvalidInput,
            ),
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

fn validate_histogram_shape(
    explicit_bounds: &[f64],
    bucket_counts: &[u64],
    count: u64,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    validate_histogram_schema_shape(explicit_bounds, bucket_counts.len(), error_kind)?;
    validate_histogram_bucket_total(bucket_counts, count, error_kind)
}

fn validate_histogram_schema_shape(
    explicit_bounds: &[f64],
    bucket_count: usize,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    if explicit_bounds.iter().any(|bound| !bound.is_finite())
        || explicit_bounds.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(io::Error::new(
            error_kind,
            "histogram explicit bounds must be finite and strictly ascending",
        ));
    }
    let expected_buckets = explicit_bounds
        .len()
        .checked_add(1)
        .ok_or_else(|| io::Error::new(error_kind, "histogram bucket length overflows"))?;
    if bucket_count != expected_buckets {
        return Err(io::Error::new(
            error_kind,
            "histogram bucket length must equal explicit bounds plus one",
        ));
    }
    Ok(())
}

fn validate_histogram_bucket_total(
    bucket_counts: &[u64],
    count: u64,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    let total = checked_count_total(bucket_counts.iter().copied(), error_kind, "histogram")?;
    if total != count {
        return Err(io::Error::new(
            error_kind,
            "histogram bucket total must equal count",
        ));
    }
    Ok(())
}

fn validate_exponential_histogram_shape(
    zero_count: u64,
    positive_counts: &[u64],
    negative_counts: &[u64],
    count: u64,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    let total = checked_count_total(
        std::iter::once(zero_count)
            .chain(positive_counts.iter().copied())
            .chain(negative_counts.iter().copied()),
        error_kind,
        "exponential histogram",
    )?;
    if total != count {
        return Err(io::Error::new(
            error_kind,
            "exponential histogram bucket total must equal count",
        ));
    }
    Ok(())
}

fn checked_count_total(
    counts: impl IntoIterator<Item = u64>,
    error_kind: io::ErrorKind,
    field: &'static str,
) -> io::Result<u64> {
    counts.into_iter().try_fold(0u64, |total, value| {
        total.checked_add(value).ok_or_else(|| {
            io::Error::new(error_kind, format!("{field} bucket total overflows u64"))
        })
    })
}

fn validate_summary_quantile_positions(
    quantiles: impl IntoIterator<Item = f64>,
    error_kind: io::ErrorKind,
) -> io::Result<()> {
    let mut previous = None;
    for quantile in quantiles {
        if !quantile.is_finite()
            || !(0.0..=1.0).contains(&quantile)
            || previous.is_some_and(|previous| previous >= quantile)
        {
            return Err(io::Error::new(
                error_kind,
                "summary quantile positions must be finite, within [0, 1], and strictly ascending",
            ));
        }
        previous = Some(quantile);
    }
    Ok(())
}

impl VarLenEncoding for HistogramValue {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
        validate_histogram_shape(
            &self.explicit_bounds,
            &self.bucket_counts,
            self.count,
            io::ErrorKind::InvalidInput,
        )?;
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
        ensure_decoded_items_fit(buf, cursor, bounds_len, 8, "histogram explicit bounds")?;
        let mut explicit_bounds = try_decoded_vec(bounds_len, "decoded histogram bounds")?;
        for _ in 0..bounds_len {
            explicit_bounds.push(decode_f64(buf, &mut cursor)?);
        }
        let counts_len = decode_len(buf, &mut cursor)?;
        ensure_decoded_items_fit(buf, cursor, counts_len, 1, "histogram bucket counts")?;
        let mut bucket_counts = try_decoded_vec(counts_len, "decoded histogram bucket counts")?;
        for _ in 0..counts_len {
            bucket_counts.push(decode_varint(buf, &mut cursor)?);
        }
        validate_histogram_shape(
            &explicit_bounds,
            &bucket_counts,
            count,
            io::ErrorKind::InvalidData,
        )?;
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
        validate_exponential_histogram_shape(
            self.zero_count,
            &self.positive.counts,
            &self.negative.counts,
            self.count,
            io::ErrorKind::InvalidInput,
        )?;
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
        validate_exponential_histogram_shape(
            zero_count,
            &positive.counts,
            &negative.counts,
            count,
            io::ErrorKind::InvalidData,
        )?;
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
        validate_summary_quantile_positions(
            self.quantiles.iter().map(|value| value.quantile),
            io::ErrorKind::InvalidInput,
        )?;
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
        ensure_decoded_items_fit(buf, cursor, quantile_len, 16, "summary quantile pairs")?;
        let mut quantiles = try_decoded_vec(quantile_len, "decoded summary quantile pairs")?;
        for _ in 0..quantile_len {
            let quantile = decode_f64(buf, &mut cursor)?;
            let value = decode_f64(buf, &mut cursor)?;
            quantiles.push(SummaryQuantileValue { quantile, value });
        }
        validate_summary_quantile_positions(
            quantiles.iter().map(|value| value.quantile),
            io::ErrorKind::InvalidData,
        )?;
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
        validate_histogram_schema_shape(
            &self.explicit_bounds,
            self.bucket_counts.len(),
            io::ErrorKind::InvalidInput,
        )?;
        encode_varint(self.explicit_bounds.len() as u64, out);
        for bound in &self.explicit_bounds {
            encode_f64(*bound, out);
        }
        encode_varint(self.bucket_counts.len() as u64, out);
        Ok(())
    }

    fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
        let bounds_len = decode_len(buf, cursor)?;
        ensure_decoded_items_fit(buf, *cursor, bounds_len, 8, "histogram schema bounds")?;
        let mut explicit_bounds = try_decoded_vec(bounds_len, "decoded histogram schema bounds")?;
        for _ in 0..bounds_len {
            explicit_bounds.push(decode_f64(buf, cursor)?);
        }
        let bucket_len = decode_len(buf, cursor)?;
        let expected_bucket_len = bounds_len.checked_add(1).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "histogram bucket length overflows",
            )
        })?;
        if bucket_len != expected_bucket_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "histogram bucket length must equal explicit bounds plus one",
            ));
        }
        if explicit_bounds.iter().any(|bound| !bound.is_finite())
            || explicit_bounds.windows(2).any(|pair| pair[0] >= pair[1])
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "histogram explicit bounds must be finite and strictly ascending",
            ));
        }
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
        validate_histogram_bucket_total(
            &self.bucket_counts,
            self.count,
            io::ErrorKind::InvalidInput,
        )?;
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
        ensure_decoded_items_fit(
            buf,
            *cursor,
            schema.bucket_len,
            1,
            "histogram schema bucket counts",
        )?;
        let mut bucket_counts =
            try_decoded_vec(schema.bucket_len, "decoded histogram schema bucket counts")?;
        for _ in 0..schema.bucket_len {
            bucket_counts.push(decode_varint(buf, cursor)?);
        }
        let bucket_total = checked_count_total(
            bucket_counts.iter().copied(),
            io::ErrorKind::InvalidData,
            "histogram",
        )?;
        if bucket_total != count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "histogram bucket total must equal count",
            ));
        }
        let explicit_bounds =
            try_clone_decoded_slice(&schema.explicit_bounds, "decoded histogram sample bounds")?;
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
        validate_exponential_histogram_shape(
            self.zero_count,
            &self.positive.counts,
            &self.negative.counts,
            self.count,
            io::ErrorKind::InvalidInput,
        )?;
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
        ensure_decoded_items_fit(
            buf,
            *cursor,
            positive_len,
            1,
            "positive exponential histogram bucket counts",
        )?;
        let mut positive_counts = try_decoded_vec(
            positive_len,
            "decoded positive exponential histogram bucket counts",
        )?;
        for _ in 0..positive_len {
            positive_counts.push(decode_varint(buf, cursor)?);
        }
        let negative_offset = decode_i32(buf, cursor)?;
        let negative_len = decode_len(buf, cursor)?;
        ensure_decoded_items_fit(
            buf,
            *cursor,
            negative_len,
            1,
            "negative exponential histogram bucket counts",
        )?;
        let mut negative_counts = try_decoded_vec(
            negative_len,
            "decoded negative exponential histogram bucket counts",
        )?;
        for _ in 0..negative_len {
            negative_counts.push(decode_varint(buf, cursor)?);
        }
        validate_exponential_histogram_shape(
            zero_count,
            &positive_counts,
            &negative_counts,
            count,
            io::ErrorKind::InvalidData,
        )?;
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
        validate_summary_quantile_positions(
            self.quantiles.iter().map(|value| value.quantile),
            io::ErrorKind::InvalidInput,
        )?;
        encode_varint(self.quantiles.len() as u64, out);
        for quantile in &self.quantiles {
            encode_f64(quantile.quantile, out);
        }
        Ok(())
    }

    fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
        let len = decode_len(buf, cursor)?;
        ensure_decoded_items_fit(buf, *cursor, len, 8, "summary schema quantiles")?;
        let mut quantiles = try_decoded_vec(len, "decoded summary schema quantiles")?;
        for _ in 0..len {
            quantiles.push(decode_f64(buf, cursor)?);
        }
        validate_summary_quantile_positions(quantiles.iter().copied(), io::ErrorKind::InvalidData)?;
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
        ensure_decoded_items_fit(buf, *cursor, schema.quantiles.len(), 8, "summary values")?;
        let mut quantiles = try_decoded_vec(schema.quantiles.len(), "decoded summary values")?;
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

#[cfg(test)]
#[allow(
    clippy::items_after_test_module,
    reason = "codec safety tests stay adjacent to the private codec implementations they exercise"
)]
mod allocation_safety_tests {
    use super::*;

    fn minimal_typed_value_prefix() -> Vec<u8> {
        let mut bytes = Vec::new();
        encode_typed_metadata(TypedSampleMetadata::default(), &mut bytes);
        encode_varint(0, &mut bytes);
        encode_opt_f64(None, &mut bytes);
        encode_opt_f64(None, &mut bytes);
        encode_opt_f64(None, &mut bytes);
        bytes
    }

    #[test]
    fn histogram_schema_rejects_infeasible_u32_max_bounds_before_allocation() {
        let mut bytes = Vec::new();
        encode_varint(u64::from(u32::MAX), &mut bytes);
        let mut cursor = 0;
        let error = <HistogramValue as SchemaVarLenEncoding>::decode_schema(&bytes, &mut cursor)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("histogram schema bounds count is infeasible")
        );
    }

    #[test]
    fn histogram_value_rejects_infeasible_u32_max_buckets_before_allocation() {
        let schema = HistogramSchema {
            explicit_bounds: Vec::new(),
            bucket_len: u32::MAX as usize,
        };
        let bytes = minimal_typed_value_prefix();
        let mut cursor = 0;
        let error = <HistogramValue as SchemaVarLenEncoding>::decode_value_with_schema(
            &schema,
            &bytes,
            &mut cursor,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("bucket counts count is infeasible")
        );
    }

    #[test]
    fn exponential_histogram_rejects_infeasible_nested_counts_before_allocation() {
        let schema = ExponentialHistogramSchema {
            scale: 0,
            zero_threshold: 0.0,
        };

        let mut positive = minimal_typed_value_prefix();
        encode_varint(0, &mut positive);
        encode_varint(encode_zigzag_i64(0), &mut positive);
        encode_varint(u64::from(u32::MAX), &mut positive);
        let mut cursor = 0;
        let error = <ExponentialHistogramValue as SchemaVarLenEncoding>::decode_value_with_schema(
            &schema,
            &positive,
            &mut cursor,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("positive exponential histogram"));

        let mut negative = minimal_typed_value_prefix();
        encode_varint(0, &mut negative);
        encode_varint(encode_zigzag_i64(0), &mut negative);
        encode_varint(0, &mut negative);
        encode_varint(encode_zigzag_i64(0), &mut negative);
        encode_varint(u64::from(u32::MAX), &mut negative);
        let mut cursor = 0;
        let error = <ExponentialHistogramValue as SchemaVarLenEncoding>::decode_value_with_schema(
            &schema,
            &negative,
            &mut cursor,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("negative exponential histogram"));
    }

    #[test]
    fn summary_schema_rejects_infeasible_u32_max_quantiles_before_allocation() {
        let mut bytes = Vec::new();
        encode_varint(u64::from(u32::MAX), &mut bytes);
        let mut cursor = 0;
        let error =
            <SummaryValue as SchemaVarLenEncoding>::decode_schema(&bytes, &mut cursor).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(
            error
                .to_string()
                .contains("summary schema quantiles count is infeasible")
        );
    }

    #[test]
    fn typed_decoders_reject_invalid_shapes_and_overflowing_bucket_totals() {
        let mut histogram_schema = Vec::new();
        encode_varint(2, &mut histogram_schema);
        histogram_schema.extend_from_slice(&1.0f64.to_le_bytes());
        histogram_schema.extend_from_slice(&1.0f64.to_le_bytes());
        encode_varint(3, &mut histogram_schema);
        let mut cursor = 0;
        let error =
            <HistogramValue as SchemaVarLenEncoding>::decode_schema(&histogram_schema, &mut cursor)
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("finite and strictly ascending"));

        let schema = HistogramSchema {
            explicit_bounds: vec![0.0],
            bucket_len: 2,
        };
        let mut histogram_value = minimal_typed_value_prefix();
        encode_varint(u64::MAX, &mut histogram_value);
        encode_varint(1, &mut histogram_value);
        let mut cursor = 0;
        let error = <HistogramValue as SchemaVarLenEncoding>::decode_value_with_schema(
            &schema,
            &histogram_value,
            &mut cursor,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("overflows u64"));

        let schema = ExponentialHistogramSchema {
            scale: 0,
            zero_threshold: 0.0,
        };
        let mut exponential_value = minimal_typed_value_prefix();
        encode_varint(u64::MAX, &mut exponential_value);
        encode_varint(encode_zigzag_i64(0), &mut exponential_value);
        encode_varint(1, &mut exponential_value);
        encode_varint(1, &mut exponential_value);
        encode_varint(encode_zigzag_i64(0), &mut exponential_value);
        encode_varint(0, &mut exponential_value);
        let mut cursor = 0;
        let error = <ExponentialHistogramValue as SchemaVarLenEncoding>::decode_value_with_schema(
            &schema,
            &exponential_value,
            &mut cursor,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("overflows u64"));

        let mut summary_schema = Vec::new();
        encode_varint(2, &mut summary_schema);
        summary_schema.extend_from_slice(&0.5f64.to_le_bytes());
        summary_schema.extend_from_slice(&0.5f64.to_le_bytes());
        let mut cursor = 0;
        let error =
            <SummaryValue as SchemaVarLenEncoding>::decode_schema(&summary_schema, &mut cursor)
                .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("quantile positions"));
    }

    fn valid_histogram() -> HistogramValue {
        HistogramValue {
            count: 3,
            sum: None,
            min: None,
            max: None,
            metadata: TypedSampleMetadata::default(),
            explicit_bounds: vec![0.0],
            bucket_counts: vec![1, 2],
        }
    }

    fn valid_exponential_histogram() -> ExponentialHistogramValue {
        ExponentialHistogramValue {
            count: 3,
            sum: None,
            min: None,
            max: None,
            scale: 0,
            zero_threshold: 0.0,
            zero_count: 1,
            metadata: TypedSampleMetadata::default(),
            positive: ExponentialHistogramBuckets {
                offset: 0,
                counts: vec![2],
            },
            negative: ExponentialHistogramBuckets {
                offset: 0,
                counts: Vec::new(),
            },
        }
    }

    fn valid_summary() -> SummaryValue {
        SummaryValue {
            count: 2,
            sum: 3.0,
            metadata: TypedSampleMetadata::default(),
            quantiles: vec![
                SummaryQuantileValue {
                    quantile: 0.5,
                    value: 1.0,
                },
                SummaryQuantileValue {
                    quantile: 1.0,
                    value: 2.0,
                },
            ],
        }
    }

    fn assert_invalid_varlen<T: VarLenEncoding>(value: &T, expected: &str) {
        let error = value.encode_into(&mut Vec::new()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains(expected), "{error}");
    }

    #[test]
    fn histogram_encoders_reject_invalid_bounds_lengths_and_totals() {
        for bounds in [
            vec![f64::NAN],
            vec![f64::INFINITY],
            vec![1.0, 1.0],
            vec![2.0, 1.0],
        ] {
            let mut value = valid_histogram();
            value.explicit_bounds = bounds;
            value.bucket_counts = vec![0; value.explicit_bounds.len() + 1];
            value.count = 0;
            assert_invalid_varlen(&value, "finite and strictly ascending");
            let error = value.encode_schema_from_value(&mut Vec::new()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }

        let mut wrong_len = valid_histogram();
        wrong_len.bucket_counts.pop();
        assert_invalid_varlen(&wrong_len, "bucket length");

        let mut wrong_total = valid_histogram();
        wrong_total.count = 4;
        assert_invalid_varlen(&wrong_total, "bucket total must equal count");

        let mut overflow = valid_histogram();
        overflow.count = 0;
        overflow.bucket_counts = vec![u64::MAX, 1];
        assert_invalid_varlen(&overflow, "overflows u64");
    }

    #[test]
    fn exponential_histogram_encoders_reject_mismatched_and_overflowing_totals() {
        let mut mismatch = valid_exponential_histogram();
        mismatch.count = 4;
        assert_invalid_varlen(&mismatch, "bucket total must equal count");

        let mut overflow = valid_exponential_histogram();
        overflow.count = 0;
        overflow.zero_count = u64::MAX;
        overflow.positive.counts = vec![1];
        assert_invalid_varlen(&overflow, "overflows u64");
    }

    #[test]
    fn summary_encoders_reject_invalid_quantile_positions() {
        for positions in [
            vec![f64::NAN],
            vec![-0.1],
            vec![1.1],
            vec![0.5, 0.5],
            vec![0.75, 0.25],
        ] {
            let mut value = valid_summary();
            value.quantiles = positions
                .into_iter()
                .map(|quantile| SummaryQuantileValue {
                    quantile,
                    value: 0.0,
                })
                .collect();
            assert_invalid_varlen(&value, "quantile positions");
            let error = value.encode_schema_from_value(&mut Vec::new()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        }
    }
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
