use std::collections::HashMap;
use std::io;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use smallvec::SmallVec;
use tracing::{info, warn};

use crate::labels::SeriesRef;
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

#[derive(Debug, Clone)]
pub struct HeadConfig {
    pub window_duration: Duration,
    pub block_size: usize,
    pub float_encoding: FloatEncoding,
    pub int_encoding: IntEncoding,
    pub varlen_encoding: VarLenEncodingKind,
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
        }
    }

    pub fn with_varlen_encoding(mut self, varlen_encoding: VarLenEncodingKind) -> Self {
        self.varlen_encoding = varlen_encoding;
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

#[derive(Debug, Clone, PartialEq)]
pub struct HistogramValue {
    pub count: u64,
    pub sum: Option<f64>,
    pub min: Option<f64>,
    pub max: Option<f64>,
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
    pub zero_count: u64,
    pub positive: ExponentialHistogramBuckets,
    pub negative: ExponentialHistogramBuckets,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ExponentialHistogramBuckets {
    pub offset: i32,
    pub counts: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryValue {
    pub count: u64,
    pub sum: f64,
    pub quantiles: Vec<SummaryQuantileValue>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SummaryQuantileValue {
    pub quantile: f64,
    pub value: f64,
}

impl VarLenEncoding for HistogramValue {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
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
            explicit_bounds,
            bucket_counts,
        })
    }
}

impl VarLenEncoding for ExponentialHistogramValue {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_varint(self.count, out);
        encode_opt_f64(self.sum, out);
        encode_opt_f64(self.min, out);
        encode_opt_f64(self.max, out);
        encode_varint(encode_zigzag_i64(self.scale as i64), out);
        encode_varint(self.zero_count, out);
        encode_buckets(&self.positive, out);
        encode_buckets(&self.negative, out);
        Ok(())
    }

    fn decode_from(buf: &[u8]) -> io::Result<Self> {
        let mut cursor = 0usize;
        let count = decode_varint(buf, &mut cursor)?;
        let sum = decode_opt_f64(buf, &mut cursor)?;
        let min = decode_opt_f64(buf, &mut cursor)?;
        let max = decode_opt_f64(buf, &mut cursor)?;
        let scale = decode_i32(buf, &mut cursor)?;
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
            zero_count,
            positive,
            negative,
        })
    }
}

impl VarLenEncoding for SummaryValue {
    fn encode_into(&self, out: &mut Vec<u8>) -> io::Result<()> {
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
    positive_len: usize,
    negative_len: usize,
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
            explicit_bounds: schema.explicit_bounds.clone(),
            bucket_counts,
        })
    }
}

impl SchemaVarLenEncoding for ExponentialHistogramValue {
    type Schema = ExponentialHistogramSchema;

    fn encode_schema_from_value(&self, out: &mut Vec<u8>) -> io::Result<()> {
        encode_varint(encode_zigzag_i64(self.scale as i64), out);
        encode_varint(self.positive.counts.len() as u64, out);
        encode_varint(self.negative.counts.len() as u64, out);
        Ok(())
    }

    fn decode_schema(buf: &[u8], cursor: &mut usize) -> io::Result<Self::Schema> {
        let scale = decode_i32(buf, cursor)?;
        let positive_len = decode_len(buf, cursor)?;
        let negative_len = decode_len(buf, cursor)?;
        Ok(Self::Schema {
            scale,
            positive_len,
            negative_len,
        })
    }

    fn encode_value_with_schema(&self, schema: &Self::Schema, out: &mut Vec<u8>) -> io::Result<()> {
        if self.positive.counts.len() != schema.positive_len
            || self.negative.counts.len() != schema.negative_len
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "exponential histogram bucket length mismatch",
            ));
        }
        encode_varint(self.count, out);
        encode_opt_f64(self.sum, out);
        encode_opt_f64(self.min, out);
        encode_opt_f64(self.max, out);
        encode_varint(self.zero_count, out);
        encode_varint(encode_zigzag_i64(self.positive.offset as i64), out);
        for count in &self.positive.counts {
            encode_varint(*count, out);
        }
        encode_varint(encode_zigzag_i64(self.negative.offset as i64), out);
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
        let count = decode_varint(buf, cursor)?;
        let sum = decode_opt_f64(buf, cursor)?;
        let min = decode_opt_f64(buf, cursor)?;
        let max = decode_opt_f64(buf, cursor)?;
        let zero_count = decode_varint(buf, cursor)?;
        let positive_offset = decode_i32(buf, cursor)?;
        let mut positive_counts = Vec::with_capacity(schema.positive_len);
        for _ in 0..schema.positive_len {
            positive_counts.push(decode_varint(buf, cursor)?);
        }
        let negative_offset = decode_i32(buf, cursor)?;
        let mut negative_counts = Vec::with_capacity(schema.negative_len);
        for _ in 0..schema.negative_len {
            negative_counts.push(decode_varint(buf, cursor)?);
        }
        Ok(Self {
            count,
            sum,
            min,
            max,
            scale: schema.scale,
            zero_count,
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
    pub fn into_series_samples(self) -> io::Result<Vec<(SeriesRef, SeriesSamples)>> {
        let mut window = self;
        window.seal_all_series();
        let HeadWindow { series, arena, .. } = window;
        let mut decoded = Vec::with_capacity(series.len());
        for (series, encoded) in series {
            let series_estimated_bytes = encoded.estimated_bytes();
            if series_estimated_bytes > 1000 {
                info!(
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

pub struct HeadBuffer {
    config: HeadConfig,
    window: Option<HeadWindow>,
}

impl HeadBuffer {
    pub fn new(config: HeadConfig) -> io::Result<Self> {
        let _ = Self::window_duration_ms(&config)?;
        Self::validate_block_size(&config)?;
        Ok(Self {
            config,
            window: None,
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
            let (start_ms, end_ms) = window_for(*ts, duration_ms);
            let rotate = match &self.window {
                None => true,
                Some(window) => *ts < window.start_ms || *ts >= window.end_ms,
            };

            if rotate {
                if let Some(mut window) = self.window.take() {
                    window.seal_all_series();
                    flushed.push(window);
                }
                self.window = Some(HeadWindow {
                    start_ms,
                    end_ms,
                    series: HashMap::new(),
                    datapoints: 0,
                    arena: BlockArena::new(DEFAULT_HEAD_ARENA_PAGE_BYTES),
                });
            }

            if let Some(window) = self.window.as_mut() {
                let base_ms = window.start_ms;
                let block_size = self.config.block_size;
                let encoding = match value.kind() {
                    SampleKind::Float => SeriesEncoding::Float(self.config.float_encoding),
                    SampleKind::Int64 => SeriesEncoding::Int(self.config.int_encoding),
                    SampleKind::Histogram => SeriesEncoding::Histogram(self.config.varlen_encoding),
                    SampleKind::ExponentialHistogram => {
                        SeriesEncoding::ExponentialHistogram(self.config.varlen_encoding)
                    }
                    SampleKind::Summary => SeriesEncoding::Summary(self.config.varlen_encoding),
                };
                match window.series.entry(series) {
                    std::collections::hash_map::Entry::Vacant(entry) => {
                        let mut encoded = EncodedSeries::new(encoding);
                        encoded.push_sample(
                            series,
                            base_ms,
                            *ts,
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
                            continue;
                        }
                        entry.get_mut().push_sample(
                            series,
                            base_ms,
                            *ts,
                            value.clone(),
                            block_size,
                            &mut window.arena,
                        )?;
                    }
                }
                window.datapoints = window.datapoints.saturating_add(1);
            }
        }

        Ok(flushed)
    }

    pub fn drain(&mut self) -> Option<HeadWindow> {
        if let Some(mut window) = self.window.take() {
            window.seal_all_series();
            Some(window)
        } else {
            None
        }
    }

    pub fn window_range(&self) -> Option<(u64, u64)> {
        self.window.as_ref().map(|w| (w.start_ms, w.end_ms))
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
                info!(
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
    use crate::storage::arena::BlockArena;
    use crate::storage::block::{
        BlockBuilder, BlockCodec, FloatChimp128DuckDBDeferredCodec, FloatGorillaCodec,
        FloatRawCodec, IntDeltaCodec, IntRawCodec,
    };
    use crate::storage::encoding::chimp::Chimp128DuckDBEncoder;
    use crate::storage::encoding::{GorillaEncoder, encode_varint, encode_zigzag_i64};

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
            zero_count: 3,
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
            zero_count: 3,
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
