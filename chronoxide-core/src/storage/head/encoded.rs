use super::*;

pub(super) enum SeriesEncoding {
    Float(FloatEncoding),
    Int(IntEncoding),
    Histogram(VarLenEncodingKind),
    ExponentialHistogram(VarLenEncodingKind),
    Summary(VarLenEncodingKind),
}

pub(super) type HistogramRawCodec = VarLenCodec<HistogramValue>;
pub(super) type HistogramSchemaCodec = SchemaVarLenCodec<HistogramValue>;
pub(super) type ExponentialHistogramRawCodec = VarLenCodec<ExponentialHistogramValue>;
pub(super) type ExponentialHistogramSchemaCodec = SchemaVarLenCodec<ExponentialHistogramValue>;
pub(super) type SummaryRawCodec = VarLenCodec<SummaryValue>;
pub(super) type SummarySchemaCodec = SchemaVarLenCodec<SummaryValue>;

pub(super) const INLINE_NUMERIC_SERIES_CAPACITY: usize = 4;

/// Exact staging for the common short numeric-series case.
///
/// Values remain as their original bits until the series either drains or is
/// promoted to the configured block codec. This avoids allocating a block
/// builder and its timestamp/value buffers for series that never exceed the
/// inline capacity.
#[derive(Debug)]
pub(super) struct InlineNumericSeries {
    timestamps: [u64; INLINE_NUMERIC_SERIES_CAPACITY],
    values: [u64; INLINE_NUMERIC_SERIES_CAPACITY],
    block_size: usize,
    len: u8,
}

impl InlineNumericSeries {
    fn new(block_size: usize) -> Self {
        Self {
            timestamps: [0; INLINE_NUMERIC_SERIES_CAPACITY],
            values: [0; INLINE_NUMERIC_SERIES_CAPACITY],
            block_size,
            len: 0,
        }
    }

    fn push(&mut self, timestamp_ms: u64, value_bits: u64) -> bool {
        let index = usize::from(self.len);
        if index >= INLINE_NUMERIC_SERIES_CAPACITY {
            return false;
        }
        self.timestamps[index] = timestamp_ms;
        self.values[index] = value_bits;
        self.len = self.len.saturating_add(1);
        true
    }

    fn is_full(&self) -> bool {
        usize::from(self.len) == INLINE_NUMERIC_SERIES_CAPACITY
    }

    fn sample_count(&self) -> u64 {
        u64::from(self.len)
    }

    fn block_count(&self) -> usize {
        usize::from(self.len).div_ceil(self.block_size)
    }

    fn for_each_block_sample<F>(&self, f: &mut F)
    where
        F: FnMut(u64),
    {
        let mut remaining = usize::from(self.len);
        while remaining > 0 {
            let samples = remaining.min(self.block_size);
            f(samples as u64);
            remaining -= samples;
        }
    }

    fn estimated_bytes(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn payload_bytes(&self) -> usize {
        usize::from(self.len).saturating_mul(std::mem::size_of::<(u64, u64)>())
    }

    fn seal_current(&mut self, _arena: &mut BlockArena) {}

    fn codec_name(&self) -> &'static str {
        "inline_numeric"
    }

    fn all_samples<T, F>(&self, mut decode: F) -> Vec<(u64, T)>
    where
        F: FnMut(u64) -> T,
    {
        let mut samples = Vec::with_capacity(usize::from(self.len));
        for index in 0..usize::from(self.len) {
            samples.push((self.timestamps[index], decode(self.values[index])));
        }
        samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
        samples
    }

    fn samples<T, F>(&self, start_ms: u64, end_ms: u64, mut decode: F) -> Vec<(u64, T)>
    where
        F: FnMut(u64) -> T,
    {
        let mut samples = Vec::with_capacity(usize::from(self.len));
        for index in 0..usize::from(self.len) {
            let timestamp_ms = self.timestamps[index];
            if timestamp_ms >= start_ms && timestamp_ms < end_ms {
                samples.push((timestamp_ms, decode(self.values[index])));
            }
        }
        samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
        samples
    }
}

#[derive(Debug)]
pub(super) enum EncodedSeries {
    InlineFloatGorilla(InlineNumericSeries),
    InlineIntDelta(InlineNumericSeries),
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

macro_rules! with_encoded_series {
    ($encoded:expr, |$series:ident| $body:expr) => {
        match $encoded {
            EncodedSeries::InlineFloatGorilla($series) => $body,
            EncodedSeries::InlineIntDelta($series) => $body,
            EncodedSeries::FloatRaw($series) => $body,
            EncodedSeries::IntRaw($series) => $body,
            EncodedSeries::FloatGorilla($series) => $body,
            EncodedSeries::FloatElf($series) => $body,
            EncodedSeries::FloatAlp($series) => $body,
            EncodedSeries::FloatAlpRd($series) => $body,
            EncodedSeries::FloatAlpSpiral($series) => $body,
            EncodedSeries::FloatAlpRdSpiral($series) => $body,
            EncodedSeries::FloatChimp128DuckDB($series) => $body,
            EncodedSeries::FloatChimp128Baseline($series) => $body,
            EncodedSeries::IntDelta($series) => $body,
            EncodedSeries::Histogram($series) => $body,
            EncodedSeries::HistogramSchema($series) => $body,
            EncodedSeries::ExponentialHistogram($series) => $body,
            EncodedSeries::ExponentialHistogramSchema($series) => $body,
            EncodedSeries::Summary($series) => $body,
            EncodedSeries::SummarySchema($series) => $body,
        }
    };
}

impl EncodedSeries {
    pub(super) fn new(
        encoding: SeriesEncoding,
        compact_numeric_series: bool,
        block_size: usize,
    ) -> Self {
        if compact_numeric_series {
            match encoding {
                SeriesEncoding::Float(FloatEncoding::Gorilla) => {
                    return Self::InlineFloatGorilla(InlineNumericSeries::new(block_size));
                }
                SeriesEncoding::Int(IntEncoding::DeltaZigZag) => {
                    return Self::InlineIntDelta(InlineNumericSeries::new(block_size));
                }
                _ => {}
            }
        }
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

    pub(super) fn kind(&self) -> SampleKind {
        match self {
            Self::InlineFloatGorilla(_)
            | Self::FloatGorilla(_)
            | Self::FloatElf(_)
            | Self::FloatAlp(_)
            | Self::FloatAlpRd(_)
            | Self::FloatAlpSpiral(_)
            | Self::FloatAlpRdSpiral(_)
            | Self::FloatChimp128DuckDB(_)
            | Self::FloatChimp128Baseline(_)
            | Self::FloatRaw(_) => SampleKind::Float,
            Self::InlineIntDelta(_) | Self::IntDelta(_) | Self::IntRaw(_) => SampleKind::Int64,
            Self::Histogram(_) | Self::HistogramSchema(_) => SampleKind::Histogram,
            Self::ExponentialHistogram(_) | Self::ExponentialHistogramSchema(_) => {
                SampleKind::ExponentialHistogram
            }
            Self::Summary(_) | Self::SummarySchema(_) => SampleKind::Summary,
        }
    }

    pub(super) fn codec_name(&self) -> &'static str {
        match self {
            Self::InlineFloatGorilla(_) => std::any::type_name::<FloatGorillaCodec>(),
            Self::InlineIntDelta(_) => std::any::type_name::<IntDeltaCodec>(),
            _ => with_encoded_series!(self, |series| series.codec_name()),
        }
    }

    pub(super) fn sample_count(&self) -> u64 {
        with_encoded_series!(self, |series| series.sample_count())
    }

    pub(super) fn block_count(&self) -> usize {
        with_encoded_series!(self, |series| series.block_count())
    }

    pub(super) fn for_each_block_sample<F>(&self, f: &mut F)
    where
        F: FnMut(u64),
    {
        with_encoded_series!(self, |series| series.for_each_block_sample(f))
    }

    pub(super) fn estimated_bytes(&self) -> usize {
        with_encoded_series!(self, |series| series.estimated_bytes())
    }

    pub(super) fn payload_bytes(&self) -> usize {
        with_encoded_series!(self, |series| series.payload_bytes())
    }

    pub(super) fn seal(&mut self, arena: &mut BlockArena) {
        with_encoded_series!(self, |series| series.seal_current(arena))
    }

    #[cfg(test)]
    pub(super) fn is_inline_numeric(&self) -> bool {
        matches!(self, Self::InlineFloatGorilla(_) | Self::InlineIntDelta(_))
    }

    pub(super) fn push_sample(
        &mut self,
        series: SeriesRef,
        base_ms: u64,
        timestamp_ms: u64,
        value: SampleValue,
        block_size: usize,
        arena: &mut BlockArena,
    ) -> io::Result<()> {
        match self {
            Self::InlineFloatGorilla(short) => match value {
                SampleValue::Float(value) => {
                    if timestamp_ms < base_ms {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "timestamp precedes window start",
                        ));
                    }
                    if !short.is_full() {
                        let inserted = short.push(timestamp_ms, value.to_bits());
                        debug_assert!(inserted, "non-full inline float series rejected a sample");
                        return Ok(());
                    }

                    let mut promoted = Series::<FloatGorillaCodec>::new();
                    for index in 0..usize::from(short.len) {
                        promoted.push_sample(
                            series,
                            "float",
                            base_ms,
                            short.timestamps[index],
                            f64::from_bits(short.values[index]),
                            block_size,
                            arena,
                        )?;
                    }
                    promoted.push_sample(
                        series,
                        "float",
                        base_ms,
                        timestamp_ms,
                        value,
                        block_size,
                        arena,
                    )?;
                    *self = Self::FloatGorilla(promoted);
                    Ok(())
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "float series received non-float sample",
                )),
            },
            Self::InlineIntDelta(short) => match value {
                SampleValue::Int64(value) => {
                    if timestamp_ms < base_ms {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "timestamp precedes window start",
                        ));
                    }
                    if !short.is_full() {
                        let value_bits = u64::from_le_bytes(value.to_le_bytes());
                        let inserted = short.push(timestamp_ms, value_bits);
                        debug_assert!(inserted, "non-full inline int series rejected a sample");
                        return Ok(());
                    }

                    let mut promoted = Series::<IntDeltaCodec>::new();
                    for index in 0..usize::from(short.len) {
                        promoted.push_sample(
                            series,
                            "int64",
                            base_ms,
                            short.timestamps[index],
                            i64::from_le_bytes(short.values[index].to_le_bytes()),
                            block_size,
                            arena,
                        )?;
                    }
                    promoted.push_sample(
                        series,
                        "int64",
                        base_ms,
                        timestamp_ms,
                        value,
                        block_size,
                        arena,
                    )?;
                    *self = Self::IntDelta(promoted);
                    Ok(())
                }
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "int series received non-int sample",
                )),
            },
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

    pub(super) fn into_samples(self, arena: &BlockArena) -> io::Result<SeriesSamples> {
        match self {
            Self::InlineFloatGorilla(short) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: short.all_samples(f64::from_bits),
            }),
            Self::InlineIntDelta(short) => Ok(SeriesSamples::Int64 {
                encoding: IntEncoding::DeltaZigZag,
                samples: short.all_samples(|bits| i64::from_le_bytes(bits.to_le_bytes())),
            }),
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

    pub(super) fn samples_in_range(
        &self,
        arena: &BlockArena,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<SeriesSamples> {
        match self {
            Self::InlineFloatGorilla(short) => Ok(SeriesSamples::Float {
                encoding: FloatEncoding::Gorilla,
                samples: short.samples(start_ms, end_ms, f64::from_bits),
            }),
            Self::InlineIntDelta(short) => Ok(SeriesSamples::Int64 {
                encoding: IntEncoding::DeltaZigZag,
                samples: short.samples(start_ms, end_ms, |bits| {
                    i64::from_le_bytes(bits.to_le_bytes())
                }),
            }),
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

pub(super) fn encode_f64(value: f64, out: &mut Vec<u8>) {
    out.extend_from_slice(&value.to_le_bytes());
}

pub(super) fn decode_f64(buf: &[u8], cursor: &mut usize) -> io::Result<f64> {
    if cursor.saturating_add(8) > buf.len() {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "short f64"));
    }
    let value = f64::from_le_bytes(buf[*cursor..*cursor + 8].try_into().unwrap());
    *cursor += 8;
    Ok(value)
}

pub(super) fn encode_opt_f64(value: Option<f64>, out: &mut Vec<u8>) {
    match value {
        Some(value) => {
            out.push(1);
            encode_f64(value, out);
        }
        None => out.push(0),
    }
}

pub(crate) fn decode_opt_f64(buf: &[u8], cursor: &mut usize) -> io::Result<Option<f64>> {
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

pub(super) fn encode_typed_metadata(metadata: TypedSampleMetadata, out: &mut Vec<u8>) {
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

pub(crate) fn decode_typed_metadata(
    buf: &[u8],
    cursor: &mut usize,
) -> io::Result<TypedSampleMetadata> {
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

pub(super) fn decode_temporality(value: u64) -> io::Result<OtlpAggregationTemporality> {
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

pub(super) fn decode_counter_reset_hint(value: u64) -> io::Result<CounterResetHint> {
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

pub(super) fn decode_len(buf: &[u8], cursor: &mut usize) -> io::Result<usize> {
    let len = decode_varint(buf, cursor)?;
    usize::try_from(len).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "length overflow"))
}

pub(super) fn decode_i32(buf: &[u8], cursor: &mut usize) -> io::Result<i32> {
    let encoded = decode_varint(buf, cursor)?;
    let decoded = decode_zigzag_i64(encoded);
    i32::try_from(decoded).map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "i32 overflow"))
}

pub(super) fn encode_buckets(buckets: &ExponentialHistogramBuckets, out: &mut Vec<u8>) {
    encode_varint(encode_zigzag_i64(buckets.offset as i64), out);
    encode_varint(buckets.counts.len() as u64, out);
    for count in &buckets.counts {
        encode_varint(*count, out);
    }
}

pub(super) fn decode_buckets(
    buf: &[u8],
    cursor: &mut usize,
) -> io::Result<ExponentialHistogramBuckets> {
    let offset = decode_i32(buf, cursor)?;
    let len = decode_len(buf, cursor)?;
    let mut counts = Vec::with_capacity(len);
    for _ in 0..len {
        counts.push(decode_varint(buf, cursor)?);
    }
    Ok(ExponentialHistogramBuckets { offset, counts })
}

pub(super) fn ensure_consumed(buf: &[u8], cursor: usize) -> io::Result<()> {
    if cursor != buf.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "value buffer has trailing bytes",
        ));
    }
    Ok(())
}

#[derive(Debug)]
pub(super) struct Series<C: BlockCodec> {
    pub(super) blocks: SmallVec<[Block<C>; 1]>,
    pub(super) current: Option<Box<BlockBuilder<C>>>,
    pub(super) samples: u64,
}

impl<C: BlockCodec> Series<C> {
    pub(super) fn new() -> Self {
        Self {
            blocks: SmallVec::new(),
            current: None,
            samples: 0,
        }
    }

    #[expect(
        clippy::too_many_arguments,
        reason = "encoding one sample needs explicit series diagnostics, timing, block sizing, and arena state"
    )]
    pub(super) fn push_sample(
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

    pub(super) fn seal_current(&mut self, arena: &mut BlockArena) {
        if let Some(block) = self.current.take() {
            self.blocks.push(block.seal(arena));
        }
    }

    pub(super) fn into_samples(self, arena: &BlockArena) -> io::Result<Vec<(u64, C::Value)>> {
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

    pub(super) fn samples_in_range(
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

    pub(super) fn codec_name(&self) -> &'static str {
        std::any::type_name::<C>()
    }

    pub(super) fn sample_count(&self) -> u64 {
        self.samples
    }

    pub(super) fn block_count(&self) -> usize {
        self.blocks
            .len()
            .saturating_add(self.current.as_ref().map_or(0, |_| 1))
    }

    pub(super) fn for_each_block_sample<F>(&self, f: &mut F)
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

    pub(super) fn estimated_bytes(&self) -> usize {
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

    pub(super) fn payload_bytes(&self) -> usize {
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

pub(super) fn window_for(timestamp_ms: u64, duration_ms: u64) -> (u64, u64) {
    let start_ms = timestamp_ms.saturating_sub(timestamp_ms % duration_ms);
    (start_ms, start_ms.saturating_add(duration_ms))
}
