use super::*;

pub(super) fn ingest_number_datapoints<'a>(
    processor: &mut OtlpLabelSetProcessor,
    mut head_state: Option<&mut PartitionHead>,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    points: &'a [tonic::metrics::v1::NumberDataPoint],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
    captured_at_ms: i64,
) -> Result<DatapointIngestResult> {
    let mut result = DatapointIngestResult::default();
    for dp in points {
        let decision = processor.evaluate_datapoint_time(dp.time_unix_nano, captured_at_ms);
        let Some(ts_ms) = result.record(decision) else {
            continue;
        };
        let value = number_value(dp);
        if value.is_none() {
            processor.labelset_stats.record_missing_number_values(1);
        }
        let series = intern_labelset(
            &mut processor.labelsets,
            &mut processor.labelset_stats,
            resource_attrs,
            metric_name,
            &dp.attributes,
            scratch_values,
            tmp_labels,
        )?;
        if let (Some(series), Some(value)) = (series, value) {
            if let Some(head_state) = head_state.as_deref_mut() {
                processor.record_head_sample(head_state, series, ts_ms, value)?;
            }
        }
    }
    Ok(result)
}

struct ProcessorLabelSetInterner<'a> {
    labelsets: &'a mut LabelSetInterner,
    stats: &'a mut OtlpMetricsIngestionStats,
}

impl<'a> OtlpLabelSetInterner for ProcessorLabelSetInterner<'a> {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {
        self.stats.record_skipped_non_scalar_value();
    }

    fn on_intern_error(&mut self, error: Self::Error) {
        self.stats.record_labelset_error();
        if should_log(Level::ERROR, "LabelSetStoreInternError", Instant::now()) {
            error!("LabelSetStore intern failed: {}", error);
        }
    }

    fn intern(
        &mut self,
        labels: &[KeyValueRef<'_>],
    ) -> std::result::Result<SeriesRef, Self::Error> {
        self.labelsets.intern(labels, self.stats)
    }
}

pub(super) fn intern_labelset<'a>(
    labelsets: &mut LabelSetInterner,
    stats: &mut OtlpMetricsIngestionStats,
    resource_attrs: &'a [tonic::common::v1::KeyValue],
    metric_name: &'a str,
    datapoint_attrs: &'a [tonic::common::v1::KeyValue],
    scratch_values: &mut Vec<Box<str>>,
    tmp_labels: &mut Vec<TmpLabel<'a>>,
) -> Result<Option<SeriesRef>> {
    let mut interner = ProcessorLabelSetInterner { labelsets, stats };
    Ok(intern_otlp_labelset(
        &mut interner,
        resource_attrs,
        metric_name,
        datapoint_attrs,
        scratch_values,
        tmp_labels,
    ))
}

#[derive(Default)]
pub(super) struct LabelSetStoreStats {
    pub(super) series: usize,
    pub(super) symbols: Option<usize>,
    pub(super) keysets: Option<usize>,
    pub(super) alloc_bytes: usize,
    pub(super) used_bytes: usize,
    pub(super) symbols_alloc_bytes: usize,
    pub(super) symbols_used_bytes: usize,
    pub(super) buffer_stats: Option<String>,
    pub(super) symbol_table_stats: Option<String>,
}

pub(super) enum LabelSetInterner {
    Naive(NaiveLabelSetStore),
    FlatInterned(InternedStore),
    KeySetDictEncoded(KeysetStore),
}

impl LabelSetInterner {
    pub(super) fn new(kind: LabelSetStoreKind) -> Self {
        match kind {
            LabelSetStoreKind::FlatInterned => Self::FlatInterned(InternedStore::default()),
            LabelSetStoreKind::KeySetDictEncoded => Self::KeySetDictEncoded(KeysetStore::default()),
            LabelSetStoreKind::Naive => Self::Naive(NaiveLabelSetStore::default()),
        }
    }

    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::FlatInterned(_) => "FlatInterned",
            Self::KeySetDictEncoded(_) => "KeySetDictEncoded",
            Self::Naive(_) => "Naive",
        }
    }

    pub(super) fn as_flat_interned(&self) -> Option<&InternedStore> {
        match self {
            Self::FlatInterned(store) => Some(store),
            Self::Naive(_) | Self::KeySetDictEncoded(_) => None,
        }
    }

    pub(super) fn intern(
        &mut self,
        labels: &[KeyValueRef<'_>],
        stats: &mut OtlpMetricsIngestionStats,
    ) -> std::result::Result<SeriesRef, LabelSetStoreError> {
        match self {
            Self::Naive(store) => {
                let start = Instant::now();
                let series = store.intern(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::Naive, elapsed);
                Ok(series)
            }
            Self::FlatInterned(store) => {
                let start = Instant::now();
                let series = store.intern(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::FlatInterned, elapsed);
                Ok(series)
            }
            Self::KeySetDictEncoded(store) => {
                let start = Instant::now();
                let series = store.intern(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::KeySetDictEncoded, elapsed);
                Ok(series)
            }
        }
    }

    pub(super) fn segment_metadata(&self, series: SeriesRef) -> SegmentSeriesMetadata {
        let mut builder = SegmentSeriesMetadataBuilder::new();
        match self {
            Self::Naive(store) => {
                store.visit_labelset(series, |key, value| {
                    builder.push_label(key, value);
                });
            }
            Self::FlatInterned(store) => {
                store.visit_labelset(series, |key, value| {
                    builder.push_label(key, value);
                });
            }
            Self::KeySetDictEncoded(store) => {
                store.visit_labelset(series, |key, value| {
                    builder.push_label(key, value);
                });
            }
        }
        builder.finish()
    }

    pub(super) fn visit_labelset(&self, series: SeriesRef, mut visitor: impl FnMut(&str, &str)) {
        match self {
            Self::Naive(store) => {
                store.visit_labelset(series, |key, value| visitor(key, value));
            }
            Self::FlatInterned(store) => {
                store.visit_labelset(series, |key, value| visitor(key, value));
            }
            Self::KeySetDictEncoded(store) => {
                store.visit_labelset(series, |key, value| visitor(key, value));
            }
        }
    }

    pub(super) fn stats(&self) -> LabelSetStoreStats {
        match self {
            Self::Naive(store) => LabelSetStoreStats {
                series: store.len(),
                symbols: None,
                keysets: None,
                alloc_bytes: store.estimate_size_bytes(),
                used_bytes: store.estimate_used_bytes(),
                symbols_alloc_bytes: 0,
                symbols_used_bytes: 0,
                buffer_stats: Some(store.buffer_stats().to_string()),
                symbol_table_stats: None,
            },
            Self::FlatInterned(store) => {
                let symbols = store.symbols();
                LabelSetStoreStats {
                    series: store.len(),
                    symbols: Some(symbols.len()),
                    keysets: None,
                    alloc_bytes: store.estimate_size_bytes(),
                    used_bytes: store.estimate_used_bytes(),
                    symbols_alloc_bytes: symbols.estimate_allocated_bytes(),
                    symbols_used_bytes: symbols.estimate_used_bytes(),
                    buffer_stats: Some(store.buffer_stats().to_string()),
                    symbol_table_stats: Some(symbols.stats().to_string()),
                }
            }
            Self::KeySetDictEncoded(store) => {
                let symbols = store.symbols();
                LabelSetStoreStats {
                    series: store.len(),
                    symbols: Some(symbols.len()),
                    keysets: Some(store.keysets().len()),
                    alloc_bytes: store.estimate_size_bytes(),
                    used_bytes: store.estimate_used_bytes(),
                    symbols_alloc_bytes: symbols.estimate_allocated_bytes(),
                    symbols_used_bytes: symbols.estimate_used_bytes(),
                    buffer_stats: Some(store.buffer_stats().to_string()),
                    symbol_table_stats: Some(symbols.stats().to_string()),
                }
            }
        }
    }
}
