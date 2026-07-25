use super::*;

pub(super) fn ingest_number_datapoints<'plan, 'input>(
    processor: &mut OtlpLabelSetProcessor,
    mut head_state: Option<&mut PartitionHead>,
    metric_labels: &PreparedOtlpMetricLabels<'plan, 'input>,
    points: &'input [tonic::metrics::v1::NumberDataPoint],
    label_scratch: &mut PreparedOtlpLabelSetScratch<'input>,
    captured_at_ms: i64,
) -> Result<DatapointIngestResult> {
    let mut result = DatapointIngestResult::default();
    for dp in points {
        let order = processor.next_sample_order()?;
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
            metric_labels,
            &dp.attributes,
            label_scratch,
        )?;
        if let (Some(series), Some(value)) = (series, value)
            && let Some(head_state) = head_state.as_deref_mut()
        {
            processor.record_head_sample(head_state, series, ts_ms, value, order)?;
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
        labels: CanonicalLabelSet<'_, '_>,
    ) -> std::result::Result<SeriesRef, Self::Error> {
        self.labelsets.intern_canonical(labels, self.stats)
    }
}

pub(super) fn intern_labelset<'plan, 'input>(
    labelsets: &mut LabelSetInterner,
    stats: &mut OtlpMetricsIngestionStats,
    metric_labels: &PreparedOtlpMetricLabels<'plan, 'input>,
    datapoint_attrs: &'input [tonic::common::v1::KeyValue],
    label_scratch: &mut PreparedOtlpLabelSetScratch<'input>,
) -> Result<Option<SeriesRef>> {
    let mut interner = ProcessorLabelSetInterner { labelsets, stats };
    Ok(intern_prepared_otlp_labelset(
        &mut interner,
        metric_labels,
        datapoint_attrs,
        label_scratch,
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

// There is exactly one interner per ingestion processor, not one per series.
// Keeping the selected store inline avoids adding a pointer chase to every
// live and disabled-mode label lookup merely to save a few hundred stack bytes.
#[allow(clippy::large_enum_variant)]
pub(super) enum LabelSetInterner {
    Naive(NaiveLabelSetStore),
    FlatInterned(InternedStore),
    VersionedFlatInterned(LiveInternedStore),
    KeySetDictEncoded(KeysetStore),
}

impl LabelSetInterner {
    pub(super) fn new(kind: LabelSetStoreKind) -> Self {
        match kind {
            LabelSetStoreKind::FlatInterned => {
                Self::FlatInterned(InternedStore::with_interned_id_labelset_hash())
            }
            LabelSetStoreKind::ExperimentalFlatInternedPaged => {
                Self::FlatInterned(InternedStore::with_paged_key_values())
            }
            LabelSetStoreKind::ExperimentalFlatInternedCanonicalStringHash => {
                Self::FlatInterned(InternedStore::with_canonical_string_labelset_hash())
            }
            LabelSetStoreKind::ExperimentalFlatInternedSipHash => {
                Self::FlatInterned(InternedStore::with_interned_id_siphash_labelset_hash())
            }
            LabelSetStoreKind::ExperimentalFlatInternedSipHashSymbols => {
                Self::FlatInterned(InternedStore::with_interned_id_labelset_hash_and_symbols(
                    DefaultSymbolTable::with_siphash_symbol_hash(),
                ))
            }
            LabelSetStoreKind::KeySetDictEncoded => Self::KeySetDictEncoded(KeysetStore::default()),
            LabelSetStoreKind::Naive => Self::Naive(NaiveLabelSetStore::default()),
        }
    }

    /// Constructs the snapshot-shareable store required by embedded live
    /// queries. Configuration validation restricts this path to the production
    /// FlatInterned semantics.
    pub(super) fn new_versioned_flat() -> Self {
        Self::VersionedFlatInterned(LiveInternedStore::default())
    }

    pub(super) fn kind(&self) -> &'static str {
        match self {
            Self::FlatInterned(store) => {
                if store.key_value_storage_kind() == "paged" {
                    "ExperimentalFlatInternedPaged"
                } else if store.labelset_hash_kind() == "canonical_strings" {
                    "ExperimentalFlatInternedCanonicalStringHash"
                } else if store.labelset_hash_kind() == "interned_ids_siphash" {
                    "ExperimentalFlatInternedSipHash"
                } else if store.symbols().symbol_hash_kind() == "siphash" {
                    "ExperimentalFlatInternedSipHashSymbols"
                } else {
                    "FlatInterned"
                }
            }
            Self::VersionedFlatInterned(_) => "FlatInternedVersionedLive",
            Self::KeySetDictEncoded(_) => "KeySetDictEncoded",
            Self::Naive(_) => "Naive",
        }
    }

    pub(super) fn as_flat_interned(&self) -> Option<&InternedStore> {
        match self {
            Self::FlatInterned(store) => Some(store),
            Self::Naive(_) | Self::VersionedFlatInterned(_) | Self::KeySetDictEncoded(_) => None,
        }
    }

    pub(super) fn as_versioned_flat_interned(&self) -> Option<&LiveInternedStore> {
        match self {
            Self::VersionedFlatInterned(store) => Some(store),
            Self::Naive(_) | Self::FlatInterned(_) | Self::KeySetDictEncoded(_) => None,
        }
    }

    pub(super) fn live_snapshot(
        &mut self,
    ) -> std::result::Result<VersionedFlatInternedLabelSetSnapshot, LabelSetStoreError> {
        match self {
            Self::VersionedFlatInterned(store) => store.snapshot().map_err(Into::into),
            Self::Naive(_) | Self::FlatInterned(_) | Self::KeySetDictEncoded(_) => {
                Err(LabelSetStoreError::SealedStore)
            }
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
            Self::VersionedFlatInterned(store) => {
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

    fn intern_canonical(
        &mut self,
        labels: CanonicalLabelSet<'_, '_>,
        stats: &mut OtlpMetricsIngestionStats,
    ) -> std::result::Result<SeriesRef, LabelSetStoreError> {
        match self {
            Self::FlatInterned(store) => {
                let start = Instant::now();
                let series = store.intern_prepared_otlp(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::FlatInterned, elapsed);
                Ok(series)
            }
            Self::VersionedFlatInterned(store) => {
                let start = Instant::now();
                let series = store.intern_prepared_otlp(labels)?;
                let elapsed = start.elapsed();
                stats.record_intern(LabelSetStoreKind::FlatInterned, elapsed);
                Ok(series)
            }
            Self::Naive(_) | Self::KeySetDictEncoded(_) => {
                let labels = labels.iter().collect::<Vec<_>>();
                self.intern(labels.as_slice(), stats)
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
            Self::VersionedFlatInterned(store) => {
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
            Self::VersionedFlatInterned(store) => {
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
            Self::VersionedFlatInterned(store) => {
                let memory = store.memory_stats();
                LabelSetStoreStats {
                    series: store.len(),
                    symbols: Some(store.symbols().len()),
                    keysets: None,
                    alloc_bytes: store.estimate_size_bytes(),
                    used_bytes: store.estimate_used_bytes(),
                    symbols_alloc_bytes: memory
                        .shared_allocated_bytes
                        .saturating_add(memory.tail_allocated_bytes),
                    symbols_used_bytes: memory
                        .shared_used_bytes
                        .saturating_add(memory.tail_used_bytes),
                    buffer_stats: Some(format!(
                        "versioned pages={} non_empty_tails={}",
                        memory.shared_pages, memory.non_empty_tails
                    )),
                    symbol_table_stats: Some("versioned live symbol pages".to_string()),
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
