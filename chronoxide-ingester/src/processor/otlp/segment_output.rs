use super::*;

#[derive(Debug, Eq, PartialEq)]
struct HeadSeriesQueryOrderKey {
    metric_name: String,
    kind_mask: u8,
    labels: Vec<(String, String)>,
    series_id: u64,
    source_ref: SeriesRef,
    old_ref: usize,
}

pub(super) fn order_series_samples_for_metric_query(
    series_samples: &mut Vec<(SeriesRef, SeriesSamples)>,
    labelsets: &LabelSetInterner,
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        return order_flat_interned_series_samples_for_metric_query(series_samples, flat);
    }

    order_series_samples_for_metric_query_with_metadata(series_samples, labelsets)
}

pub(super) fn order_series_samples_for_metric_query_with_metadata(
    series_samples: &mut Vec<(SeriesRef, SeriesSamples)>,
    labelsets: &LabelSetInterner,
) -> Result<()> {
    let mut keys = Vec::with_capacity(series_samples.len());
    for (old_ref, (series, samples)) in series_samples.iter().enumerate() {
        let metadata = labelsets.segment_metadata(*series);
        let metric_name = metadata
            .labels()
            .iter()
            .find_map(|(key, value)| (key == METRIC_NAME_LABEL).then(|| value.clone()))
            .unwrap_or_default();
        keys.push(HeadSeriesQueryOrderKey {
            metric_name,
            kind_mask: series_samples_kind_mask(samples),
            labels: metadata.labels().to_vec(),
            series_id: metadata.series_id(),
            source_ref: *series,
            old_ref,
        });
    }

    keys.sort_by(|left, right| {
        left.metric_name
            .cmp(&right.metric_name)
            .then_with(|| left.kind_mask.cmp(&right.kind_mask))
            .then_with(|| left.labels.cmp(&right.labels))
            .then_with(|| left.series_id.cmp(&right.series_id))
            .then_with(|| left.source_ref.cmp(&right.source_ref))
            .then_with(|| left.old_ref.cmp(&right.old_ref))
    });

    reorder_series_samples_by_old_indices(series_samples, keys.into_iter().map(|key| key.old_ref))
}

struct FlatHeadSeriesQueryOrderKey {
    metric_name: Arc<str>,
    kind_mask: u8,
    labels: Vec<FlatHeadSeriesLabel>,
    source_ref: SeriesRef,
    old_ref: usize,
}

struct FlatHeadSeriesLabel {
    name: Arc<str>,
    value: FlatHeadSeriesLabelValue,
}

enum FlatHeadSeriesLabelValue {
    Source(SymbolId),
    Normalized(Arc<str>),
}

impl FlatHeadSeriesLabelValue {
    fn as_str<'a>(&'a self, symbols: &'a DefaultSymbolTable) -> &'a str {
        match self {
            Self::Source(id) => symbols.resolve(*id),
            Self::Normalized(value) => value.as_ref(),
        }
    }
}

struct FlatHeadSeriesOrderNameCache<'a> {
    symbols: &'a DefaultSymbolTable,
    metric_label_name: Arc<str>,
    empty_metric_name: Arc<str>,
    label_names: HashMap<SymbolId, Arc<str>>,
    metric_names: HashMap<SymbolId, Arc<str>>,
}

impl<'a> FlatHeadSeriesOrderNameCache<'a> {
    fn new(symbols: &'a DefaultSymbolTable) -> Self {
        Self {
            symbols,
            metric_label_name: Arc::from(METRIC_NAME_LABEL),
            empty_metric_name: Arc::from(""),
            label_names: HashMap::new(),
            metric_names: HashMap::new(),
        }
    }

    fn label_name(&mut self, source_id: SymbolId) -> Arc<str> {
        if let Some(name) = self.label_names.get(&source_id) {
            return Arc::clone(name);
        }
        let name = Arc::from(normalize_label_name(self.symbols.resolve(source_id)));
        self.label_names.insert(source_id, Arc::clone(&name));
        name
    }

    fn metric_name(&mut self, source_id: SymbolId) -> Arc<str> {
        if let Some(name) = self.metric_names.get(&source_id) {
            return Arc::clone(name);
        }
        let name = Arc::from(normalize_metric_name(self.symbols.resolve(source_id)));
        self.metric_names.insert(source_id, Arc::clone(&name));
        name
    }

    fn build_key(
        &mut self,
        labelsets: &InternedStore,
        series: SeriesRef,
        samples: &SeriesSamples,
        old_ref: usize,
    ) -> FlatHeadSeriesQueryOrderKey {
        let mut metric_name = None;
        let mut metric_name_seen = false;
        let mut labels = Vec::with_capacity(16);

        labelsets.visit_labelset_symbol_ids(series, |key_id, value_id| {
            let source_name = self.symbols.resolve(key_id);
            if source_name == METRIC_NAME_LABEL {
                if !metric_name_seen {
                    metric_name = Some(self.metric_name(value_id));
                    metric_name_seen = true;
                }
            } else {
                labels.push(FlatHeadSeriesLabel {
                    name: self.label_name(key_id),
                    value: FlatHeadSeriesLabelValue::Source(value_id),
                });
            }
        });

        let metric_name = metric_name.unwrap_or_else(|| Arc::clone(&self.empty_metric_name));
        labels.push(FlatHeadSeriesLabel {
            name: Arc::clone(&self.metric_label_name),
            value: FlatHeadSeriesLabelValue::Normalized(Arc::clone(&metric_name)),
        });
        labels.sort_by(|left, right| left.name.as_ref().cmp(right.name.as_ref()));

        let mut canonical: Vec<FlatHeadSeriesLabel> = Vec::with_capacity(labels.len());
        for label in labels {
            if let Some(last) = canonical.last_mut()
                && last.name == label.name
            {
                *last = label;
                continue;
            }
            canonical.push(label);
        }

        FlatHeadSeriesQueryOrderKey {
            metric_name,
            kind_mask: series_samples_kind_mask(samples),
            labels: canonical,
            source_ref: series,
            old_ref,
        }
    }
}

fn order_flat_interned_series_samples_for_metric_query(
    series_samples: &mut Vec<(SeriesRef, SeriesSamples)>,
    labelsets: &InternedStore,
) -> Result<()> {
    let mut cache = FlatHeadSeriesOrderNameCache::new(labelsets.symbols());
    let mut keys = Vec::with_capacity(series_samples.len());
    for (old_ref, (series, samples)) in series_samples.iter().enumerate() {
        keys.push(cache.build_key(labelsets, *series, samples, old_ref));
    }
    let symbols = labelsets.symbols();
    keys.sort_by(|left, right| compare_flat_order_keys(left, right, symbols));

    reorder_series_samples_by_old_indices(series_samples, keys.into_iter().map(|key| key.old_ref))
}

fn compare_flat_order_keys(
    left: &FlatHeadSeriesQueryOrderKey,
    right: &FlatHeadSeriesQueryOrderKey,
    symbols: &DefaultSymbolTable,
) -> Ordering {
    left.metric_name
        .as_ref()
        .cmp(right.metric_name.as_ref())
        .then_with(|| left.kind_mask.cmp(&right.kind_mask))
        .then_with(|| compare_flat_order_labels(&left.labels, &right.labels, symbols))
        .then_with(|| left.source_ref.cmp(&right.source_ref))
        .then_with(|| left.old_ref.cmp(&right.old_ref))
}

fn compare_flat_order_labels(
    left: &[FlatHeadSeriesLabel],
    right: &[FlatHeadSeriesLabel],
    symbols: &DefaultSymbolTable,
) -> Ordering {
    let mut left_iter = left.iter();
    let mut right_iter = right.iter();
    loop {
        match (left_iter.next(), right_iter.next()) {
            (Some(left), Some(right)) => {
                let ordering = left
                    .name
                    .as_ref()
                    .cmp(right.name.as_ref())
                    .then_with(|| left.value.as_str(symbols).cmp(right.value.as_str(symbols)));
                if ordering != Ordering::Equal {
                    return ordering;
                }
            }
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (None, None) => return Ordering::Equal,
        }
    }
}

fn reorder_series_samples_by_old_indices(
    series_samples: &mut Vec<(SeriesRef, SeriesSamples)>,
    order: impl IntoIterator<Item = usize>,
) -> Result<()> {
    let mut slots = std::mem::take(series_samples)
        .into_iter()
        .map(Some)
        .collect::<Vec<_>>();
    for old_ref in order {
        let Some(slot) = slots.get_mut(old_ref) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series sample order contains out-of-range ref",
            )
            .into());
        };
        let Some(item) = slot.take() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "series sample order contains duplicate ref",
            )
            .into());
        };
        series_samples.push(item);
    }
    if series_samples.len() != slots.len() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "series sample order is missing a ref",
        )
        .into());
    }
    Ok(())
}

fn series_samples_kind_mask(samples: &SeriesSamples) -> u8 {
    match samples {
        SeriesSamples::Float { .. } | SeriesSamples::Int64 { .. } => SERIES_KIND_FLOAT,
        SeriesSamples::Histogram { .. } => SERIES_KIND_HISTOGRAM,
        SeriesSamples::ExponentialHistogram { .. } => SERIES_KIND_EXPONENTIAL_HISTOGRAM,
        SeriesSamples::Summary { .. } => SERIES_KIND_SUMMARY,
    }
}

pub(super) fn record_segment_float_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, f64)],
    raw: bool,
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        if raw {
            writer.record_samples_raw_ordered_with_flat_interned_labels(series, samples, flat)?;
        } else {
            writer.record_samples_ordered_with_flat_interned_labels(series, samples, flat)?;
        }
        return Ok(());
    }

    if raw {
        writer.record_samples_raw_ordered_with_label_visitor(series, samples, |visit| {
            labelsets.visit_labelset(series, |key, value| visit(key, value));
        })?;
    } else {
        writer.record_samples_ordered_with_label_visitor(series, samples, |visit| {
            labelsets.visit_labelset(series, |key, value| visit(key, value));
        })?;
    }
    Ok(())
}

pub(super) fn record_segment_histogram_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, HistogramValue)],
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        writer.record_histogram_samples_ordered_with_flat_interned_labels(series, samples, flat)?;
        return Ok(());
    }

    writer.record_histogram_samples_ordered_with_label_visitor(series, samples, |visit| {
        labelsets.visit_labelset(series, |key, value| visit(key, value));
    })?;
    Ok(())
}

pub(super) fn record_segment_exponential_histogram_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, ExponentialHistogramValue)],
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        writer.record_exponential_histogram_samples_ordered_with_flat_interned_labels(
            series, samples, flat,
        )?;
        return Ok(());
    }

    writer.record_exponential_histogram_samples_ordered_with_label_visitor(
        series,
        samples,
        |visit| {
            labelsets.visit_labelset(series, |key, value| visit(key, value));
        },
    )?;
    Ok(())
}

pub(super) fn record_segment_summary_samples(
    labelsets: &LabelSetInterner,
    writer: &mut SegmentWriter,
    series: SeriesRef,
    samples: &[(u64, SummaryValue)],
) -> Result<()> {
    if let Some(flat) = labelsets.as_flat_interned() {
        writer.record_summary_samples_ordered_with_flat_interned_labels(series, samples, flat)?;
        return Ok(());
    }

    writer.record_summary_samples_ordered_with_label_visitor(series, samples, |visit| {
        labelsets.visit_labelset(series, |key, value| visit(key, value));
    })?;
    Ok(())
}
