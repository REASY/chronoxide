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
) -> Result<Vec<u32>> {
    match series_samples.as_slice() {
        [] => return Ok(Vec::new()),
        [(series, _)] => {
            return Ok(vec![checked_canonical_label_count(
                labelsets.segment_metadata(*series).labels().len(),
            )?]);
        }
        _ => {}
    }
    if let Some(flat) = labelsets.as_flat_interned() {
        return order_flat_interned_series_samples_for_metric_query(series_samples, flat);
    }

    order_series_samples_for_metric_query_with_metadata(series_samples, labelsets)
}

pub(super) fn order_series_samples_for_metric_query_with_metadata(
    series_samples: &mut Vec<(SeriesRef, SeriesSamples)>,
    labelsets: &LabelSetInterner,
) -> Result<Vec<u32>> {
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

    let canonical_label_counts = keys
        .iter()
        .map(|key| checked_canonical_label_count(key.labels.len()))
        .collect::<Result<Vec<_>>>()?;
    reorder_series_samples_by_old_indices(series_samples, keys.into_iter().map(|key| key.old_ref))?;
    Ok(canonical_label_counts)
}

const FLAT_ORDER_UNSET_ID: u32 = u32::MAX;
const FLAT_ORDER_METRIC_VALUE_SLOT: u32 = u32::MAX;

#[derive(Clone, Copy)]
struct FlatHeadSeriesProjectionLabel {
    name_id: u32,
    value_slot: u32,
}

struct FlatHeadSeriesProjectionPlan {
    metric_slot: Option<u32>,
    labels: Vec<FlatHeadSeriesProjectionLabel>,
}

struct FlatHeadSeriesProjectionColumns {
    metric_order: Vec<u32>,
    kind_masks: Vec<u8>,
    plan_ids: Vec<u32>,
    source_refs: Vec<SeriesRef>,
}

impl FlatHeadSeriesProjectionColumns {
    fn with_capacity(capacity: usize) -> Self {
        Self {
            metric_order: Vec::with_capacity(capacity),
            kind_masks: Vec::with_capacity(capacity),
            plan_ids: Vec::with_capacity(capacity),
            source_refs: Vec::with_capacity(capacity),
        }
    }

    fn push(&mut self, metric_name_id: u32, kind_mask: u8, plan_id: u32, source_ref: SeriesRef) {
        self.metric_order.push(metric_name_id);
        self.kind_masks.push(kind_mask);
        self.plan_ids.push(plan_id);
        self.source_refs.push(source_ref);
    }

    fn replace_metric_name_ids_with_ranks(&mut self, metric_name_ranks: &[u32]) {
        for metric_order in &mut self.metric_order {
            *metric_order = metric_name_ranks[*metric_order as usize];
        }
    }
}

struct FlatHeadSeriesProjectionBuilder {
    keyset_to_plan: ahash::AHashMap<Vec<SymbolId>, u32>,
    keyset_scratch: Vec<SymbolId>,
    plans: Vec<FlatHeadSeriesProjectionPlan>,
    label_name_id_by_symbol: Vec<u32>,
    label_names: Vec<String>,
    metric_name_id_by_symbol: Vec<u32>,
    metric_names: Vec<String>,
    source_value_seen: Vec<bool>,
    source_value_ids: Vec<SymbolId>,
}

impl FlatHeadSeriesProjectionBuilder {
    fn new(symbol_count: usize) -> Self {
        Self {
            keyset_to_plan: ahash::AHashMap::new(),
            keyset_scratch: Vec::new(),
            plans: Vec::new(),
            label_name_id_by_symbol: vec![FLAT_ORDER_UNSET_ID; symbol_count],
            label_names: vec![METRIC_NAME_LABEL.to_string()],
            metric_name_id_by_symbol: vec![FLAT_ORDER_UNSET_ID; symbol_count],
            metric_names: vec![String::new()],
            source_value_seen: vec![false; symbol_count],
            source_value_ids: Vec::new(),
        }
    }

    fn record_series(
        &mut self,
        row: FlatInternedLabelSetRow<'_>,
        symbols: &DefaultSymbolTable,
    ) -> Result<(u32, u32)> {
        let plan_id = self.plan_id(row, symbols)?;
        let plan_index = plan_id as usize;
        let metric_name_id = if let Some(slot) = self.plans[plan_index].metric_slot {
            let (_, value_id) = row.get(slot as usize).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "flat metric-order projection contains an out-of-range metric slot",
                )
            })?;
            self.metric_name_id(value_id, symbols)?
        } else {
            0
        };

        Ok((plan_id, metric_name_id))
    }

    fn plan_id(
        &mut self,
        row: FlatInternedLabelSetRow<'_>,
        symbols: &DefaultSymbolTable,
    ) -> Result<u32> {
        self.keyset_scratch.clear();
        for (key_id, value_id) in row.iter() {
            self.keyset_scratch.push(key_id);
            let seen = &mut self.source_value_seen[value_id.get() as usize];
            if !*seen {
                *seen = true;
                self.source_value_ids.push(value_id);
            }
        }
        if let Some(&plan_id) = self.keyset_to_plan.get(self.keyset_scratch.as_slice()) {
            return Ok(plan_id);
        }

        let plan_id = flat_order_index(self.plans.len(), "projection plan")?;
        let mut metric_slot = None;
        let mut labels = Vec::with_capacity(row.len().saturating_add(1));
        for (slot, (key_id, _)) in row.iter().enumerate() {
            if symbols.resolve(key_id) == METRIC_NAME_LABEL {
                if metric_slot.is_none() {
                    metric_slot = Some(flat_order_index(slot, "metric label slot")?);
                }
                continue;
            }
            labels.push(FlatHeadSeriesProjectionLabel {
                name_id: self.label_name_id(key_id, symbols)?,
                value_slot: flat_order_index(slot, "label value slot")?,
            });
        }
        labels.push(FlatHeadSeriesProjectionLabel {
            name_id: 0,
            value_slot: FLAT_ORDER_METRIC_VALUE_SLOT,
        });

        self.plans.push(FlatHeadSeriesProjectionPlan {
            metric_slot,
            labels,
        });
        self.keyset_to_plan
            .insert(self.keyset_scratch.clone(), plan_id);
        Ok(plan_id)
    }

    fn label_name_id(&mut self, source_id: SymbolId, symbols: &DefaultSymbolTable) -> Result<u32> {
        let source_index = source_id.get() as usize;
        let cached = self.label_name_id_by_symbol[source_index];
        if cached != FLAT_ORDER_UNSET_ID {
            return Ok(cached);
        }

        let name_id = flat_order_index(self.label_names.len(), "normalized label name")?;
        self.label_names
            .push(normalize_label_name(symbols.resolve(source_id)));
        self.label_name_id_by_symbol[source_index] = name_id;
        Ok(name_id)
    }

    fn metric_name_id(&mut self, source_id: SymbolId, symbols: &DefaultSymbolTable) -> Result<u32> {
        let source_index = source_id.get() as usize;
        let cached = self.metric_name_id_by_symbol[source_index];
        if cached != FLAT_ORDER_UNSET_ID {
            return Ok(cached);
        }

        let name_id = flat_order_index(self.metric_names.len(), "normalized metric name")?;
        self.metric_names
            .push(normalize_metric_name(symbols.resolve(source_id)));
        self.metric_name_id_by_symbol[source_index] = name_id;
        Ok(name_id)
    }

    fn finish(mut self, symbols: &DefaultSymbolTable) -> Result<FlatHeadSeriesProjectionTables> {
        let label_name_ranks = lexical_string_ranks(&self.label_names, "label-name rank")?;
        for plan in &mut self.plans {
            for label in &mut plan.labels {
                label.name_id = label_name_ranks[label.name_id as usize];
            }
            plan.labels.sort_by_key(|label| label.name_id);

            let mut canonical_len = 0;
            for read_index in 0..plan.labels.len() {
                let label = plan.labels[read_index];
                if canonical_len > 0 && plan.labels[canonical_len - 1].name_id == label.name_id {
                    plan.labels[canonical_len - 1] = label;
                } else {
                    plan.labels[canonical_len] = label;
                    canonical_len += 1;
                }
            }
            plan.labels.truncate(canonical_len);
        }

        let metric_name_ranks = lexical_string_ranks(&self.metric_names, "metric-name rank")?;
        let source_value_ranks =
            lexical_symbol_ranks(symbols, &mut self.source_value_ids, "label-value rank")?;

        Ok(FlatHeadSeriesProjectionTables {
            plans: self.plans,
            metric_name_ranks,
            source_value_ranks,
        })
    }
}

struct FlatHeadSeriesProjectionTables {
    plans: Vec<FlatHeadSeriesProjectionPlan>,
    metric_name_ranks: Vec<u32>,
    source_value_ranks: Vec<u32>,
}

fn flat_order_index(value: usize, field: &'static str) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("flat metric-order {field} exceeds u32 capacity"),
        )
        .into()
    })
}

pub(super) fn checked_canonical_label_count(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "canonical metric-order label count exceeds u32",
        )
        .into()
    })
}

fn lexical_string_ranks(values: &[String], field: &'static str) -> Result<Vec<u32>> {
    flat_order_index(values.len(), field)?;
    let mut order = (0..values.len() as u32).collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| {
        values[*left as usize]
            .cmp(&values[*right as usize])
            .then_with(|| left.cmp(right))
    });

    let mut ranks = vec![FLAT_ORDER_UNSET_ID; values.len()];
    let mut rank = 0_u32;
    for (position, &value_id) in order.iter().enumerate() {
        if position > 0 {
            let previous_id = order[position - 1];
            if values[value_id as usize] != values[previous_id as usize] {
                rank = rank.checked_add(1).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidData, format!("{field} overflow"))
                })?;
            }
        }
        ranks[value_id as usize] = rank;
    }
    Ok(ranks)
}

fn lexical_symbol_ranks(
    symbols: &DefaultSymbolTable,
    value_ids: &mut [SymbolId],
    field: &'static str,
) -> Result<Vec<u32>> {
    flat_order_index(value_ids.len(), field)?;
    value_ids.sort_unstable_by(|left, right| {
        symbols
            .resolve(*left)
            .cmp(symbols.resolve(*right))
            .then_with(|| left.cmp(right))
    });

    let mut ranks = vec![FLAT_ORDER_UNSET_ID; symbols.len()];
    let mut rank = 0_u32;
    for position in 0..value_ids.len() {
        if position > 0
            && symbols.resolve(value_ids[position]) != symbols.resolve(value_ids[position - 1])
        {
            rank = rank.checked_add(1).ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, format!("{field} overflow"))
            })?;
        }
        ranks[value_ids[position].get() as usize] = rank;
    }
    Ok(ranks)
}

fn order_flat_interned_series_samples_for_metric_query(
    series_samples: &mut Vec<(SeriesRef, SeriesSamples)>,
    labelsets: &InternedStore,
) -> Result<Vec<u32>> {
    let series_count = flat_order_index(series_samples.len(), "series count")?;
    let symbols = labelsets.symbols();
    let mut builder = FlatHeadSeriesProjectionBuilder::new(symbols.len());
    let mut columns = FlatHeadSeriesProjectionColumns::with_capacity(series_samples.len());
    for (series, samples) in series_samples.iter() {
        let (plan_id, metric_name_id) =
            builder.record_series(labelsets.labelset_symbol_ids(*series), symbols)?;
        columns.push(
            metric_name_id,
            series_samples_kind_mask(samples),
            plan_id,
            *series,
        );
    }
    let mut tables = builder.finish(symbols)?;
    columns.replace_metric_name_ids_with_ranks(&tables.metric_name_ranks);
    tables.metric_name_ranks = Vec::new();

    let mut order = (0..series_count).collect::<Vec<_>>();
    order.sort_unstable_by(|left, right| {
        compare_flat_indirect_order(*left, *right, labelsets, &columns, &tables)
    });
    for plan_id in &mut columns.plan_ids {
        let plan = tables.plans.get(*plan_id as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "flat metric-order label count references an unknown projection plan",
            )
        })?;
        *plan_id = checked_canonical_label_count(plan.labels.len())?;
    }
    let FlatHeadSeriesProjectionColumns {
        metric_order,
        kind_masks,
        plan_ids: canonical_label_counts_by_old_ref,
        source_refs,
    } = columns;
    drop((metric_order, kind_masks, source_refs, tables));

    reorder_series_samples_by_old_indices(
        series_samples,
        order.iter().map(|old_ref| *old_ref as usize),
    )?;
    for old_ref in &mut order {
        let old_index = *old_ref as usize;
        *old_ref = *canonical_label_counts_by_old_ref
            .get(old_index)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "flat metric-order label count is missing a projection plan",
                )
            })?;
    }

    Ok(order)
}

fn compare_flat_indirect_order(
    left: u32,
    right: u32,
    labelsets: &InternedStore,
    columns: &FlatHeadSeriesProjectionColumns,
    tables: &FlatHeadSeriesProjectionTables,
) -> Ordering {
    let left_index = left as usize;
    let right_index = right as usize;

    columns.metric_order[left_index]
        .cmp(&columns.metric_order[right_index])
        .then_with(|| columns.kind_masks[left_index].cmp(&columns.kind_masks[right_index]))
        .then_with(|| {
            compare_flat_projection_labels(
                &tables.plans[columns.plan_ids[left_index] as usize],
                labelsets.labelset_symbol_ids(columns.source_refs[left_index]),
                &tables.plans[columns.plan_ids[right_index] as usize],
                labelsets.labelset_symbol_ids(columns.source_refs[right_index]),
                tables,
            )
        })
        .then_with(|| columns.source_refs[left_index].cmp(&columns.source_refs[right_index]))
        .then_with(|| left.cmp(&right))
}

fn compare_flat_projection_labels(
    left_plan: &FlatHeadSeriesProjectionPlan,
    left_row: FlatInternedLabelSetRow<'_>,
    right_plan: &FlatHeadSeriesProjectionPlan,
    right_row: FlatInternedLabelSetRow<'_>,
    tables: &FlatHeadSeriesProjectionTables,
) -> Ordering {
    let common_len = left_plan.labels.len().min(right_plan.labels.len());
    for index in 0..common_len {
        let left = left_plan.labels[index];
        let right = right_plan.labels[index];
        let ordering = left
            .name_id
            .cmp(&right.name_id)
            .then_with(|| compare_flat_projection_values(left, left_row, right, right_row, tables));
        if ordering != Ordering::Equal {
            return ordering;
        }
    }
    left_plan.labels.len().cmp(&right_plan.labels.len())
}

fn compare_flat_projection_values(
    left: FlatHeadSeriesProjectionLabel,
    left_row: FlatInternedLabelSetRow<'_>,
    right: FlatHeadSeriesProjectionLabel,
    right_row: FlatInternedLabelSetRow<'_>,
    tables: &FlatHeadSeriesProjectionTables,
) -> Ordering {
    match (
        left.value_slot == FLAT_ORDER_METRIC_VALUE_SLOT,
        right.value_slot == FLAT_ORDER_METRIC_VALUE_SLOT,
    ) {
        (true, true) => Ordering::Equal,
        (false, false) => {
            let (_, left_value_id) = left_row.symbol_ids_at(left.value_slot as usize);
            let (_, right_value_id) = right_row.symbol_ids_at(right.value_slot as usize);
            tables.source_value_ranks[left_value_id.get() as usize]
                .cmp(&tables.source_value_ranks[right_value_id.get() as usize])
        }
        (true, false) | (false, true) => {
            unreachable!("canonical metric-label projections must use the synthetic value")
        }
    }
}

#[cfg(test)]
struct FlatHeadSeriesQueryOrderKey {
    metric_name: Arc<str>,
    kind_mask: u8,
    labels: Vec<FlatHeadSeriesLabel>,
    source_ref: SeriesRef,
    old_ref: usize,
}

#[cfg(test)]
struct FlatHeadSeriesLabel {
    name: Arc<str>,
    value: FlatHeadSeriesLabelValue,
}

#[cfg(test)]
enum FlatHeadSeriesLabelValue {
    Source(SymbolId),
    Normalized(Arc<str>),
}

#[cfg(test)]
impl FlatHeadSeriesLabelValue {
    fn as_str<'a>(&'a self, symbols: &'a DefaultSymbolTable) -> &'a str {
        match self {
            Self::Source(id) => symbols.resolve(*id),
            Self::Normalized(value) => value.as_ref(),
        }
    }
}

#[cfg(test)]
struct FlatHeadSeriesOrderNameCache<'a> {
    symbols: &'a DefaultSymbolTable,
    metric_label_name: Arc<str>,
    empty_metric_name: Arc<str>,
    label_names: HashMap<SymbolId, Arc<str>>,
    metric_names: HashMap<SymbolId, Arc<str>>,
}

#[cfg(test)]
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

#[cfg(test)]
pub(super) fn order_flat_interned_series_samples_for_metric_query_owned_reference(
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

#[cfg(test)]
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

#[cfg(test)]
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
