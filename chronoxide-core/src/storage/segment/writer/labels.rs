use super::*;

impl SegmentSeriesMetadata {
    pub fn series_id(&self) -> u64 {
        self.series_id
    }

    pub fn labels(&self) -> &[(String, String)] {
        &self.labels
    }
}
impl SegmentSeriesMetadataBuilder {
    pub fn new() -> Self {
        let mut labels = BTreeMap::new();
        labels.insert(METRIC_NAME_LABEL.to_string(), String::new());
        Self {
            labels,
            metric_name_seen: false,
        }
    }

    pub fn push_label(&mut self, name: &str, value: &str) {
        if name == METRIC_NAME_LABEL {
            if !self.metric_name_seen {
                self.labels
                    .insert(METRIC_NAME_LABEL.to_string(), normalize_metric_name(value));
                self.metric_name_seen = true;
            }
        } else {
            self.labels
                .insert(normalize_label_name(name), value.to_string());
        }
    }

    pub fn finish(self) -> SegmentSeriesMetadata {
        let labels: Vec<_> = self.labels.into_iter().collect();
        let series_id = segment_series_id(&labels);
        SegmentSeriesMetadata { series_id, labels }
    }
}

pub(in super::super) fn encode_canonical_segment_labels(
    labels: Vec<(String, String)>,
    symbols: &mut SegmentSymbols,
) -> WriterSeriesEntry {
    encode_borrowed_canonical_segment_labels(
        labels
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str())),
        symbols,
    )
}

pub(in super::super) fn encode_borrowed_canonical_segment_labels<'a>(
    labels: impl IntoIterator<Item = (&'a str, &'a str)>,
    symbols: &mut SegmentSymbols,
) -> WriterSeriesEntry {
    let mut bytes = Vec::new();
    let mut encoded_labels = Vec::new();
    for (key, value) in labels {
        bytes.extend_from_slice(key.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);

        let key_sym = symbols.intern(key);
        let value_sym = symbols.intern(value);
        encoded_labels.push((key_sym, value_sym));
    }

    WriterSeriesEntry {
        series_id: xxhash64(&bytes),
        kind_mask: SERIES_KIND_FLOAT,
        labels: encoded_labels,
    }
}

impl Default for SegmentSeriesMetadataBuilder {
    fn default() -> Self {
        Self::new()
    }
}
pub(in super::super) fn canonical_segment_metadata(
    labels: &[(String, String)],
) -> SegmentSeriesMetadata {
    let mut builder = SegmentSeriesMetadataBuilder::new();
    for (key, value) in labels {
        builder.push_label(key, value);
    }
    builder.finish()
}

pub(in super::super) fn apply_segment_metadata(
    active: &mut ActiveSegment,
    local_ref: u32,
    metadata: &SegmentSeriesMetadata,
) {
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        return;
    }

    let mut encoded_labels = Vec::with_capacity(metadata.labels.len());
    for (key, value) in &metadata.labels {
        let key_sym = active.symbols.intern(key);
        let value_sym = active.symbols.intern(value);
        encoded_labels.push((key_sym, value_sym));
    }

    active.series_entries[idx] = WriterSeriesEntry {
        series_id: metadata.series_id,
        kind_mask: SERIES_KIND_FLOAT,
        labels: encoded_labels,
    };
    active.metadata_present[idx] = true;
}

pub(in super::super) fn apply_label_visitor<F>(
    active: &mut ActiveSegment,
    local_ref: u32,
    visit_labels: &mut F,
) where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    apply_label_visitor_with_kind(active, local_ref, SERIES_KIND_FLOAT, visit_labels);
}

pub(in super::super) fn apply_label_visitor_with_kind<F>(
    active: &mut ActiveSegment,
    local_ref: u32,
    kind_mask: u8,
    visit_labels: &mut F,
) where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        active.series_entries[idx].kind_mask |= kind_mask;
        return;
    }

    let mut entry = encode_label_visitor_metadata(&mut active.symbols, |visit| {
        visit_labels(visit);
    });
    entry.kind_mask = kind_mask;
    active.series_entries[idx] = entry;
    active.metadata_present[idx] = true;
}

pub(in super::super) fn apply_flat_interned_label_metadata<S: SymbolTable>(
    active: &mut ActiveSegment,
    local_ref: u32,
    kind_mask: u8,
    source_series: SeriesRef,
    labelsets: &FlatInternedLabelSetStore<S>,
) {
    let idx = local_ref as usize;
    if active.metadata_present[idx] {
        active.series_entries[idx].kind_mask |= kind_mask;
        return;
    }

    let mut entry = encode_flat_interned_label_metadata(
        &mut active.symbols,
        &mut active.normalized_names,
        &mut active.metadata_hash_scratch,
        &mut active.metadata_label_scratch,
        labelsets,
        source_series,
    );
    entry.kind_mask = kind_mask;
    active.series_entries[idx] = entry;
    active.metadata_present[idx] = true;
}

impl Default for NormalizedNameCache {
    fn default() -> Self {
        Self::with_max_entries(MAX_NORMALIZED_NAME_CACHE_ENTRIES)
    }
}

impl NormalizedNameCache {
    pub(in super::super) fn with_max_entries(max_entries: usize) -> Self {
        Self {
            metric_label_name: Arc::from(METRIC_NAME_LABEL),
            label_names: HashMap::new(),
            metric_names: HashMap::new(),
            max_entries,
        }
    }

    pub(in super::super) fn metric_label_name(&self) -> Arc<str> {
        Arc::clone(&self.metric_label_name)
    }

    pub(in super::super) fn label_name(
        &mut self,
        source_id: SymbolId,
        source_name: &str,
        normalize: impl FnOnce(&str) -> String,
    ) -> Arc<str> {
        normalized_name(
            &mut self.label_names,
            self.max_entries,
            source_id,
            source_name,
            normalize,
        )
    }

    pub(in super::super) fn metric_name(
        &mut self,
        source_id: SymbolId,
        source_name: &str,
        normalize: impl FnOnce(&str) -> String,
    ) -> Arc<str> {
        normalized_name(
            &mut self.metric_names,
            self.max_entries,
            source_id,
            source_name,
            normalize,
        )
    }
}

fn normalized_name(
    cache: &mut HashMap<SymbolId, Arc<str>>,
    max_entries: usize,
    source_id: SymbolId,
    source_name: &str,
    normalize: impl FnOnce(&str) -> String,
) -> Arc<str> {
    if let Some(name) = cache.get(&source_id) {
        return Arc::clone(name);
    }

    let name = Arc::from(normalize(source_name));
    if cache.len() < max_entries {
        cache.insert(source_id, Arc::clone(&name));
    }
    name
}

pub(in super::super) fn encode_flat_interned_label_metadata<S: SymbolTable>(
    symbols: &mut SegmentSymbols,
    normalized_names: &mut NormalizedNameCache,
    hash_scratch: &mut Vec<u8>,
    label_scratch: &mut Vec<(Arc<str>, SourceLabelValue)>,
    labelsets: &FlatInternedLabelSetStore<S>,
    source_series: SeriesRef,
) -> WriterSeriesEntry {
    let source_symbols = labelsets.symbols();
    label_scratch.clear();
    let mut metric_name_seen = false;
    let mut labels_sorted = true;

    labelsets.visit_labelset_symbol_ids(source_series, |key_id, value_id| {
        let name = source_symbols.resolve(key_id);
        if name == METRIC_NAME_LABEL {
            if !metric_name_seen {
                let metric_name = normalized_names.metric_name(
                    value_id,
                    source_symbols.resolve(value_id),
                    normalize_metric_name,
                );
                let key = normalized_names.metric_label_name();
                if let Some((last_key, _)) = label_scratch.last()
                    && last_key.as_ref() > key.as_ref()
                {
                    labels_sorted = false;
                }
                label_scratch.push((key, SourceLabelValue::Owned(metric_name)));
                metric_name_seen = true;
            }
        } else {
            let key = normalized_names.label_name(key_id, name, normalize_label_name);
            if let Some((last_key, _)) = label_scratch.last()
                && last_key.as_ref() > key.as_ref()
            {
                labels_sorted = false;
            }
            label_scratch.push((key, SourceLabelValue::Symbol(value_id)));
        }
    });

    if !metric_name_seen {
        let key = normalized_names.metric_label_name();
        if let Some((last_key, _)) = label_scratch.last()
            && last_key.as_ref() > key.as_ref()
        {
            labels_sorted = false;
        }
        label_scratch.push((key, SourceLabelValue::Owned(Arc::from(""))));
    }

    if !labels_sorted {
        label_scratch.sort_by(|left, right| left.0.as_ref().cmp(right.0.as_ref()));
    }

    let entry =
        encode_flat_interned_sorted_labels(label_scratch, source_symbols, symbols, hash_scratch);
    label_scratch.clear();
    entry
}

pub(in super::super) fn encode_flat_interned_sorted_labels<S: SymbolTable>(
    labels: &[(Arc<str>, SourceLabelValue)],
    source_symbols: &S,
    symbols: &mut SegmentSymbols,
    hash_scratch: &mut Vec<u8>,
) -> WriterSeriesEntry {
    hash_scratch.clear();
    let mut encoded_labels = Vec::with_capacity(labels.len());

    let mut idx = 0usize;
    while idx < labels.len() {
        let mut next = idx + 1;
        while next < labels.len() && labels[next].0 == labels[idx].0 {
            next += 1;
        }

        let (key, value) = &labels[next - 1];
        let value = resolve_source_label_value(source_symbols, value);

        hash_scratch.extend_from_slice(key.as_ref().as_bytes());
        hash_scratch.push(0);
        hash_scratch.extend_from_slice(value.as_bytes());
        hash_scratch.push(0xff);

        let key_sym = symbols.intern(key.as_ref());
        let value_sym = symbols.intern(value);
        encoded_labels.push((key_sym, value_sym));
        idx = next;
    }

    let series_id = xxhash64(hash_scratch);
    hash_scratch.clear();

    WriterSeriesEntry {
        series_id,
        kind_mask: SERIES_KIND_FLOAT,
        labels: encoded_labels,
    }
}

fn resolve_source_label_value<'a, S: SymbolTable>(
    source_symbols: &'a S,
    value: &'a SourceLabelValue,
) -> &'a str {
    match value {
        SourceLabelValue::Symbol(id) => source_symbols.resolve(*id),
        SourceLabelValue::Owned(value) => value.as_ref(),
    }
}

pub(in super::super) fn encode_label_visitor_metadata<F>(
    symbols: &mut SegmentSymbols,
    mut visit_labels: F,
) -> WriterSeriesEntry
where
    F: FnMut(&mut dyn FnMut(&str, &str)),
{
    let mut labels = Vec::new();
    let mut metric_name = String::new();
    let mut metric_name_seen = false;
    let mut push_label = |name: &str, value: &str| {
        if name == METRIC_NAME_LABEL {
            if !metric_name_seen {
                metric_name = normalize_metric_name(value);
                metric_name_seen = true;
            }
        } else {
            labels.push((normalize_label_name(name), value.to_string()));
        }
    };
    visit_labels(&mut push_label);

    labels.push((METRIC_NAME_LABEL.to_string(), metric_name));
    labels.sort_by(|left, right| left.0.cmp(&right.0));

    let mut canonical = Vec::with_capacity(labels.len());
    for (key, value) in labels {
        if let Some((last_key, last_value)) = canonical.last_mut()
            && last_key == &key
        {
            *last_value = value;
            continue;
        }
        canonical.push((key, value));
    }

    encode_canonical_segment_labels(canonical, symbols)
}

pub(in super::super) fn update_label_value_time_ranges(
    index: &mut LabelValueTimeRangeIndex,
    entry: &impl crate::storage::series::SeriesEntryView,
    chunk: &ChunkIndexEntry,
) {
    index.insert_many(entry.labels(), chunk.min_time_ms, chunk.max_time_ms);
}

pub(crate) fn segment_series_id(labels: &[(String, String)]) -> u64 {
    let mut bytes = Vec::new();
    for (name, value) in labels {
        bytes.extend_from_slice(name.as_bytes());
        bytes.push(0);
        bytes.extend_from_slice(value.as_bytes());
        bytes.push(0xff);
    }
    xxhash64(&bytes)
}
