use super::*;

#[derive(Debug)]
pub struct HeadWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub(super) series: HashMap<SeriesRef, EncodedSeries>,
    pub datapoints: u64,
    pub(super) arena: BlockArena,
}

impl HeadWindow {
    pub(super) fn new(start_ms: u64, end_ms: u64) -> Self {
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

    pub(super) fn seal_all_series(&mut self) {
        for encoded in self.series.values_mut() {
            encoded.seal(&mut self.arena);
        }
    }

    pub(super) fn bytes_by_kind<F, G>(&self, mut bytes_fn: F, mut number_kind: G) -> BytesByKind
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
pub(super) struct HeadSelectorIndexKey {
    pub(super) start_ms: u64,
    pub(super) end_ms: u64,
    pub(super) datapoints: u64,
    pub(super) series_len: usize,
    pub(super) label_resolver_len: usize,
}

impl HeadSelectorIndexKey {
    pub(super) fn new(window: &HeadWindow, label_resolver_len: usize) -> Self {
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
pub(super) struct CachedHeadSelectorIndex {
    pub(super) key: HeadSelectorIndexKey,
    pub(super) index: HeadSelectorIndex,
}

#[derive(Debug, Clone)]
pub(super) struct HeadIndexedSeries {
    pub(super) series_id: u64,
    pub(super) labels: Vec<(String, String)>,
}

#[derive(Debug, Clone, Default)]
pub(super) struct HeadSelectorIndex {
    pub(super) all_series: Vec<SeriesRef>,
    pub(super) series: BTreeMap<SeriesRef, HeadIndexedSeries>,
    pub(super) postings: BTreeMap<(String, String), Vec<SeriesRef>>,
    pub(super) label_values: BTreeMap<String, Vec<String>>,
}

impl HeadSelectorIndex {
    pub(super) fn build<R>(window: &HeadWindow, labels: &R) -> io::Result<Self>
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

    pub(super) fn series(&self, series: &SeriesRef) -> Option<&HeadIndexedSeries> {
        self.series.get(series)
    }

    pub(super) fn matching_series(
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

    pub(super) fn exact_postings(&self, name: &str, value: &str) -> Vec<SeriesRef> {
        self.postings
            .get(&(name.to_string(), value.to_string()))
            .cloned()
            .unwrap_or_default()
    }

    pub(super) fn regex_postings(
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
