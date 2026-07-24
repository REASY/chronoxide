use super::*;

#[derive(Debug)]
pub struct HeadWindow {
    pub start_ms: u64,
    pub end_ms: u64,
    pub(super) series: HeadSeriesTable<EncodedSeries>,
    pub datapoints: u64,
    pub(super) arena: BlockArena,
    pub(super) out_of_order: bool,
}

/// Decoded rows for one head seal operation.
///
/// Most series produce one row. A canonical series carrying multiple native
/// metric kinds produces one row per kind while retaining one metadata series
/// in the segment writer.
#[derive(Debug)]
pub struct SealedHeadWindowSamples {
    series_samples: Vec<(SeriesRef, SeriesSamples)>,
    unique_series_count: usize,
}

impl SealedHeadWindowSamples {
    pub fn into_parts(self) -> (Vec<(SeriesRef, SeriesSamples)>, usize) {
        (self.series_samples, self.unique_series_count)
    }
}

impl HeadWindow {
    pub(super) fn new(start_ms: u64, end_ms: u64, adaptive_series_table: bool) -> Self {
        Self::new_with_lane(start_ms, end_ms, adaptive_series_table, false)
    }

    pub(super) fn new_out_of_order(
        start_ms: u64,
        end_ms: u64,
        adaptive_series_table: bool,
    ) -> Self {
        Self::new_with_lane(start_ms, end_ms, adaptive_series_table, true)
    }

    fn new_with_lane(
        start_ms: u64,
        end_ms: u64,
        adaptive_series_table: bool,
        out_of_order: bool,
    ) -> Self {
        Self {
            start_ms,
            end_ms,
            series: HeadSeriesTable::new(adaptive_series_table),
            datapoints: 0,
            arena: BlockArena::new(DEFAULT_HEAD_ARENA_PAGE_BYTES),
            out_of_order,
        }
    }

    pub fn is_out_of_order(&self) -> bool {
        self.out_of_order
    }

    pub fn into_series_samples(self) -> io::Result<Vec<(SeriesRef, SeriesSamples)>> {
        let mut window = self;
        window.seal_all_series();
        let HeadWindow { series, arena, .. } = window;
        let mut decoded = Vec::with_capacity(series.len());
        for (series, encoded) in series.into_entries() {
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

    /// Decodes one window into timestamp-ordered, last-write-wins series.
    ///
    /// This is the sealing decode for both in-order and OOO-only windows.
    /// Equal timestamps preserve their arrival order through the stable sort,
    /// then retain the final complete sample.
    pub fn into_deduped_series_samples(self) -> io::Result<Vec<(SeriesRef, SeriesSamples)>> {
        let start_ms = self.start_ms;
        let end_ms = self.end_ms;
        let mut decoded = self.into_series_samples()?;
        for (series, samples) in &mut decoded {
            validate_series_samples_range(*series, samples, start_ms, end_ms)?;
            sort_and_dedupe_series_samples(samples);
        }
        Ok(decoded)
    }

    /// Decodes an in-order window together with its co-resident OOO lane.
    ///
    /// The receiver has lower precedence than `ooo`: samples are combined in
    /// that order, stably sorted by timestamp, and the final value at an equal
    /// timestamp is retained. This preserves the complete winning typed value,
    /// including its OTLP metadata.
    pub fn into_series_samples_with_ooo(
        self,
        ooo: Option<HeadWindow>,
    ) -> io::Result<SealedHeadWindowSamples> {
        if self.out_of_order {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "in-order head window expected as merge receiver",
            ));
        }

        let Some(ooo) = ooo else {
            let series_samples = self.into_deduped_series_samples()?;
            let unique_series_count = series_samples.len();
            return Ok(SealedHeadWindowSamples {
                series_samples,
                unique_series_count,
            });
        };
        if !ooo.out_of_order {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "out-of-order head window expected as merge argument",
            ));
        }
        if (self.start_ms, self.end_ms) != (ooo.start_ms, ooo.end_ms) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "cannot merge head windows with different ranges: [{}, {}) and [{}, {})",
                    self.start_ms, self.end_ms, ooo.start_ms, ooo.end_ms
                ),
            ));
        }

        let mut earlier = self.into_deduped_series_samples()?;
        let later = ooo.into_deduped_series_samples()?;
        let mut later_by_series = SeriesRefHashMap::default();
        later_by_series
            .try_reserve(later.len())
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        for (series, samples) in later {
            if later_by_series.insert(series, samples).is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("OOO head window contains duplicate series {}", series.get()),
                ));
            }
        }

        // Size the temporary lookup by the sparse OOO side, not by the
        // potentially multi-million-series active window. The caller performs
        // the final metric-query ordering after this merge.
        let mut additional_kind_streams = Vec::new();
        for (series, samples) in &mut earlier {
            if let Some(later_samples) = later_by_series.remove(series)
                && let Some(additional) = merge_series_samples(samples, later_samples, *series)?
            {
                additional_kind_streams.push((*series, additional));
            }
        }
        earlier
            .try_reserve(
                later_by_series
                    .len()
                    .saturating_add(additional_kind_streams.len()),
            )
            .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
        earlier.extend(later_by_series);
        let unique_series_count = earlier.len();
        earlier.extend(additional_kind_streams);
        Ok(SealedHeadWindowSamples {
            series_samples: earlier,
            unique_series_count,
        })
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

    pub fn series_table_stats(&self) -> HeadSeriesTableStats {
        self.series.stats()
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
        for (series, encoded) in self.series.iter() {
            let value = bytes_fn(encoded) as u64;
            match encoded.kind() {
                SampleKind::Float => {
                    bytes.float = bytes.float.saturating_add(value);
                    match number_kind(series) {
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
                    match number_kind(series) {
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
        for (series, encoded) in self.series.iter() {
            let samples = encoded.samples_in_range(&self.arena, start_ms, end_ms)?;
            if !samples.is_empty() {
                decoded.push((series, samples));
            }
        }
        Ok(decoded)
    }
}

fn validate_series_samples_range(
    series: SeriesRef,
    samples: &SeriesSamples,
    start_ms: u64,
    end_ms: u64,
) -> io::Result<()> {
    let invalid_timestamp = match samples {
        SeriesSamples::Float { samples, .. } => samples.iter().find_map(|(timestamp_ms, _)| {
            (*timestamp_ms < start_ms || *timestamp_ms >= end_ms).then_some(*timestamp_ms)
        }),
        SeriesSamples::Int64 { samples, .. } => samples.iter().find_map(|(timestamp_ms, _)| {
            (*timestamp_ms < start_ms || *timestamp_ms >= end_ms).then_some(*timestamp_ms)
        }),
        SeriesSamples::Histogram { samples } => samples.iter().find_map(|(timestamp_ms, _)| {
            (*timestamp_ms < start_ms || *timestamp_ms >= end_ms).then_some(*timestamp_ms)
        }),
        SeriesSamples::ExponentialHistogram { samples } => {
            samples.iter().find_map(|(timestamp_ms, _)| {
                (*timestamp_ms < start_ms || *timestamp_ms >= end_ms).then_some(*timestamp_ms)
            })
        }
        SeriesSamples::Summary { samples } => samples.iter().find_map(|(timestamp_ms, _)| {
            (*timestamp_ms < start_ms || *timestamp_ms >= end_ms).then_some(*timestamp_ms)
        }),
    };
    if let Some(timestamp_ms) = invalid_timestamp {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "head series {} timestamp {timestamp_ms} falls outside window [{start_ms}, {end_ms})",
                series.get()
            ),
        ));
    }
    Ok(())
}

fn merge_series_samples(
    earlier: &mut SeriesSamples,
    later: SeriesSamples,
    series: SeriesRef,
) -> io::Result<Option<SeriesSamples>> {
    match (&mut *earlier, later) {
        (
            SeriesSamples::Float { encoding, samples },
            SeriesSamples::Float {
                encoding: later_encoding,
                samples: later_samples,
            },
        ) if *encoding == later_encoding => samples.extend(later_samples),
        (
            SeriesSamples::Int64 { encoding, samples },
            SeriesSamples::Int64 {
                encoding: later_encoding,
                samples: later_samples,
            },
        ) if *encoding == later_encoding => samples.extend(later_samples),
        (
            SeriesSamples::Float { samples, .. },
            SeriesSamples::Int64 {
                samples: later_samples,
                ..
            },
        ) => samples.extend(
            later_samples
                .into_iter()
                .map(|(timestamp_ms, value)| (timestamp_ms, value as f64)),
        ),
        (
            earlier @ SeriesSamples::Int64 { .. },
            SeriesSamples::Float {
                encoding,
                samples: later_samples,
            },
        ) => {
            let SeriesSamples::Int64 {
                samples: earlier_samples,
                ..
            } = std::mem::replace(
                earlier,
                SeriesSamples::Float {
                    encoding,
                    samples: Vec::new(),
                },
            )
            else {
                unreachable!("the match arm established an Int64 series");
            };
            let SeriesSamples::Float { samples, .. } = earlier else {
                unreachable!("the replacement installed a Float series");
            };
            samples
                .try_reserve(earlier_samples.len().saturating_add(later_samples.len()))
                .map_err(|error| io::Error::new(io::ErrorKind::OutOfMemory, error))?;
            samples.extend(
                earlier_samples
                    .into_iter()
                    .map(|(timestamp_ms, value)| (timestamp_ms, value as f64)),
            );
            samples.extend(later_samples);
        }
        (
            SeriesSamples::Histogram { samples },
            SeriesSamples::Histogram {
                samples: later_samples,
            },
        ) => samples.extend(later_samples),
        (
            SeriesSamples::ExponentialHistogram { samples },
            SeriesSamples::ExponentialHistogram {
                samples: later_samples,
            },
        ) => samples.extend(later_samples),
        (
            SeriesSamples::Summary { samples },
            SeriesSamples::Summary {
                samples: later_samples,
            },
        ) => samples.extend(later_samples),
        (
            SeriesSamples::Float {
                encoding,
                samples: _,
            },
            SeriesSamples::Float {
                encoding: later_encoding,
                samples: _,
            },
        ) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "head series {} float encoding mismatch while merging {encoding:?} with {later_encoding:?}",
                    series.get(),
                ),
            ));
        }
        (
            SeriesSamples::Int64 {
                encoding,
                samples: _,
            },
            SeriesSamples::Int64 {
                encoding: later_encoding,
                samples: _,
            },
        ) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "head series {} int64 encoding mismatch while merging {encoding:?} with {later_encoding:?}",
                    series.get(),
                ),
            ));
        }
        (_, later) => return Ok(Some(later)),
    }
    sort_and_dedupe_series_samples(earlier);
    Ok(None)
}

fn sort_and_dedupe_series_samples(samples: &mut SeriesSamples) {
    match samples {
        SeriesSamples::Float { samples, .. } => sort_and_dedupe_last(samples),
        SeriesSamples::Int64 { samples, .. } => sort_and_dedupe_last(samples),
        SeriesSamples::Histogram { samples } => sort_and_dedupe_last(samples),
        SeriesSamples::ExponentialHistogram { samples } => sort_and_dedupe_last(samples),
        SeriesSamples::Summary { samples } => sort_and_dedupe_last(samples),
    }
}

fn sort_and_dedupe_last<T>(samples: &mut Vec<(u64, T)>) {
    if samples.len() < 2 || samples.windows(2).all(|pair| pair[0].0 < pair[1].0) {
        return;
    }
    if samples.windows(2).any(|pair| pair[0].0 > pair[1].0) {
        samples.sort_by_key(|(timestamp_ms, _)| *timestamp_ms);
    }
    samples.reverse();
    samples.dedup_by_key(|(timestamp_ms, _)| *timestamp_ms);
    samples.reverse();
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
        let mut all_series: Vec<_> = window.series.keys().collect();
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
        let compiled_matchers = compile_label_matchers(matchers)?;
        let mut candidates: Option<Vec<SeriesRef>> = None;
        for (matcher, compiled) in matchers.iter().zip(&compiled_matchers) {
            if compiled.requires_missing_label_scan() {
                continue;
            }
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
                    Some(existing) => intersect_sorted(&existing, &positive),
                    None => positive,
                });
            }
        }

        let mut candidate_refs = candidates.unwrap_or_else(|| self.all_series.clone());
        for (matcher, compiled) in matchers.iter().zip(&compiled_matchers) {
            if compiled.requires_missing_label_scan() {
                continue;
            }
            match matcher {
                NormalizedMatcher::NotEq { name, value } => {
                    let posting = self.exact_postings(name, value);
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::NotRegex { name, pattern } => {
                    let posting = self.regex_postings(name, pattern, budget, false)?;
                    if !posting.is_empty() {
                        candidate_refs = subtract_sorted(&candidate_refs, &posting);
                    }
                }
                NormalizedMatcher::Eq { .. } | NormalizedMatcher::Regex { .. } => {}
            }
        }

        if compiled_matchers
            .iter()
            .any(CompiledLabelMatcher::requires_missing_label_scan)
        {
            candidate_refs.retain(|series_ref| {
                let Some(indexed) = self.series.get(series_ref) else {
                    return false;
                };
                compiled_matchers
                    .iter()
                    .filter(|matcher| matcher.requires_missing_label_scan())
                    .all(|matcher| {
                        let value = indexed
                            .labels
                            .iter()
                            .find_map(|(name, value)| {
                                (name == matcher.name()).then_some(value.as_str())
                            })
                            .unwrap_or("");
                        matcher.matches_value(value, match_promql_projection_names)
                    })
            });
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
                out = union_sorted(&out, posting);
            }
        }

        Ok(out)
    }
}
