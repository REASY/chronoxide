use super::*;

pub struct HeadBuffer {
    pub(super) config: HeadConfig,
    pub(super) window: Option<HeadWindow>,
    pub(super) ooo_windows: BTreeMap<(u64, u64), HeadWindow>,
    pub(super) last_timestamps: HashMap<SeriesRef, u64>,
    pub(super) selector_index: Mutex<Option<CachedHeadSelectorIndex>>,
}

impl HeadBuffer {
    pub fn new(config: HeadConfig) -> io::Result<Self> {
        let _ = Self::window_duration_ms(&config)?;
        let _ = Self::out_of_order_time_window_ms(&config)?;
        Self::validate_block_size(&config)?;
        Ok(Self {
            config,
            window: None,
            ooo_windows: BTreeMap::new(),
            last_timestamps: HashMap::new(),
            selector_index: Mutex::new(None),
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
            self.validate_sample_timestamp(series, *ts)?;
            let (start_ms, end_ms) = window_for(*ts, duration_ms);
            let route_to_ooo = self.should_route_to_ooo_window(series, *ts);

            let accepted = if route_to_ooo {
                let window = self
                    .ooo_windows
                    .entry((start_ms, end_ms))
                    .or_insert_with(|| HeadWindow::new(start_ms, end_ms));
                Self::push_sample_to_window(&self.config, window, series, *ts, value)?
            } else {
                let rotate = match &self.window {
                    None => true,
                    Some(window) => *ts >= window.end_ms,
                };

                if rotate {
                    if let Some(mut window) = self.window.take() {
                        window.seal_all_series();
                        flushed.push(window);
                    }
                    self.window = Some(HeadWindow::new(start_ms, end_ms));
                }

                let Some(window) = self.window.as_mut() else {
                    continue;
                };
                Self::push_sample_to_window(&self.config, window, series, *ts, value)?
            };

            if accepted {
                self.record_accepted_timestamp(series, *ts);
                self.clear_selector_index_cache();
            }
        }

        Ok(flushed)
    }

    pub fn drain(&mut self) -> Option<HeadWindow> {
        self.clear_selector_index_cache();
        if let Some(mut window) = self.window.take() {
            window.seal_all_series();
            Some(window)
        } else {
            None
        }
    }

    pub fn drain_windows(&mut self) -> Vec<HeadWindow> {
        self.clear_selector_index_cache();
        let mut windows = Vec::new();
        for (_range, mut window) in std::mem::take(&mut self.ooo_windows) {
            window.seal_all_series();
            windows.push(window);
        }
        if let Some(mut window) = self.window.take() {
            window.seal_all_series();
            windows.push(window);
        }
        windows.sort_by_key(|window| (window.start_ms, window.end_ms));
        windows
    }

    pub fn window_range(&self) -> Option<(u64, u64)> {
        self.window.as_ref().map(|w| (w.start_ms, w.end_ms))
    }

    pub fn query_selector<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::unlimited();
        self.query_selector_with_budget(labels, selector, start_ms, end_ms, &mut budget)
    }

    pub(crate) fn query_selector_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let matchers = selector.normalized_matchers();
        let mut results = Vec::new();
        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            results.extend(self.query_window_selector_with_budget(
                labels,
                window,
                &matchers,
                selector.projection(),
                start_ms,
                end_ms,
                budget,
            )?);
        }

        Ok(merge_head_query_results(results))
    }

    pub(crate) fn query_native_histogram_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let matchers = selector.normalized_matchers();
        let mut results = Vec::new();
        let range_end_ms = end_ms.saturating_add(1);
        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            let index = self.selector_index(labels, window)?;
            let candidate_series = index.matching_series(&matchers, budget, false)?;

            for series in candidate_series {
                let Some(encoded) = window.series.get(&series) else {
                    continue;
                };
                if encoded.kind() != SampleKind::Histogram {
                    continue;
                }
                let Some(indexed) = index.series(&series) else {
                    continue;
                };
                budget.observe_matched_series(indexed.series_id)?;

                let SeriesSamples::Histogram { samples } =
                    encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?
                else {
                    continue;
                };
                budget.observe_samples_decoded(samples.len() as u64)?;
                if samples.is_empty() {
                    continue;
                }

                let mut result = PromqlHistogramSeries::new(
                    indexed.series_id,
                    shared_query_labels(indexed.labels.clone()),
                );
                for (timestamp_ms, value) in samples {
                    if timestamp_ms < start_ms || timestamp_ms > end_ms {
                        continue;
                    }
                    result.push_sample(PromqlHistogramSample::from_histogram_value(
                        timestamp_ms,
                        value,
                    ));
                }
                if !result.samples.is_empty() {
                    budget.observe_projected_series(result.series_id)?;
                    results.push(result);
                }
            }
        }

        Ok(merge_histogram_query_results(results))
    }

    pub(crate) fn query_native_exponential_histogram_with_budget<R>(
        &self,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<PromqlExponentialHistogramSeries>>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let matchers = selector.normalized_matchers();
        let mut results = Vec::new();
        let range_end_ms = end_ms.saturating_add(1);
        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            let index = self.selector_index(labels, window)?;
            let candidate_series = index.matching_series(&matchers, budget, false)?;

            for series in candidate_series {
                let Some(encoded) = window.series.get(&series) else {
                    continue;
                };
                if encoded.kind() != SampleKind::ExponentialHistogram {
                    continue;
                }
                let Some(indexed) = index.series(&series) else {
                    continue;
                };
                budget.observe_matched_series(indexed.series_id)?;

                let SeriesSamples::ExponentialHistogram { samples } =
                    encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?
                else {
                    continue;
                };
                budget.observe_samples_decoded(samples.len() as u64)?;
                if samples.is_empty() {
                    continue;
                }

                let mut result = PromqlExponentialHistogramSeries::new(
                    indexed.series_id,
                    shared_query_labels(indexed.labels.clone()),
                );
                for (timestamp_ms, value) in samples {
                    if timestamp_ms < start_ms || timestamp_ms > end_ms {
                        continue;
                    }
                    result.push_sample(
                        PromqlExponentialHistogramSample::from_exponential_histogram_value(
                            timestamp_ms,
                            value,
                        ),
                    );
                }
                if !result.samples.is_empty() {
                    budget.observe_projected_series(result.series_id)?;
                    results.push(result);
                }
            }
        }

        Ok(merge_exponential_histogram_query_results(results))
    }

    pub(super) fn query_window_selector_with_budget<R>(
        &self,
        labels: &R,
        window: &HeadWindow,
        matchers: &[NormalizedMatcher],
        projection: &SegmentProjection,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        let index = self.selector_index(labels, window)?;
        let candidate_series = index.matching_series(
            &matchers,
            budget,
            projection_matches_promql_metric_name_regex(projection),
        )?;
        let projected_label_filter = match projection {
            SegmentProjection::AllPromql { .. } => Some(compile_label_matchers(matchers)?),
            SegmentProjection::None
            | SegmentProjection::Count
            | SegmentProjection::Sum
            | SegmentProjection::HistogramBucket { .. }
            | SegmentProjection::NativeHistogram
            | SegmentProjection::NativeExponentialHistogram
            | SegmentProjection::SummaryQuantile { .. } => None,
        };
        let mut results = Vec::new();
        let range_end_ms = end_ms.saturating_add(1);

        for series in candidate_series {
            let Some(encoded) = window.series.get(&series) else {
                continue;
            };
            if !sample_kind_matches_projection(projection, encoded.kind()) {
                continue;
            }
            let Some(indexed) = index.series(&series) else {
                continue;
            };
            budget.observe_matched_series(indexed.series_id)?;

            let samples = encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?;
            match (projection, samples) {
                (
                    SegmentProjection::None | SegmentProjection::AllPromql { .. },
                    SeriesSamples::Float { samples, .. },
                ) => {
                    budget.observe_samples_decoded(samples.len() as u64)?;
                    if samples.is_empty() {
                        continue;
                    }
                    if projected_label_filter
                        .as_ref()
                        .is_some_and(|filter| !labels_match_compiled(&indexed.labels, filter))
                    {
                        continue;
                    }

                    results.push(SegmentQueryResult::with_samples(
                        indexed.series_id,
                        indexed.labels.clone(),
                        samples,
                    ));
                }
                (
                    SegmentProjection::None | SegmentProjection::AllPromql { .. },
                    SeriesSamples::Int64 { samples, .. },
                ) => {
                    budget.observe_samples_decoded(samples.len() as u64)?;
                    if samples.is_empty() {
                        continue;
                    }
                    if projected_label_filter
                        .as_ref()
                        .is_some_and(|filter| !labels_match_compiled(&indexed.labels, filter))
                    {
                        continue;
                    }

                    results.push(SegmentQueryResult::with_samples(
                        indexed.series_id,
                        indexed.labels.clone(),
                        samples
                            .into_iter()
                            .map(|(timestamp_ms, value)| (timestamp_ms, value as f64))
                            .collect(),
                    ));
                }
                (SegmentProjection::None, _) => {}
                (projection, samples) => {
                    let decoded_count = series_samples_len(&samples);
                    let mut projected = project_head_series_samples(
                        projection,
                        &indexed.labels,
                        samples,
                        start_ms,
                        end_ms,
                    );
                    budget.observe_samples_decoded(decoded_count as u64)?;
                    if let Some(filter) = &projected_label_filter {
                        projected.retain(|result| labels_match_compiled(&result.labels, filter));
                    }
                    results.append(&mut projected);
                }
            }
        }

        results.sort_by_key(|result| result.series_id);
        budget.observe_projected_results(&results)?;
        Ok(results)
    }

    pub fn metric_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names<R>(&self, labels: &R, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values<R>(
        &self,
        labels: &R,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        let label_name = if label_name == METRIC_NAME_LABEL {
            METRIC_NAME_LABEL.to_string()
        } else {
            crate::promql::normalize_label_name(label_name)
        };
        Ok(metadata.label_values(&label_name))
    }

    pub(crate) fn collect_metadata<R>(
        &self,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        if end_ms < start_ms {
            return Ok(());
        }

        for window in self.query_windows() {
            if !Self::window_overlaps_range(window, start_ms, end_ms) {
                continue;
            }
            Self::collect_window_metadata(labels, window, start_ms, end_ms, metadata)?;
        }

        Ok(())
    }

    pub(super) fn collect_window_metadata<R>(
        labels: &R,
        window: &HeadWindow,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()>
    where
        R: SeriesLabelResolver,
    {
        let range_end_ms = end_ms.saturating_add(1);
        for (series, encoded) in &window.series {
            let samples = encoded.samples_in_range(&window.arena, start_ms, range_end_ms)?;
            if samples.is_empty() {
                continue;
            }
            let Some((_, canonical_labels)) = canonical_head_labelset(labels, *series) else {
                continue;
            };
            metadata.add_labelset(&canonical_labels);
        }

        Ok(())
    }

    pub(super) fn query_windows(&self) -> Vec<&HeadWindow> {
        let mut windows: Vec<(u8, &HeadWindow)> = Vec::new();
        if let Some(window) = &self.window {
            windows.push((0, window));
        }
        for window in self.ooo_windows.values() {
            windows.push((1, window));
        }
        windows.sort_by_key(|(lane_precedence, window)| {
            (window.start_ms, window.end_ms, *lane_precedence)
        });
        windows.into_iter().map(|(_, window)| window).collect()
    }

    pub(super) fn window_overlaps_range(window: &HeadWindow, start_ms: u64, end_ms: u64) -> bool {
        window.end_ms > start_ms && window.start_ms <= end_ms
    }

    pub(super) fn selector_index<R>(
        &self,
        labels: &R,
        window: &HeadWindow,
    ) -> io::Result<HeadSelectorIndex>
    where
        R: SeriesLabelResolver,
    {
        let key = HeadSelectorIndexKey::new(window, labels.len());
        {
            let cache = self
                .selector_index
                .lock()
                .map_err(|_| io::Error::other("head selector index cache lock poisoned"))?;
            if let Some(cached) = cache.as_ref()
                && cached.key == key
            {
                return Ok(cached.index.clone());
            }
        }

        let index = HeadSelectorIndex::build(window, labels)?;
        let mut cache = self
            .selector_index
            .lock()
            .map_err(|_| io::Error::other("head selector index cache lock poisoned"))?;
        *cache = Some(CachedHeadSelectorIndex {
            key,
            index: index.clone(),
        });
        Ok(index)
    }

    pub(super) fn clear_selector_index_cache(&mut self) {
        if let Ok(cache) = self.selector_index.get_mut() {
            *cache = None;
        }
    }

    pub(super) fn should_route_to_ooo_window(&self, series: SeriesRef, timestamp_ms: u64) -> bool {
        if self
            .last_timestamps
            .get(&series)
            .is_some_and(|last_timestamp_ms| timestamp_ms < *last_timestamp_ms)
        {
            return true;
        }
        self.window
            .as_ref()
            .is_some_and(|window| timestamp_ms < window.start_ms)
    }

    pub(super) fn push_sample_to_window(
        config: &HeadConfig,
        window: &mut HeadWindow,
        series: SeriesRef,
        timestamp_ms: u64,
        value: &SampleValue,
    ) -> io::Result<bool> {
        let base_ms = window.start_ms;
        let block_size = config.block_size;
        let encoding = match value.kind() {
            SampleKind::Float => SeriesEncoding::Float(config.float_encoding),
            SampleKind::Int64 => SeriesEncoding::Int(config.int_encoding),
            SampleKind::Histogram => SeriesEncoding::Histogram(config.varlen_encoding),
            SampleKind::ExponentialHistogram => {
                SeriesEncoding::ExponentialHistogram(config.varlen_encoding)
            }
            SampleKind::Summary => SeriesEncoding::Summary(config.varlen_encoding),
        };
        match window.series.entry(series) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                let mut encoded = EncodedSeries::new(encoding);
                encoded.push_sample(
                    series,
                    base_ms,
                    timestamp_ms,
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
                    return Ok(false);
                }
                entry.get_mut().push_sample(
                    series,
                    base_ms,
                    timestamp_ms,
                    value.clone(),
                    block_size,
                    &mut window.arena,
                )?;
            }
        }
        window.datapoints = window.datapoints.saturating_add(1);
        Ok(true)
    }

    pub(super) fn window_duration_ms(config: &HeadConfig) -> io::Result<u64> {
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

    pub(super) fn out_of_order_time_window_ms(config: &HeadConfig) -> io::Result<u64> {
        let ms = config.out_of_order_time_window.as_millis();
        if ms > u64::MAX as u128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "out_of_order_time_window is too large",
            ));
        }
        Ok(ms as u64)
    }

    pub(super) fn validate_sample_timestamp(
        &self,
        series: SeriesRef,
        timestamp_ms: u64,
    ) -> io::Result<()> {
        let Some(last_timestamp_ms) = self.last_timestamps.get(&series).copied() else {
            return Ok(());
        };
        if timestamp_ms >= last_timestamp_ms {
            return Ok(());
        }

        let window_ms = Self::out_of_order_time_window_ms(&self.config)?;
        let lower_bound_ms = last_timestamp_ms.saturating_sub(window_ms);
        if timestamp_ms < lower_bound_ms {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "sample is outside out_of_order_time_window",
            ));
        }
        Ok(())
    }

    pub(super) fn record_accepted_timestamp(&mut self, series: SeriesRef, timestamp_ms: u64) {
        self.last_timestamps
            .entry(series)
            .and_modify(|last| *last = (*last).max(timestamp_ms))
            .or_insert(timestamp_ms);
    }

    pub(super) fn validate_block_size(config: &HeadConfig) -> io::Result<()> {
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

pub(super) fn canonical_head_labelset<R>(
    labels: &R,
    series: SeriesRef,
) -> Option<(u64, Vec<(String, String)>)>
where
    R: SeriesLabelResolver,
{
    if series.get() as usize >= labels.len() {
        return None;
    }

    let mut metric_name = String::new();
    let mut attributes = Vec::new();
    labels.visit_labelset(series, &mut |key, value| {
        if key == METRIC_NAME_LABEL {
            metric_name = value.to_string();
        } else {
            attributes.push((key.to_string(), value.to_string()));
        }
    });

    let attribute_refs: Vec<(&str, &str)> = attributes
        .iter()
        .map(|(key, value)| (key.as_str(), value.as_str()))
        .collect();
    let canonical = canonicalize_labelset(&metric_name, &attribute_refs);
    let id = series_id(&canonical);
    let labels = canonical
        .labels()
        .iter()
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect();

    Some((id, labels))
}

pub(super) fn intersect_series_refs(left: &[SeriesRef], right: &[SeriesRef]) -> Vec<SeriesRef> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() && ri < right.len() {
        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => li += 1,
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

pub(super) fn union_series_refs(left: &[SeriesRef], right: &[SeriesRef]) -> Vec<SeriesRef> {
    let mut out = Vec::with_capacity(left.len().saturating_add(right.len()));
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() || ri < right.len() {
        if li >= left.len() {
            out.extend_from_slice(&right[ri..]);
            break;
        }
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(right[ri]);
                ri += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(left[li]);
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

pub(super) fn subtract_series_refs(left: &[SeriesRef], right: &[SeriesRef]) -> Vec<SeriesRef> {
    let mut out = Vec::new();
    let mut li = 0usize;
    let mut ri = 0usize;
    while li < left.len() {
        if ri >= right.len() {
            out.extend_from_slice(&left[li..]);
            break;
        }

        match left[li].cmp(&right[ri]) {
            std::cmp::Ordering::Less => {
                out.push(left[li]);
                li += 1;
            }
            std::cmp::Ordering::Greater => ri += 1,
            std::cmp::Ordering::Equal => {
                li += 1;
                ri += 1;
            }
        }
    }
    out
}

pub(super) fn merge_head_query_results(
    results: Vec<SegmentQueryResult>,
) -> Vec<SegmentQueryResult> {
    let mut merged: BTreeMap<u64, SegmentQueryResult> = BTreeMap::new();
    for result in results {
        let entry = merged.entry(result.series_id).or_insert_with(|| {
            SegmentQueryResult::with_shared_labels(result.series_id, result.labels.clone())
        });
        entry.extend_from(result);
    }

    let mut results: Vec<_> = merged.into_values().collect();
    for result in &mut results {
        result.dedupe_samples_keep_last();
    }
    results
}

pub(super) fn sample_kind_matches_projection(
    projection: &SegmentProjection,
    kind: SampleKind,
) -> bool {
    match projection {
        SegmentProjection::None => matches!(kind, SampleKind::Float | SampleKind::Int64),
        SegmentProjection::AllPromql { .. } => true,
        SegmentProjection::Count | SegmentProjection::Sum => matches!(
            kind,
            SampleKind::Histogram | SampleKind::ExponentialHistogram | SampleKind::Summary
        ),
        SegmentProjection::HistogramBucket { .. } => matches!(
            kind,
            SampleKind::Histogram | SampleKind::ExponentialHistogram
        ),
        SegmentProjection::NativeHistogram => kind == SampleKind::Histogram,
        SegmentProjection::NativeExponentialHistogram => kind == SampleKind::ExponentialHistogram,
        SegmentProjection::SummaryQuantile { .. } => kind == SampleKind::Summary,
    }
}
