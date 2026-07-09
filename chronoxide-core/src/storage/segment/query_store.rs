use super::*;

impl SegmentStoreReader {
    pub fn open(segments_dir: impl AsRef<Path>) -> io::Result<Self> {
        let mut segments = Vec::new();
        for entry in fs::read_dir(segments_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if !name.starts_with("seg-") {
                continue;
            }
            if SegmentId::parse_dir_name(&name).is_err() {
                continue;
            }
            segments.push(SegmentReader::open(entry.path())?);
        }

        sort_segment_readers(&mut segments);

        Ok(Self {
            segments,
            query_projection_config: QueryProjectionConfig::default(),
        })
    }

    pub fn open_with_query_projection_config(
        segments_dir: impl AsRef<Path>,
        query_projection_config: QueryProjectionConfig,
    ) -> io::Result<Self> {
        Ok(Self::open(segments_dir)?.with_query_projection_config(query_projection_config))
    }

    pub fn with_query_projection_config(
        mut self,
        query_projection_config: QueryProjectionConfig,
    ) -> Self {
        self.query_projection_config = query_projection_config;
        self
    }

    pub fn query_session(&self) -> io::Result<SegmentStoreQuerySession<'_>> {
        SegmentStoreQuerySession::open(self)
    }

    pub fn smoke_verify(
        &self,
        start_ms: u64,
        end_ms: u64,
        sample_limit_per_kind: usize,
    ) -> io::Result<SegmentStoreSmokeReport> {
        if end_ms < start_ms {
            return Ok(SegmentStoreSmokeReport::default());
        }

        let mut report = SegmentStoreSmokeReport::default();
        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }

            report.totals.segments = report.totals.segments.saturating_add(1);
            report.totals.datapoints = report
                .totals
                .datapoints
                .saturating_add(segment.meta.datapoints);
            report.totals.series = report.totals.series.saturating_add(segment.meta.series);
            let summary_covers_requested_range =
                start_ms <= segment.meta.start_ms && segment.meta.end_ms <= end_ms;
            let collect_totals = if summary_covers_requested_range {
                if let Some(summary) = &segment.meta.chunk_summary {
                    report.totals.add_chunk_summary(summary);
                    false
                } else {
                    true
                }
            } else {
                true
            };
            segment.collect_smoke_report(
                start_ms,
                end_ms,
                sample_limit_per_kind,
                collect_totals,
                &mut report,
            )?;
        }

        let queries = report
            .sample_series
            .iter()
            .flat_map(|sample| smoke_queries_for_sample(sample, start_ms, end_ms))
            .collect::<Vec<_>>();
        if queries.is_empty() {
            return Ok(report);
        }
        let mut query_session = self.query_session()?;
        for (kind, query, query_start_ms, query_end_ms) in queries {
            let execution = query_session
                .query_promql_with_limits(
                    &query,
                    query_start_ms,
                    query_end_ms,
                    smoke_query_limits(),
                )
                .map_err(|err| smoke_query_error(&query, err))?;
            let result_series = execution.results.len() as u64;
            let result_samples = execution
                .results
                .iter()
                .map(|result| result.samples.len() as u64)
                .sum::<u64>();
            if result_samples == 0 {
                return Err(io::Error::other(format!(
                    "smoke query returned no samples: {query}"
                )));
            }
            report.queries.push(SegmentStoreSmokeQuery {
                kind,
                query,
                result_series,
                result_samples,
                matched_series: execution.stats.matched_series,
                projected_series: execution.stats.projected_series,
                chunk_reads: execution.stats.chunk_reads,
                bytes_read: execution.stats.bytes_read,
                samples_decoded: execution.stats.samples_decoded,
                typed_scalar_chunks_decoded: execution.stats.typed_scalar_chunks_decoded,
                typed_full_chunks_decoded: execution.stats.typed_full_chunks_decoded,
            });
        }

        Ok(report)
    }

    pub fn open_manifest_published(
        segments_dir: impl AsRef<Path>,
        manifest_dir: impl AsRef<Path>,
    ) -> io::Result<Self> {
        Self::open_manifest_published_with_options(
            segments_dir,
            manifest_dir,
            SegmentStoreOpenOptions::default(),
        )
    }

    pub fn open_manifest_published_with_options(
        segments_dir: impl AsRef<Path>,
        manifest_dir: impl AsRef<Path>,
        options: SegmentStoreOpenOptions,
    ) -> io::Result<Self> {
        let Some(inventory) = read_manifest_inventory(manifest_dir)? else {
            return Ok(Self {
                segments: Vec::new(),
                query_projection_config: QueryProjectionConfig::default(),
            });
        };
        Self::open_manifest_inventory_with_options(segments_dir, &inventory, options)
    }

    pub fn open_manifest_inventory(
        segments_dir: impl AsRef<Path>,
        inventory: &ManifestInventory,
    ) -> io::Result<Self> {
        Self::open_manifest_inventory_with_options(
            segments_dir,
            inventory,
            SegmentStoreOpenOptions::default(),
        )
    }

    pub fn open_manifest_inventory_with_options(
        segments_dir: impl AsRef<Path>,
        inventory: &ManifestInventory,
        options: SegmentStoreOpenOptions,
    ) -> io::Result<Self> {
        let segments_dir = segments_dir.as_ref();
        let mut segments = Vec::with_capacity(inventory.segments.len());

        for manifest_segment in &inventory.segments {
            let parsed =
                SegmentId::parse_dir_name(&manifest_segment.segment_id).map_err(|err| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("invalid manifest segment id: {err}"),
                    )
                })?;
            if parsed.start_ms() != manifest_segment.start_ms
                || parsed.end_ms() != manifest_segment.end_ms
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "manifest segment id range does not match inventory range",
                ));
            }

            let segment_dir = segments_dir.join(&manifest_segment.segment_id);
            let reader = if options.validate_segment_footers {
                SegmentReader::open_validated(segment_dir)?
            } else {
                SegmentReader::open(segment_dir)?
            };
            validate_manifest_segment_meta(manifest_segment, reader.meta())?;
            segments.push(reader);
        }

        sort_segment_readers(&mut segments);
        Ok(Self {
            segments,
            query_projection_config: QueryProjectionConfig::default(),
        })
    }

    pub fn query_exact(
        &self,
        matchers: &[(&str, &str)],
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }

            results.extend(segment.query_exact(matchers, start_ms, end_ms)?);
        }

        Ok(merge_query_results(results))
    }

    pub fn query_selector(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        self.query_selector_with_limits(selector, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_selector_with_limits(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let results = self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
        Ok(QueryExecution {
            results,
            stats: budget.stats(),
        })
    }

    pub(super) fn query_selectors_with_limits(
        &self,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        let mut seen_branches = BTreeMap::new();
        for selector in selectors {
            let selector_results =
                self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
            observe_promql_selector_branch_conflicts(
                &mut seen_branches,
                selector,
                &selector_results,
            )?;
            results.extend(selector_results);
        }
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub(super) fn query_native_histogram_selector_with_limits(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlHistogramSeries>, QueryStats), PromqlQueryError> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        for segment in &self.segments {
            budget.observe_segment_considered();
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_histogram_with_budget(selector, start_ms, end_ms, &mut budget)
                    .map_err(promql_error_from_query_io)?,
            );
        }

        Ok((merge_histogram_query_results(results), budget.stats()))
    }

    pub(super) fn query_native_exponential_histogram_selector_with_limits(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlExponentialHistogramSeries>, QueryStats), PromqlQueryError> {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        for segment in &self.segments {
            budget.observe_segment_considered();
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_exponential_histogram_with_budget(
                        selector,
                        start_ms,
                        end_ms,
                        &mut budget,
                    )
                    .map_err(promql_error_from_query_io)?,
            );
        }

        Ok((
            merge_exponential_histogram_query_results(results),
            budget.stats(),
        ))
    }

    pub(super) fn query_native_histogram_selector_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlHistogramSeries>, QueryStats), PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        for segment in &self.segments {
            budget.observe_segment_considered();
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_histogram_with_budget(selector, start_ms, end_ms, &mut budget)
                    .map_err(promql_error_from_query_io)?,
            );
        }
        results.extend(
            head.query_native_histogram_with_budget(
                labels,
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )
            .map_err(promql_error_from_query_io)?,
        );

        Ok((merge_histogram_query_results(results), budget.stats()))
    }

    pub(super) fn query_native_exponential_histogram_selector_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(Vec<PromqlExponentialHistogramSeries>, QueryStats), PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        if end_ms < start_ms {
            return Ok((results, budget.stats()));
        }

        for segment in &self.segments {
            budget.observe_segment_considered();
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }
            results.extend(
                segment
                    .query_native_exponential_histogram_with_budget(
                        selector,
                        start_ms,
                        end_ms,
                        &mut budget,
                    )
                    .map_err(promql_error_from_query_io)?,
            );
        }
        results.extend(
            head.query_native_exponential_histogram_with_budget(
                labels,
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )
            .map_err(promql_error_from_query_io)?,
        );

        Ok((
            merge_exponential_histogram_query_results(results),
            budget.stats(),
        ))
    }

    pub fn query_selector_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<SegmentQueryResult>>
    where
        R: SeriesLabelResolver,
    {
        self.query_selector_with_head_with_limits(
            head,
            labels,
            selector,
            start_ms,
            end_ms,
            QueryLimits::unlimited(),
        )
        .map(|execution| execution.results)
    }

    pub fn query_selector_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::new(limits);
        let mut results =
            self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
        results.extend(head.query_selector_with_budget(
            labels,
            selector,
            start_ms,
            end_ms,
            &mut budget,
        )?);
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub(super) fn query_selectors_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        selectors: &[SegmentSelector],
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> io::Result<QueryExecution>
    where
        R: SeriesLabelResolver,
    {
        let mut budget = QueryBudget::new(limits);
        let mut results = Vec::new();
        let mut seen_branches = BTreeMap::new();
        for selector in selectors {
            let mut selector_results =
                self.query_selector_with_budget(selector, start_ms, end_ms, &mut budget)?;
            selector_results.extend(head.query_selector_with_budget(
                labels,
                selector,
                start_ms,
                end_ms,
                &mut budget,
            )?);
            observe_promql_selector_branch_conflicts(
                &mut seen_branches,
                selector,
                &selector_results,
            )?;
            results.extend(selector_results);
        }
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats: budget.stats(),
        })
    }

    pub fn query_promql(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_with_limits(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_query(&query, start_ms, end_ms, limits)
    }

    pub fn query_promql_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let query = parse_query(query)?;
        self.execute_promql_query_with_head(
            head,
            labels,
            &query,
            start_ms,
            end_ms,
            QueryLimits::unlimited(),
        )
        .map(|execution| execution.results)
    }

    pub fn query_promql_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let query = parse_query(query)?;
        self.execute_promql_query_with_head(head, labels, &query, start_ms, end_ms, limits)
    }

    pub(super) fn execute_promql_query(
        &self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation(
                            aggregation,
                            end_ms,
                            limits,
                        )?
                {
                    return Ok(execution);
                }
                let mut execution =
                    self.execute_promql_instant_query(&aggregation.input, end_ms, limits)?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_instant_query(&absent.input, end_ms, limits)?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.execute_promql_histogram_fraction(function, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.execute_promql_histogram_scalar_function(function, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.execute_promql_histogram_quantile(function, end_ms, limits)
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.execute_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    fn execute_promql_instant_query(
        &self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation(
                            aggregation,
                            end_ms,
                            limits,
                        )?
                {
                    return Ok(execution);
                }
                let mut execution =
                    self.execute_promql_instant_query(&aggregation.input, end_ms, limits)?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_instant_query(&absent.input, end_ms, limits)?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => {
                self.execute_promql_histogram_fraction(function, end_ms, limits)
            }
            PromqlQuery::HistogramScalarFunction(function) => {
                self.execute_promql_histogram_scalar_function(function, end_ms, limits)
            }
            PromqlQuery::HistogramQuantile(function) => {
                self.execute_promql_histogram_quantile(function, end_ms, limits)
            }
            PromqlQuery::BinaryExpression(expression) => {
                self.execute_promql_binary_expression(expression, end_ms, limits)
            }
        }
    }

    fn execute_promql_float_only_instant_query(
        &self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_float_selectors_from_promql(selector.clone())?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_selectors_with_limits(&selectors, start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                let mut execution = self.execute_promql_float_only_instant_query(
                    &aggregation.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&absent.input, end_ms, limits)?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::BinaryExpression(_) => Ok(QueryExecution {
                results: Vec::new(),
                stats: QueryStats::default(),
            }),
        }
    }

    fn execute_promql_histogram_fraction(
        &self,
        function: &PromqlHistogramFraction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&function.input, end_ms, limits)?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_fraction(function, series, end_ms));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_fraction(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_histogram_quantile(
        &self,
        function: &PromqlHistogramQuantile,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&function.input, end_ms, limits)?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_quantile(function, series, end_ms));
            }
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_quantile(
                    function, series, end_ms,
                ));
            }
        }

        if saw_native_input {
            let mut classic_execution =
                self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
            stats.merge_from(classic_execution.stats);
            stats.check_limits(limits)?;
            classic_execution.results =
                evaluate_histogram_quantile(function, classic_execution.results, end_ms);
            results.extend(classic_execution.results);
            return Ok(QueryExecution {
                results: merge_query_results(results),
                stats,
            });
        }

        let mut execution = self.execute_promql_instant_query(&function.input, end_ms, limits)?;
        execution.results = evaluate_histogram_quantile(function, execution.results, end_ms);
        Ok(execution)
    }

    fn execute_promql_histogram_scalar_function(
        &self,
        function: &PromqlHistogramScalarFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&function.input, end_ms, limits)?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_scalar_function(
                function, series, end_ms,
            ));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &function.input,
                end_ms,
                limits,
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_scalar_function(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_native_histogram_scalar_aggregation(
        &self,
        aggregation: &PromqlAggregation,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        let mut histogram_series = Vec::new();
        let mut exponential_histogram_series = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) =
            self.execute_promql_native_histogram_instant_query(&aggregation.input, end_ms, limits)?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                histogram_series = series;
            }
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query(
                &aggregation.input,
                end_ms,
                limits,
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                exponential_histogram_series = series;
            }
        }

        if !saw_native_input {
            return Ok(None);
        }
        let scalar_execution =
            self.execute_promql_float_only_instant_query(&aggregation.input, end_ms, limits)?;
        stats.merge_from(scalar_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_native_histogram_scalar_aggregation(
            aggregation,
            scalar_execution.results,
            histogram_series,
            exponential_histogram_series,
            end_ms,
        );
        Ok(Some(QueryExecution { results, stats }))
    }

    fn execute_promql_binary_expression(
        &self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        if binary_operator_is_set(expression.op) {
            if scalar_expression_value(&expression.left).is_some()
                || scalar_expression_value(&expression.right).is_some()
            {
                return Err(PromqlQueryError::Unsupported(
                    "set binary operators require instant-vector operands".to_string(),
                ));
            }

            let left_execution =
                self.execute_promql_instant_query(&expression.left, end_ms, limits)?;
            let right_execution =
                self.execute_promql_instant_query(&expression.right, end_ms, limits)?;
            let mut stats = left_execution.stats;
            stats.merge_from(right_execution.stats);
            stats.check_limits(limits)?;
            let results = evaluate_binary_vector_set(
                expression,
                left_execution.results,
                right_execution.results,
                end_ms,
            )?;
            return Ok(QueryExecution { results, stats });
        }

        if let Some(left) = scalar_expression_value(&expression.left) {
            if let Some(right) = scalar_expression_value(&expression.right) {
                return Ok(QueryExecution {
                    results: evaluate_binary_scalar_scalar(expression.op, left, right, end_ms),
                    stats: QueryStats::default(),
                });
            }

            let mut execution =
                self.execute_promql_instant_query(&expression.right, end_ms, limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, left, true, end_ms);
            return Ok(execution);
        }

        if let Some(right) = scalar_expression_value(&expression.right) {
            let mut execution =
                self.execute_promql_instant_query(&expression.left, end_ms, limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, right, false, end_ms);
            return Ok(execution);
        }

        let left_execution = self.execute_promql_instant_query(&expression.left, end_ms, limits)?;
        let right_execution =
            self.execute_promql_instant_query(&expression.right, end_ms, limits)?;
        let mut stats = left_execution.stats;
        stats.merge_from(right_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_binary_vector_vector(
            expression,
            left_execution.results,
            right_execution.results,
            end_ms,
        )?;
        Ok(QueryExecution { results, stats })
    }

    fn execute_promql_native_histogram_instant_query(
        &self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) = native_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_histogram_selector_with_limits(
                    &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self.query_native_histogram_selector_with_limits(
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
                Ok(Some((
                    evaluate_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self.execute_promql_native_histogram_instant_query(
                    &aggregation.input,
                    end_ms,
                    limits,
                )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::BinaryExpression(_) => Ok(None),
        }
    }

    fn execute_promql_native_exponential_histogram_instant_query(
        &self,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlExponentialHistogramSeries>, QueryStats)>, PromqlQueryError> {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_exponential_histogram_selector_with_limits(
                    &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self
                    .query_native_exponential_histogram_selector_with_limits(
                        &selector,
                        range_start_ms,
                        end_ms,
                        limits,
                    )?;
                Ok(Some((
                    evaluate_exponential_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query(
                        &aggregation.input,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_exponential_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::BinaryExpression(_) => Ok(None),
        }
    }

    fn execute_promql_native_histogram_instant_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlHistogramSeries>, QueryStats)>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) = native_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_histogram_selector_with_head_with_limits(
                    head, labels, &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self.query_native_histogram_selector_with_head_with_limits(
                    head,
                    labels,
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
                Ok(Some((
                    evaluate_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self
                    .execute_promql_native_histogram_instant_query_with_head(
                        head,
                        labels,
                        &aggregation.input,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::BinaryExpression(_) => Ok(None),
        }
    }

    fn execute_promql_native_exponential_histogram_instant_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<(Vec<PromqlExponentialHistogramSeries>, QueryStats)>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(selector.clone())?
                else {
                    return Ok(None);
                };
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_native_exponential_histogram_selector_with_head_with_limits(
                    head, labels, &selector, start_ms, end_ms, limits,
                )
                .map(Some)
            }
            PromqlQuery::RangeFunction(function) => {
                let Some(selector) =
                    native_exponential_histogram_selector_from_promql(function.selector.clone())?
                else {
                    return Ok(None);
                };
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let (series, stats) = self
                    .query_native_exponential_histogram_selector_with_head_with_limits(
                        head,
                        labels,
                        &selector,
                        range_start_ms,
                        end_ms,
                        limits,
                    )?;
                Ok(Some((
                    evaluate_exponential_histogram_range_function(function, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Aggregation(aggregation) => {
                if !native_histogram_aggregation_supported(&aggregation.op) {
                    return Ok(None);
                }
                let Some((series, stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query_with_head(
                        head,
                        labels,
                        &aggregation.input,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                Ok(Some((
                    evaluate_exponential_histogram_aggregation(aggregation, series, end_ms),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::BinaryExpression(_) => Ok(None),
        }
    }

    pub(super) fn execute_promql_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                self.query_selectors_with_head_with_limits(
                    head, labels, &selectors, start_ms, end_ms, limits,
                )
                .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation_with_head(
                            head,
                            labels,
                            aggregation,
                            end_ms,
                            limits,
                        )?
                {
                    return Ok(execution);
                }
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &aggregation.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &absent.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => self
                .execute_promql_histogram_fraction_with_head(
                    head, labels, function, end_ms, limits,
                ),
            PromqlQuery::HistogramScalarFunction(function) => self
                .execute_promql_histogram_scalar_function_with_head(
                    head, labels, function, end_ms, limits,
                ),
            PromqlQuery::HistogramQuantile(function) => self
                .execute_promql_histogram_quantile_with_head(
                    head, labels, function, end_ms, limits,
                ),
            PromqlQuery::BinaryExpression(expression) => self
                .execute_promql_binary_expression_with_head(
                    head, labels, expression, end_ms, limits,
                ),
        }
    }

    fn execute_promql_instant_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    selector.clone(),
                    &self.query_projection_config,
                )?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_selectors_with_head_with_limits(
                    head, labels, &selectors, start_ms, end_ms, limits,
                )
                .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                if native_histogram_scalar_aggregation_supported(&aggregation.op)
                    && let Some(execution) = self
                        .execute_promql_native_histogram_scalar_aggregation_with_head(
                            head,
                            labels,
                            aggregation,
                            end_ms,
                            limits,
                        )?
                {
                    return Ok(execution);
                }
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &aggregation.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &absent.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(function) => self
                .execute_promql_histogram_fraction_with_head(
                    head, labels, function, end_ms, limits,
                ),
            PromqlQuery::HistogramScalarFunction(function) => self
                .execute_promql_histogram_scalar_function_with_head(
                    head, labels, function, end_ms, limits,
                ),
            PromqlQuery::HistogramQuantile(function) => self
                .execute_promql_histogram_quantile_with_head(
                    head, labels, function, end_ms, limits,
                ),
            PromqlQuery::BinaryExpression(expression) => self
                .execute_promql_binary_expression_with_head(
                    head, labels, expression, end_ms, limits,
                ),
        }
    }

    fn execute_promql_float_only_instant_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        match query {
            PromqlQuery::Vector(selector) => {
                let selectors = storage_float_selectors_from_promql(selector.clone())?;
                let start_ms = instant_vector_start_ms(end_ms);
                self.query_selectors_with_head_with_limits(
                    head, labels, &selectors, start_ms, end_ms, limits,
                )
                .map_err(promql_error_from_query_io)
            }
            PromqlQuery::Scalar(value) => Ok(QueryExecution {
                results: evaluate_scalar(*value, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Aggregation(aggregation) => {
                let mut execution = self.execute_promql_float_only_instant_query_with_head(
                    head,
                    labels,
                    &aggregation.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_aggregation(aggregation, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Absent(absent) => {
                let mut execution = self.execute_promql_float_only_instant_query_with_head(
                    head,
                    labels,
                    &absent.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_absent(absent, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::AbsentOverTime(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        range_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_absent_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::InstantFunction(function) => {
                let mut execution = self.execute_promql_float_only_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_instant_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::BinaryExpression(_) => Ok(QueryExecution {
                results: Vec::new(),
                stats: QueryStats::default(),
            }),
        }
    }

    fn execute_promql_histogram_fraction_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        function: &PromqlHistogramFraction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self
            .execute_promql_native_histogram_instant_query_with_head(
                head,
                labels,
                &function.input,
                end_ms,
                limits,
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_fraction(function, series, end_ms));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query_with_head(
                head,
                labels,
                &function.input,
                end_ms,
                limits,
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_fraction(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_histogram_quantile_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        function: &PromqlHistogramQuantile,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self
            .execute_promql_native_histogram_instant_query_with_head(
                head,
                labels,
                &function.input,
                end_ms,
                limits,
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_quantile(function, series, end_ms));
            }
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query_with_head(
                head,
                labels,
                &function.input,
                end_ms,
                limits,
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_quantile(
                    function, series, end_ms,
                ));
            }
        }

        if saw_native_input {
            let mut classic_execution = self.execute_promql_float_only_instant_query_with_head(
                head,
                labels,
                &function.input,
                end_ms,
                limits,
            )?;
            stats.merge_from(classic_execution.stats);
            stats.check_limits(limits)?;
            classic_execution.results =
                evaluate_histogram_quantile(function, classic_execution.results, end_ms);
            results.extend(classic_execution.results);
            return Ok(QueryExecution {
                results: merge_query_results(results),
                stats,
            });
        }

        let mut execution = self.execute_promql_instant_query_with_head(
            head,
            labels,
            &function.input,
            end_ms,
            limits,
        )?;
        execution.results = evaluate_histogram_quantile(function, execution.results, end_ms);
        Ok(execution)
    }

    fn execute_promql_histogram_scalar_function_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        function: &PromqlHistogramScalarFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self
            .execute_promql_native_histogram_instant_query_with_head(
                head,
                labels,
                &function.input,
                end_ms,
                limits,
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_histogram_scalar_function(
                function, series, end_ms,
            ));
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query_with_head(
                head,
                labels,
                &function.input,
                end_ms,
                limits,
            )?
        {
            saw_native_input = true;
            stats.merge_from(native_stats);
            results.extend(evaluate_native_exponential_histogram_scalar_function(
                function, series, end_ms,
            ));
        }

        if !saw_native_input {
            return Ok(QueryExecution {
                results: Vec::new(),
                stats,
            });
        }
        stats.check_limits(limits)?;
        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_native_histogram_scalar_aggregation_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        aggregation: &PromqlAggregation,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let mut histogram_series = Vec::new();
        let mut exponential_histogram_series = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some((series, native_stats)) = self
            .execute_promql_native_histogram_instant_query_with_head(
                head,
                labels,
                &aggregation.input,
                end_ms,
                limits,
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                histogram_series = series;
            }
        }
        if let Some((series, native_stats)) = self
            .execute_promql_native_exponential_histogram_instant_query_with_head(
                head,
                labels,
                &aggregation.input,
                end_ms,
                limits,
            )?
        {
            if !series.is_empty() || native_stats.projected_series > 0 {
                saw_native_input = true;
                stats.merge_from(native_stats);
                exponential_histogram_series = series;
            }
        }

        if !saw_native_input {
            return Ok(None);
        }
        let scalar_execution = self.execute_promql_float_only_instant_query_with_head(
            head,
            labels,
            &aggregation.input,
            end_ms,
            limits,
        )?;
        stats.merge_from(scalar_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_native_histogram_scalar_aggregation(
            aggregation,
            scalar_execution.results,
            histogram_series,
            exponential_histogram_series,
            end_ms,
        );
        Ok(Some(QueryExecution { results, stats }))
    }

    fn execute_promql_binary_expression_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        if binary_operator_is_set(expression.op) {
            if scalar_expression_value(&expression.left).is_some()
                || scalar_expression_value(&expression.right).is_some()
            {
                return Err(PromqlQueryError::Unsupported(
                    "set binary operators require instant-vector operands".to_string(),
                ));
            }

            let left_execution = self.execute_promql_instant_query_with_head(
                head,
                labels,
                &expression.left,
                end_ms,
                limits,
            )?;
            let right_execution = self.execute_promql_instant_query_with_head(
                head,
                labels,
                &expression.right,
                end_ms,
                limits,
            )?;
            let mut stats = left_execution.stats;
            stats.merge_from(right_execution.stats);
            stats.check_limits(limits)?;
            let results = evaluate_binary_vector_set(
                expression,
                left_execution.results,
                right_execution.results,
                end_ms,
            )?;
            return Ok(QueryExecution { results, stats });
        }

        if let Some(left) = scalar_expression_value(&expression.left) {
            if let Some(right) = scalar_expression_value(&expression.right) {
                return Ok(QueryExecution {
                    results: evaluate_binary_scalar_scalar(expression.op, left, right, end_ms),
                    stats: QueryStats::default(),
                });
            }

            let mut execution = self.execute_promql_instant_query_with_head(
                head,
                labels,
                &expression.right,
                end_ms,
                limits,
            )?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, left, true, end_ms);
            return Ok(execution);
        }

        if let Some(right) = scalar_expression_value(&expression.right) {
            let mut execution = self.execute_promql_instant_query_with_head(
                head,
                labels,
                &expression.left,
                end_ms,
                limits,
            )?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, right, false, end_ms);
            return Ok(execution);
        }

        let left_execution = self.execute_promql_instant_query_with_head(
            head,
            labels,
            &expression.left,
            end_ms,
            limits,
        )?;
        let right_execution = self.execute_promql_instant_query_with_head(
            head,
            labels,
            &expression.right,
            end_ms,
            limits,
        )?;
        let mut stats = left_execution.stats;
        stats.merge_from(right_execution.stats);
        stats.check_limits(limits)?;
        let results = evaluate_binary_vector_vector(
            expression,
            left_execution.results,
            right_execution.results,
            end_ms,
        )?;
        Ok(QueryExecution { results, stats })
    }

    pub fn metric_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metric_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn metric_names_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_metric_names(start_ms, end_ms, &mut metadata)?;
        head.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.metric_names())
    }

    pub fn label_names(&self, start_ms: u64, end_ms: u64) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_names(start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_names_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_names(start_ms, end_ms, &mut metadata)?;
        head.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_names())
    }

    pub fn label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>> {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_values(label_name, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    pub fn label_values_with_head<R>(
        &self,
        label_name: &str,
        head: &HeadBuffer,
        labels: &R,
        start_ms: u64,
        end_ms: u64,
    ) -> io::Result<Vec<String>>
    where
        R: SeriesLabelResolver,
    {
        let mut metadata = MetadataAccumulator::default();
        self.collect_label_values(label_name, start_ms, end_ms, &mut metadata)?;
        head.collect_metadata(labels, start_ms, end_ms, &mut metadata)?;
        Ok(metadata.label_values(&normalize_discovery_label_name(label_name)))
    }

    pub(super) fn query_selector_with_budget(
        &self,
        selector: &SegmentSelector,
        start_ms: u64,
        end_ms: u64,
        budget: &mut QueryBudget,
    ) -> io::Result<Vec<SegmentQueryResult>> {
        if end_ms < start_ms {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();
        for segment in &self.segments {
            budget.observe_segment_considered();
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                budget.observe_segment_skipped_by_time();
                continue;
            }

            results.extend(segment.query_selector_with_budget(selector, start_ms, end_ms, budget)?);
        }

        Ok(merge_query_results(results))
    }

    pub(super) fn collect_metric_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }
            segment.collect_metric_names(start_ms, end_ms, metadata)?;
        }

        Ok(())
    }

    pub(super) fn collect_label_names(
        &self,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }
            segment.collect_label_names(start_ms, end_ms, metadata)?;
        }

        Ok(())
    }

    pub(super) fn collect_label_values(
        &self,
        label_name: &str,
        start_ms: u64,
        end_ms: u64,
        metadata: &mut MetadataAccumulator,
    ) -> io::Result<()> {
        if end_ms < start_ms {
            return Ok(());
        }

        for segment in &self.segments {
            if segment.meta.end_ms < start_ms || segment.meta.start_ms > end_ms {
                continue;
            }
            segment.collect_label_values(label_name, start_ms, end_ms, metadata)?;
        }

        Ok(())
    }
}
