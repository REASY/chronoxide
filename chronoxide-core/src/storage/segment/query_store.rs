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

    pub fn query_promql_range(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_range_query(&query, start_ms, end_ms, step_ms, QueryLimits::unlimited())
            .map(|execution| execution.results)
    }

    pub fn query_promql_range_with_limits(
        &self,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let query = parse_query(query)?;
        self.execute_promql_range_query(&query, start_ms, end_ms, step_ms, limits)
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

    pub fn query_promql_range_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
    ) -> Result<Vec<SegmentQueryResult>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let query = parse_query(query)?;
        self.execute_promql_range_query_with_head(
            head,
            labels,
            &query,
            start_ms,
            end_ms,
            step_ms,
            QueryLimits::unlimited(),
        )
        .map(|execution| execution.results)
    }

    pub fn query_promql_range_with_head_with_limits<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &str,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        let query = parse_query(query)?;
        self.execute_promql_range_query_with_head(
            head, labels, &query, start_ms, end_ms, step_ms, limits,
        )
    }

    fn evaluate_promql_vector_function(
        &self,
        function: &PromqlVectorFunction,
        end_ms: u64,
    ) -> Result<QueryExecution, PromqlQueryError> {
        let Some(value) = scalar_expression_value(&function.input, end_ms) else {
            return Err(PromqlQueryError::Invalid(
                "vector() requires a scalar expression".to_string(),
            ));
        };
        Ok(QueryExecution {
            results: evaluate_scalar(value, end_ms),
            stats: QueryStats::default(),
        })
    }

    fn execute_promql_range_query(
        &self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        validate_promql_range_bounds(start_ms, end_ms, step_ms)?;
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut eval_time_ms = start_ms;

        loop {
            let mut execution = self.execute_promql_instant_query(query, eval_time_ms, limits)?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            results.extend(retimestamp_instant_results(
                std::mem::take(&mut execution.results),
                eval_time_ms,
            ));

            let Some(next_eval_time_ms) = eval_time_ms.checked_add(step_ms) else {
                break;
            };
            if next_eval_time_ms > end_ms {
                break;
            }
            eval_time_ms = next_eval_time_ms;
        }

        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
    }

    fn execute_promql_range_query_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        validate_promql_range_bounds(start_ms, end_ms, step_ms)?;
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut eval_time_ms = start_ms;

        loop {
            let mut execution = self.execute_promql_instant_query_with_head(
                head,
                labels,
                query,
                eval_time_ms,
                limits,
            )?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            results.extend(retimestamp_instant_results(
                std::mem::take(&mut execution.results),
                eval_time_ms,
            ));

            let Some(next_eval_time_ms) = eval_time_ms.checked_add(step_ms) else {
                break;
            };
            if next_eval_time_ms > end_ms {
                break;
            }
            eval_time_ms = next_eval_time_ms;
        }

        Ok(QueryExecution {
            results: merge_query_results(results),
            stats,
        })
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
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution =
                    self.execute_promql_instant_query(&offset.input, shifted_end_ms, limits)?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) =
                    self.execute_promql_native_histogram_resets(function, end_ms, limits)?
                {
                    return Ok(execution);
                }
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, read_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
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
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution =
                    self.execute_promql_instant_query(&offset.input, shifted_end_ms, limits)?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) =
                    self.execute_promql_native_histogram_resets(function, end_ms, limits)?
                {
                    return Ok(execution);
                }
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, read_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
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
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_float_only_instant_query(
                    &offset.input,
                    shifted_end_ms,
                    limits,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution =
                    self.execute_promql_float_only_instant_query(&function.input, end_ms, limits)?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
                let selectors = storage_float_selectors_from_promql(function.selector.clone())?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let mut execution = self
                    .query_selectors_with_limits(&selectors, range_start_ms, end_ms, limits)
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
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

    fn execute_promql_native_histogram_resets(
        &self,
        function: &PromqlRangeFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        if function.kind != PromqlRangeFunctionKind::Resets {
            return Ok(None);
        }

        let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some(selector) = native_histogram_selector_from_promql(function.selector.clone())? {
            let (series, native_stats) = self.query_native_histogram_selector_with_limits(
                &selector,
                range_start_ms,
                end_ms,
                limits,
            )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_resets(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if let Some(selector) =
            native_exponential_histogram_selector_from_promql(function.selector.clone())?
        {
            let (series, native_stats) = self
                .query_native_exponential_histogram_selector_with_limits(
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_resets(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if !saw_native_input {
            return Ok(None);
        }

        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
    }

    fn execute_promql_native_histogram_resets_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        function: &PromqlRangeFunction,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        if function.kind != PromqlRangeFunctionKind::Resets {
            return Ok(None);
        }

        let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        if let Some(selector) = native_histogram_selector_from_promql(function.selector.clone())? {
            let (series, native_stats) = self
                .query_native_histogram_selector_with_head_with_limits(
                    head,
                    labels,
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_histogram_resets(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if let Some(selector) =
            native_exponential_histogram_selector_from_promql(function.selector.clone())?
        {
            let (series, native_stats) = self
                .query_native_exponential_histogram_selector_with_head_with_limits(
                    head,
                    labels,
                    &selector,
                    range_start_ms,
                    end_ms,
                    limits,
                )?;
            if native_histogram_input_present(&series, native_stats) {
                saw_native_input = true;
                stats.merge_from(native_stats);
                results.extend(evaluate_native_exponential_histogram_resets(
                    function,
                    series,
                    range_start_ms,
                    end_ms,
                ));
            }
        }

        if !saw_native_input {
            return Ok(None);
        }

        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
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

    fn execute_promql_native_histogram_binary_bool_comparison(
        &self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError> {
        if !expression.return_bool
            || !matches!(
                expression.op,
                PromqlBinaryOp::Eq
                    | PromqlBinaryOp::NotEq
                    | PromqlBinaryOp::Gt
                    | PromqlBinaryOp::Gte
                    | PromqlBinaryOp::Lt
                    | PromqlBinaryOp::Lte
            )
        {
            return Ok(None);
        }

        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        let left_histogram =
            self.execute_promql_native_histogram_instant_query(&expression.left, end_ms, limits)?;
        let right_histogram =
            self.execute_promql_native_histogram_instant_query(&expression.right, end_ms, limits)?;
        let left_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.left,
            end_ms,
            limits,
        )?;
        let right_exponential = self.execute_promql_native_exponential_histogram_instant_query(
            &expression.right,
            end_ms,
            limits,
        )?;

        let left_histogram_series = if let Some((series, query_stats)) = left_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_histogram_series = if let Some((series, query_stats)) = right_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let left_exponential_series = if let Some((series, query_stats)) = left_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_exponential_series = if let Some((series, query_stats)) = right_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };

        results.extend(evaluate_native_histogram_binary_bool_vector_vector(
            expression,
            left_histogram_series.clone(),
            right_histogram_series.clone(),
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_binary_bool_vector_vector(
                expression,
                left_exponential_series.clone(),
                right_exponential_series.clone(),
                end_ms,
            )?,
        );
        results.extend(evaluate_native_histogram_mixed_binary_bool_vector_vector(
            expression,
            left_histogram_series,
            right_exponential_series,
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_mixed_binary_bool_vector_vector(
                expression,
                left_exponential_series,
                right_histogram_series,
                end_ms,
            )?,
        );

        if !saw_native_input {
            return Ok(None);
        }
        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
    }

    fn execute_promql_scalar_operand(
        &self,
        query: &PromqlQuery,
        static_value: Option<f64>,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(f64, QueryStats), PromqlQueryError> {
        if let Some(value) = static_value {
            return Ok((value, QueryStats::default()));
        }

        let execution = self.execute_promql_instant_query(query, end_ms, limits)?;
        let value = scalar_query_result_value(&execution.results)?;
        Ok((value, execution.stats))
    }

    fn execute_promql_binary_expression(
        &self,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        if binary_operator_is_set(expression.op) {
            if is_scalar_expression(&expression.left) || is_scalar_expression(&expression.right) {
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

        let left_static = scalar_expression_value(&expression.left, end_ms);
        let right_static = scalar_expression_value(&expression.right, end_ms);
        let left_is_scalar = left_static.is_some() || is_scalar_expression(&expression.left);
        let right_is_scalar = right_static.is_some() || is_scalar_expression(&expression.right);

        if !left_is_scalar
            && !right_is_scalar
            && let Some(execution) = self.execute_promql_native_histogram_binary_bool_comparison(
                expression, end_ms, limits,
            )?
        {
            return Ok(execution);
        }

        if left_is_scalar && right_is_scalar {
            let (left, mut stats) =
                self.execute_promql_scalar_operand(&expression.left, left_static, end_ms, limits)?;
            let (right, right_stats) = self.execute_promql_scalar_operand(
                &expression.right,
                right_static,
                end_ms,
                limits,
            )?;
            stats.merge_from(right_stats);
            stats.check_limits(limits)?;
            return Ok(QueryExecution {
                results: evaluate_binary_scalar_scalar(expression.op, left, right, end_ms),
                stats,
            });
        }

        if left_is_scalar {
            let (left, mut stats) =
                self.execute_promql_scalar_operand(&expression.left, left_static, end_ms, limits)?;
            let mut execution =
                self.execute_promql_instant_query(&expression.right, end_ms, limits)?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, left, true, end_ms);
            execution.stats = stats;
            return Ok(execution);
        }

        if right_is_scalar {
            let (right, right_stats) = self.execute_promql_scalar_operand(
                &expression.right,
                right_static,
                end_ms,
                limits,
            )?;
            let mut execution =
                self.execute_promql_instant_query(&expression.left, end_ms, limits)?;
            execution.stats.merge_from(right_stats);
            execution.stats.check_limits(limits)?;
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
            PromqlQuery::Offset(offset) => self.execute_promql_native_histogram_instant_query(
                &offset.input,
                offset_eval_time_ms(end_ms, offset.offset_ms),
                limits,
            ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
                    )?;
                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                        )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_combined_vector_set(
                            expression,
                            left_histogram_series,
                            right_histogram_series,
                            left_exponential_series,
                            right_exponential_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_exponential =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_exponential_histogram_instant_query(
                                &expression.right,
                                end_ms,
                                limits,
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
                    let mut results = evaluate_native_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_exponential_series, right_exponential_stats)) =
                        right_exponential
                    {
                        stats.merge_from(right_exponential_stats);
                        results.extend(evaluate_native_histogram_mixed_binary_vector_vector(
                            expression,
                            left_series,
                            right_exponential_series,
                            end_ms,
                        )?);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand(
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand(
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
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
            PromqlQuery::Offset(offset) => self
                .execute_promql_native_exponential_histogram_instant_query(
                    &offset.input,
                    offset_eval_time_ms(end_ms, offset.offset_ms),
                    limits,
                ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                        )?;
                    let left_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                    )?;
                    let right_histogram = self.execute_promql_native_histogram_instant_query(
                        &expression.right,
                        end_ms,
                        limits,
                    )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_combined_vector_set(
                            expression,
                            left_exponential_series,
                            right_exponential_series,
                            left_histogram_series,
                            right_histogram_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.left,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_histogram =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_histogram_instant_query(
                                &expression.right,
                                end_ms,
                                limits,
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
                    let mut results = evaluate_native_exponential_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_histogram_series, right_histogram_stats)) = right_histogram {
                        stats.merge_from(right_histogram_stats);
                        results.extend(
                            evaluate_native_exponential_histogram_mixed_binary_vector_vector(
                                expression,
                                left_series,
                                right_histogram_series,
                                end_ms,
                            )?,
                        );
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand(
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query(
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand(
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query(
                        &expression.left,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_exponential_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
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
            PromqlQuery::Offset(offset) => self
                .execute_promql_native_histogram_instant_query_with_head(
                    head,
                    labels,
                    &offset.input,
                    offset_eval_time_ms(end_ms, offset.offset_ms),
                    limits,
                ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?;
                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_combined_vector_set(
                            expression,
                            left_histogram_series,
                            right_histogram_series,
                            left_exponential_series,
                            right_exponential_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_exponential = if matches!(
                        expression.op,
                        PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq
                    ) {
                        self.execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    } else {
                        None
                    };
                    stats.merge_from(right_stats);
                    let mut results = evaluate_native_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_exponential_series, right_exponential_stats)) =
                        right_exponential
                    {
                        stats.merge_from(right_exponential_stats);
                        results.extend(evaluate_native_histogram_mixed_binary_vector_vector(
                            expression,
                            left_series,
                            right_exponential_series,
                            end_ms,
                        )?);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand_with_head(
                        head,
                        labels,
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand_with_head(
                    head,
                    labels,
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_histogram_instant_query_with_head(
                        head,
                        labels,
                        &expression.left,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
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
            PromqlQuery::Offset(offset) => self
                .execute_promql_native_exponential_histogram_instant_query_with_head(
                    head,
                    labels,
                    &offset.input,
                    offset_eval_time_ms(end_ms, offset.offset_ms),
                    limits,
                ),
            PromqlQuery::BinaryExpression(expression) => {
                if binary_operator_is_set(expression.op) {
                    if is_scalar_expression(&expression.left)
                        || is_scalar_expression(&expression.right)
                    {
                        return Err(PromqlQueryError::Unsupported(
                            "set binary operators require instant-vector operands".to_string(),
                        ));
                    }

                    let left_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_exponential = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?;
                    let left_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?;
                    let right_histogram = self
                        .execute_promql_native_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?;

                    let mut stats = QueryStats::default();
                    let mut saw_native_input = false;
                    let left_exponential_series =
                        if let Some((series, query_stats)) = left_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let right_exponential_series =
                        if let Some((series, query_stats)) = right_exponential {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };
                    let left_histogram_series = if let Some((series, query_stats)) = left_histogram
                    {
                        if native_histogram_input_present(&series, query_stats) {
                            saw_native_input = true;
                            stats.merge_from(query_stats);
                        }
                        series
                    } else {
                        Vec::new()
                    };
                    let right_histogram_series =
                        if let Some((series, query_stats)) = right_histogram {
                            if native_histogram_input_present(&series, query_stats) {
                                saw_native_input = true;
                                stats.merge_from(query_stats);
                            }
                            series
                        } else {
                            Vec::new()
                        };

                    if !saw_native_input {
                        return Ok(None);
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_combined_vector_set(
                            expression,
                            left_exponential_series,
                            right_exponential_series,
                            left_histogram_series,
                            right_histogram_series,
                            end_ms,
                        )?,
                        stats,
                    )));
                }

                let left_static = scalar_expression_value(&expression.left, end_ms);
                let right_static = scalar_expression_value(&expression.right, end_ms);
                let left_is_scalar =
                    left_static.is_some() || is_scalar_expression(&expression.left);
                let right_is_scalar =
                    right_static.is_some() || is_scalar_expression(&expression.right);

                if left_is_scalar && right_is_scalar {
                    return Ok(None);
                }

                if !left_is_scalar && !right_is_scalar {
                    let Some((left_series, mut stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.left,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let Some((right_series, right_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    let right_histogram =
                        if matches!(expression.op, PromqlBinaryOp::Eq | PromqlBinaryOp::NotEq) {
                            self.execute_promql_native_histogram_instant_query_with_head(
                                head,
                                labels,
                                &expression.right,
                                end_ms,
                                limits,
                            )?
                        } else {
                            None
                        };
                    stats.merge_from(right_stats);
                    let mut results = evaluate_native_exponential_histogram_binary_vector_vector(
                        expression,
                        left_series.clone(),
                        right_series,
                        end_ms,
                    )?;
                    if let Some((right_histogram_series, right_histogram_stats)) = right_histogram {
                        stats.merge_from(right_histogram_stats);
                        results.extend(
                            evaluate_native_exponential_histogram_mixed_binary_vector_vector(
                                expression,
                                left_series,
                                right_histogram_series,
                                end_ms,
                            )?,
                        );
                    }
                    stats.check_limits(limits)?;
                    return Ok(Some((results, stats)));
                }

                if left_is_scalar {
                    let (scalar, mut stats) = self.execute_promql_scalar_operand_with_head(
                        head,
                        labels,
                        &expression.left,
                        left_static,
                        end_ms,
                        limits,
                    )?;
                    let Some((series, histogram_stats)) = self
                        .execute_promql_native_exponential_histogram_instant_query_with_head(
                            head,
                            labels,
                            &expression.right,
                            end_ms,
                            limits,
                        )?
                    else {
                        return Ok(None);
                    };
                    stats.merge_from(histogram_stats);
                    stats.check_limits(limits)?;
                    return Ok(Some((
                        evaluate_native_exponential_histogram_binary_vector_scalar(
                            expression, series, scalar, true,
                        ),
                        stats,
                    )));
                }

                let (scalar, scalar_stats) = self.execute_promql_scalar_operand_with_head(
                    head,
                    labels,
                    &expression.right,
                    right_static,
                    end_ms,
                    limits,
                )?;
                let Some((series, mut stats)) = self
                    .execute_promql_native_exponential_histogram_instant_query_with_head(
                        head,
                        labels,
                        &expression.left,
                        end_ms,
                        limits,
                    )?
                else {
                    return Ok(None);
                };
                stats.merge_from(scalar_stats);
                stats.check_limits(limits)?;
                Ok(Some((
                    evaluate_native_exponential_histogram_binary_vector_scalar(
                        expression, series, scalar, false,
                    ),
                    stats,
                )))
            }
            PromqlQuery::Scalar(_)
            | PromqlQuery::Time
            | PromqlQuery::VectorFunction(_)
            | PromqlQuery::ScalarFunction(_)
            | PromqlQuery::QuantileOverTime(_)
            | PromqlQuery::PredictLinear(_)
            | PromqlQuery::DoubleExponentialSmoothing(_)
            | PromqlQuery::LabelReplace(_)
            | PromqlQuery::LabelJoin(_)
            | PromqlQuery::Absent(_)
            | PromqlQuery::AbsentOverTime(_)
            | PromqlQuery::InstantFunction(_)
            | PromqlQuery::HistogramQuantile(_)
            | PromqlQuery::HistogramFraction(_)
            | PromqlQuery::HistogramScalarFunction(_) => Ok(None),
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
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &offset.input,
                    shifted_end_ms,
                    limits,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) = self.execute_promql_native_histogram_resets_with_head(
                    head, labels, function, end_ms, limits,
                )? {
                    return Ok(execution);
                }
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        read_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results = evaluate_range_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::QuantileOverTime(function) => {
                let selectors = storage_selectors_from_promql_with_projection_config(
                    function.selector.clone(),
                    &self.query_projection_config,
                )?;
                let range_start_ms = range_function_start_ms(end_ms, function.range_ms);
                let read_start_ms =
                    range_selector_read_start_ms(&selectors, range_start_ms, end_ms);
                let mut execution = self
                    .query_selectors_with_head_with_limits(
                        head,
                        labels,
                        &selectors,
                        read_start_ms,
                        end_ms,
                        limits,
                    )
                    .map_err(promql_error_from_query_io)?;
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
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
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
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
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
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
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &offset.input,
                    shifted_end_ms,
                    limits,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution = self.execute_promql_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::RangeFunction(function) => {
                if let Some(execution) = self.execute_promql_native_histogram_resets_with_head(
                    head, labels, function, end_ms, limits,
                )? {
                    return Ok(execution);
                }
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
            PromqlQuery::QuantileOverTime(function) => {
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
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
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
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
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
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
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
            PromqlQuery::Time => Ok(QueryExecution {
                results: evaluate_scalar(end_ms as f64 / 1000.0, end_ms),
                stats: QueryStats::default(),
            }),
            PromqlQuery::VectorFunction(function) => {
                self.evaluate_promql_vector_function(function, end_ms)
            }
            PromqlQuery::ScalarFunction(function) => {
                let mut execution = self.execute_promql_float_only_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_scalar_function(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::Offset(offset) => {
                let shifted_end_ms = offset_eval_time_ms(end_ms, offset.offset_ms);
                let mut execution = self.execute_promql_float_only_instant_query_with_head(
                    head,
                    labels,
                    &offset.input,
                    shifted_end_ms,
                    limits,
                )?;
                execution.results = retimestamp_instant_results(execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::LabelReplace(function) => {
                let mut execution = self.execute_promql_float_only_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_label_replace(function, execution.results, end_ms)?;
                Ok(execution)
            }
            PromqlQuery::LabelJoin(function) => {
                let mut execution = self.execute_promql_float_only_instant_query_with_head(
                    head,
                    labels,
                    &function.input,
                    end_ms,
                    limits,
                )?;
                execution.results = evaluate_label_join(function, execution.results, end_ms);
                Ok(execution)
            }
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
            PromqlQuery::QuantileOverTime(function) => {
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
                execution.results =
                    evaluate_quantile_over_time(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::PredictLinear(function) => {
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
                execution.results = evaluate_predict_linear(function, execution.results, end_ms);
                Ok(execution)
            }
            PromqlQuery::DoubleExponentialSmoothing(function) => {
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
                execution.results =
                    evaluate_double_exponential_smoothing(function, execution.results, end_ms);
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

    fn execute_promql_native_histogram_binary_bool_comparison_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        expression: &PromqlBinaryExpression,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<Option<QueryExecution>, PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        if !expression.return_bool
            || !matches!(
                expression.op,
                PromqlBinaryOp::Eq
                    | PromqlBinaryOp::NotEq
                    | PromqlBinaryOp::Gt
                    | PromqlBinaryOp::Gte
                    | PromqlBinaryOp::Lt
                    | PromqlBinaryOp::Lte
            )
        {
            return Ok(None);
        }

        let mut results = Vec::new();
        let mut stats = QueryStats::default();
        let mut saw_native_input = false;

        let left_histogram = self.execute_promql_native_histogram_instant_query_with_head(
            head,
            labels,
            &expression.left,
            end_ms,
            limits,
        )?;
        let right_histogram = self.execute_promql_native_histogram_instant_query_with_head(
            head,
            labels,
            &expression.right,
            end_ms,
            limits,
        )?;
        let left_exponential = self
            .execute_promql_native_exponential_histogram_instant_query_with_head(
                head,
                labels,
                &expression.left,
                end_ms,
                limits,
            )?;
        let right_exponential = self
            .execute_promql_native_exponential_histogram_instant_query_with_head(
                head,
                labels,
                &expression.right,
                end_ms,
                limits,
            )?;

        let left_histogram_series = if let Some((series, query_stats)) = left_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_histogram_series = if let Some((series, query_stats)) = right_histogram {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let left_exponential_series = if let Some((series, query_stats)) = left_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };
        let right_exponential_series = if let Some((series, query_stats)) = right_exponential {
            if native_histogram_input_present(&series, query_stats) {
                saw_native_input = true;
                stats.merge_from(query_stats);
            }
            series
        } else {
            Vec::new()
        };

        results.extend(evaluate_native_histogram_binary_bool_vector_vector(
            expression,
            left_histogram_series.clone(),
            right_histogram_series.clone(),
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_binary_bool_vector_vector(
                expression,
                left_exponential_series.clone(),
                right_exponential_series.clone(),
                end_ms,
            )?,
        );
        results.extend(evaluate_native_histogram_mixed_binary_bool_vector_vector(
            expression,
            left_histogram_series,
            right_exponential_series,
            end_ms,
        )?);
        results.extend(
            evaluate_native_exponential_histogram_mixed_binary_bool_vector_vector(
                expression,
                left_exponential_series,
                right_histogram_series,
                end_ms,
            )?,
        );

        if !saw_native_input {
            return Ok(None);
        }
        stats.check_limits(limits)?;
        Ok(Some(QueryExecution {
            results: merge_query_results(results),
            stats,
        }))
    }

    fn execute_promql_scalar_operand_with_head<R>(
        &self,
        head: &HeadBuffer,
        labels: &R,
        query: &PromqlQuery,
        static_value: Option<f64>,
        end_ms: u64,
        limits: QueryLimits,
    ) -> Result<(f64, QueryStats), PromqlQueryError>
    where
        R: SeriesLabelResolver,
    {
        if let Some(value) = static_value {
            return Ok((value, QueryStats::default()));
        }

        let execution =
            self.execute_promql_instant_query_with_head(head, labels, query, end_ms, limits)?;
        let value = scalar_query_result_value(&execution.results)?;
        Ok((value, execution.stats))
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
            if is_scalar_expression(&expression.left) || is_scalar_expression(&expression.right) {
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

        let left_static = scalar_expression_value(&expression.left, end_ms);
        let right_static = scalar_expression_value(&expression.right, end_ms);
        let left_is_scalar = left_static.is_some() || is_scalar_expression(&expression.left);
        let right_is_scalar = right_static.is_some() || is_scalar_expression(&expression.right);

        if !left_is_scalar
            && !right_is_scalar
            && let Some(execution) = self
                .execute_promql_native_histogram_binary_bool_comparison_with_head(
                    head, labels, expression, end_ms, limits,
                )?
        {
            return Ok(execution);
        }

        if left_is_scalar && right_is_scalar {
            let (left, mut stats) = self.execute_promql_scalar_operand_with_head(
                head,
                labels,
                &expression.left,
                left_static,
                end_ms,
                limits,
            )?;
            let (right, right_stats) = self.execute_promql_scalar_operand_with_head(
                head,
                labels,
                &expression.right,
                right_static,
                end_ms,
                limits,
            )?;
            stats.merge_from(right_stats);
            stats.check_limits(limits)?;
            return Ok(QueryExecution {
                results: evaluate_binary_scalar_scalar(expression.op, left, right, end_ms),
                stats,
            });
        }

        if left_is_scalar {
            let (left, mut stats) = self.execute_promql_scalar_operand_with_head(
                head,
                labels,
                &expression.left,
                left_static,
                end_ms,
                limits,
            )?;
            let mut execution = self.execute_promql_instant_query_with_head(
                head,
                labels,
                &expression.right,
                end_ms,
                limits,
            )?;
            stats.merge_from(execution.stats);
            stats.check_limits(limits)?;
            execution.results =
                evaluate_binary_vector_scalar(expression, execution.results, left, true, end_ms);
            execution.stats = stats;
            return Ok(execution);
        }

        if right_is_scalar {
            let (right, right_stats) = self.execute_promql_scalar_operand_with_head(
                head,
                labels,
                &expression.right,
                right_static,
                end_ms,
                limits,
            )?;
            let mut execution = self.execute_promql_instant_query_with_head(
                head,
                labels,
                &expression.left,
                end_ms,
                limits,
            )?;
            execution.stats.merge_from(right_stats);
            execution.stats.check_limits(limits)?;
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
