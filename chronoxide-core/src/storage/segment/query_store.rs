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

    fn execute_promql_range_query(
        &self,
        query: &PromqlQuery,
        start_ms: u64,
        end_ms: u64,
        step_ms: u64,
        limits: QueryLimits,
    ) -> Result<QueryExecution, PromqlQueryError> {
        validate_promql_range_bounds(start_ms, end_ms, step_ms)?;
        let mut session = self.query_session().map_err(promql_error_from_query_io)?;
        let mut cache_call = super::range_scalar_cache::RangeScalarCacheCall::new(
            session.range_scalar_cache_budget_bytes,
            Arc::clone(&session.range_scalar_cache_governor),
        );
        let result = session.execute_validated_promql_range_query(
            query,
            start_ms,
            end_ms,
            step_ms,
            limits,
            &mut cache_call,
        );
        session.last_range_scalar_cache_summary = Some(cache_call.finish());
        result
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
}

mod head;
mod metadata;
mod native;
mod sealed;
