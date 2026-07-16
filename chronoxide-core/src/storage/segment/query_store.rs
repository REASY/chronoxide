use super::metadata_facade::{SegmentMetadataVisitControl, SegmentMetadataVisitError};
use super::query_reader::metadata_facade_io_error;
use super::*;

const SAMPLE_TIME_RANGE_SERIES_BATCH_SIZE: u32 = 256;

impl SegmentStoreReader {
    pub fn open(segments_dir: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_with_options(segments_dir, SegmentStoreOpenOptions::default())
    }

    pub fn open_with_options(
        segments_dir: impl AsRef<Path>,
        options: SegmentStoreOpenOptions,
    ) -> io::Result<Self> {
        let metadata_runtime = open_metadata_runtime(options.metadata_governor)?;
        let segment_dirs = discover_segment_dirs(segments_dir.as_ref())?;
        preflight_store_footers(&segment_dirs, options)?;

        let mut segments = Vec::with_capacity(segment_dirs.len());
        for segment_dir in segment_dirs {
            let reader = open_store_segment(segment_dir, options, metadata_runtime.clone())?;
            segments.push(reader);
        }

        sort_segment_readers(&mut segments);

        Ok(Self {
            segments,
            query_projection_config: QueryProjectionConfig::default(),
            metadata_runtime,
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

    /// Returns one current resource snapshot for every unique symbol-reader
    /// state retained by this store, independent of query-session clones.
    pub fn symbol_resource_snapshot(&self) -> SegmentStoreSymbolResources {
        SegmentStoreSymbolResources::snapshot_segment_readers(self.segments.iter())
    }

    pub fn metadata_governor_stats(&self) -> MetadataGovernorStats {
        self.metadata_runtime.snapshot().governor
    }

    pub fn metadata_runtime_snapshot(
        &self,
    ) -> crate::storage::metadata_runtime::StoreMetadataRuntimeSnapshot {
        self.metadata_runtime.snapshot()
    }

    /// Returns the observed sample-time range in the newest non-empty segment
    /// window.
    ///
    /// Multiple segments may share that window (for example, independently
    /// sealed shards), so their ranges are combined. Older windows are not
    /// visited once the newest non-empty window is selected.
    pub fn latest_window_sample_time_range(&self) -> io::Result<Option<(u64, u64)>> {
        let mut range: Option<(u64, u64)> = None;
        let mut selected_window = None;

        for segment in self.segments.iter().rev() {
            let segment_window = (segment.meta.start_ms, segment.meta.end_ms);
            if selected_window.is_some_and(|window| window != segment_window) {
                break;
            }

            let Some(segment_range) = segment.sample_time_range()? else {
                continue;
            };
            selected_window.get_or_insert(segment_window);
            range = Some(match range {
                Some((start_ms, end_ms)) => {
                    (start_ms.min(segment_range.0), end_ms.max(segment_range.1))
                }
                None => segment_range,
            });
        }

        Ok(range)
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
            let metadata_runtime = open_metadata_runtime(options.metadata_governor)?;
            return Ok(Self {
                segments: Vec::new(),
                query_projection_config: QueryProjectionConfig::default(),
                metadata_runtime,
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
        let metadata_runtime = open_metadata_runtime(options.metadata_governor)?;
        let mut manifest_segments = Vec::with_capacity(inventory.segments.len());

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
            manifest_segments.push((manifest_segment, segment_dir));
        }

        let segment_dirs = manifest_segments
            .iter()
            .map(|(_, segment_dir)| segment_dir.clone())
            .collect::<Vec<_>>();
        preflight_store_footers(&segment_dirs, options)?;

        let mut segments = Vec::with_capacity(manifest_segments.len());
        for (manifest_segment, segment_dir) in manifest_segments {
            let reader = open_store_segment(segment_dir, options, metadata_runtime.clone())?;
            validate_manifest_segment_meta(manifest_segment, reader.meta())?;
            segments.push(reader);
        }

        sort_segment_readers(&mut segments);
        Ok(Self {
            segments,
            query_projection_config: QueryProjectionConfig::default(),
            metadata_runtime,
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
        // Direct-store PromQL methods retain their complete-label contract;
        // callers that want demand-driven ownership use an explicit session.
        session.set_label_materialization_policy(QueryLabelMaterializationPolicy::Full);
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

impl SegmentReader {
    fn sample_time_range(&self) -> io::Result<Option<(u64, u64)>> {
        let context = self.standalone_facade_context()?;
        let series_count = context.root.series_count();
        let mut range: Option<(u64, u64)> = None;
        let mut batch_start = 0u32;

        while batch_start < series_count {
            let batch_end = batch_start
                .saturating_add(SAMPLE_TIME_RANGE_SERIES_BATCH_SIZE)
                .min(series_count);
            let series_refs = (batch_start..batch_end).collect::<Vec<_>>();
            let candidates = context
                .metadata
                .series_ref_set(&context.root, &series_refs)
                .map_err(metadata_facade_io_error)?;
            let visit = context.metadata.visit_verified_series_selected(
                &context.root,
                &candidates,
                &[],
                u8::MAX,
                false,
                |series| -> io::Result<SegmentMetadataVisitControl> {
                    series.chunks().visit(
                        |locator| -> io::Result<SegmentMetadataVisitControl> {
                            range = Some(match range {
                                Some((start_ms, end_ms)) => (
                                    start_ms.min(locator.min_time_ms()),
                                    end_ms.max(locator.max_time_ms()),
                                ),
                                None => (locator.min_time_ms(), locator.max_time_ms()),
                            });
                            Ok(SegmentMetadataVisitControl::Continue)
                        },
                    )?;
                    Ok(SegmentMetadataVisitControl::Continue)
                },
            );
            match visit {
                Ok(_) => {}
                Err(SegmentMetadataVisitError::Metadata(error)) => {
                    return Err(metadata_facade_io_error(error));
                }
                Err(SegmentMetadataVisitError::Visitor(error)) => return Err(error),
            }
            batch_start = batch_end;
        }

        Ok(range)
    }
}

fn discover_segment_dirs(segments_dir: &Path) -> io::Result<Vec<PathBuf>> {
    let mut segment_dirs = Vec::new();
    for entry in fs::read_dir(segments_dir)? {
        let entry = entry?;
        if !entry.file_type()?.is_dir() {
            continue;
        }
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if !name.starts_with("seg-") || SegmentId::parse_dir_name(&name).is_err() {
            continue;
        }
        segment_dirs.push(entry.path());
    }
    segment_dirs.sort();
    Ok(segment_dirs)
}

fn open_store_segment(
    segment_dir: impl AsRef<Path>,
    options: SegmentStoreOpenOptions,
    metadata_runtime: StoreMetadataRuntime,
) -> io::Result<SegmentReader> {
    let segment_dir = segment_dir.as_ref();
    let policy = options.storage_schema_policy;
    if options.requires_complete_footer_validation(policy) {
        SegmentReader::open_footer_validated_with_options(
            segment_dir,
            options,
            metadata_runtime,
            options.validate_segment_footers,
        )
        .map_err(|error| store_footer_error(segment_dir, "complete validation", policy, error))
    } else {
        SegmentReader::open_with_options(segment_dir, options, metadata_runtime)
    }
}

/// Validates one explicit schema policy across the complete store before any
/// segment reader can register metadata or open a metadata root.
fn preflight_store_footers(
    segment_dirs: &[PathBuf],
    options: SegmentStoreOpenOptions,
) -> io::Result<()> {
    let policy = options.storage_schema_policy;

    // First establish one homogeneous schema for the entire corpus. Keep this
    // as a separate pass so an early valid segment cannot open metadata before
    // a later segment reports a schema mismatch.
    for segment_dir in segment_dirs {
        let result = match policy {
            SegmentStoreSchemaPolicy::StrictSchema7 => {
                read_segment_footer_for_schema7(segment_dir).map(|_| ())
            }
            SegmentStoreSchemaPolicy::StrictSchema8 => {
                read_segment_footer_for_schema8(segment_dir).map(|_| ())
            }
            SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb => {
                read_segment_footer_for_schema6(segment_dir).map(|_| ())
            }
        };
        result.map_err(|error| store_footer_error(segment_dir, "preflight", policy, error))?;
    }

    Ok(())
}

fn store_footer_error(
    segment_dir: &Path,
    stage: &'static str,
    policy: SegmentStoreSchemaPolicy,
    error: io::Error,
) -> io::Error {
    io::Error::new(
        error.kind(),
        format!(
            "segment footer {stage} failed for {} under {policy:?}: {error}",
            segment_dir.display()
        ),
    )
}

mod head;
mod metadata;
mod native;
mod sealed;

#[cfg(test)]
mod metadata_governor_tests {
    use super::*;

    #[test]
    fn store_open_shares_one_metadata_governor_across_segments() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(7);
        let mut writer = SegmentWriter::new(config).unwrap();
        for (series_ref, timestamp_ms) in [(1, 1_000), (2, 11_000)] {
            writer
                .record_samples_ordered_with_label_visitor(
                    SeriesRef::new(series_ref),
                    &[(timestamp_ms, timestamp_ms as f64)],
                    |visit| {
                        visit(METRIC_NAME_LABEL, "metadata_governor_identity");
                    },
                )
                .unwrap();
        }
        writer.flush().unwrap();

        let governor_config = MetadataGovernorConfig {
            retained_max_bytes: 32 * 1024 * 1024,
            in_flight_max_bytes: 96 * 1024 * 1024,
            max_open_files: 1,
            max_cached_open_files: 0,
        };
        let store = SegmentStoreReader::open_with_options(
            tempdir.path(),
            SegmentStoreOpenOptions {
                metadata_governor: governor_config,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
        assert_eq!(store.segments.len(), 2);
        assert!(store.segments.iter().all(|segment| {
            segment.registered_metadata.segment_identity()
                == segment.dir.file_name().unwrap().to_str().unwrap()
        }));
        assert_eq!(
            store.metadata_runtime.snapshot().cache.registered_artifacts,
            store.segments.len() as u64 * SEGMENT_FOOTER_TRACKED_FILES.len() as u64
        );
        assert_eq!(store.metadata_runtime.governor().config(), governor_config);
        assert_eq!(
            store.metadata_governor_stats().in_flight_max_bytes,
            governor_config.in_flight_max_bytes
        );

        let runtime = store.metadata_runtime.clone();
        drop(store);
        assert_eq!(runtime.snapshot().cache.registered_artifacts, 0);
    }

    #[test]
    fn segment_open_rejects_a_footer_tracked_length_change_during_registration() {
        let tempdir = tempfile::tempdir().unwrap();
        let config = SegmentWriterConfig::new(tempdir.path(), Duration::from_secs(10))
            .with_deterministic_segment_ids(11);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer.record_sample(SeriesRef::new(1), 1_000, 1.0).unwrap();
        writer.flush().unwrap();

        let segment_dir = fs::read_dir(tempdir.path())
            .unwrap()
            .map(|entry| entry.unwrap())
            .find(|entry| entry.file_name().to_string_lossy().starts_with("seg-"))
            .unwrap()
            .path();
        let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
        let original_len = fs::metadata(&chunks_path).unwrap().len();
        fs::OpenOptions::new()
            .write(true)
            .open(chunks_path)
            .unwrap()
            .set_len(original_len + 1)
            .unwrap();

        let error = SegmentReader::open(segment_dir)
            .err()
            .expect("footer-tracked length change must fail ordinary open");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("length changed"));
    }

    #[test]
    fn invalid_governor_configuration_is_rejected_before_store_discovery() {
        let options = SegmentStoreOpenOptions {
            metadata_governor: MetadataGovernorConfig {
                in_flight_max_bytes: 0,
                ..MetadataGovernorConfig::default()
            },
            ..SegmentStoreOpenOptions::default()
        };
        let error = SegmentStoreReader::open_with_options(
            "/path/that/must/not/be/inspected/for-an-invalid-governor",
            options,
        )
        .err()
        .expect("invalid governor configuration must fail");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|source| source.downcast_ref::<MetadataGovernorConfigError>()),
            Some(&MetadataGovernorConfigError::ZeroInFlightBudget)
        );
    }
}

#[cfg(test)]
mod schema_policy_tests {
    use super::super::full_validation::SEGMENT_META_MAX_BYTES;
    use super::*;

    fn schema6_options() -> SegmentStoreOpenOptions {
        SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::ValidatedSchema6LayoutAb,
            ..SegmentStoreOpenOptions::default()
        }
    }

    fn write_segment(root: &Path, timestamp_ms: u64, schema7: bool, seed: u64) -> PathBuf {
        let before = discover_segment_dirs(root).unwrap();
        let config = SegmentWriterConfig::new(root, Duration::from_secs(10))
            .with_deterministic_segment_ids(seed)
            .with_storage_schema(if schema7 {
                SegmentStorageSchema::Schema7
            } else {
                SegmentStorageSchema::Schema6
            });
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_sample(
                SeriesRef::new(u32::try_from(seed).unwrap()),
                timestamp_ms,
                timestamp_ms as f64,
            )
            .unwrap();
        writer.flush().unwrap();
        discover_segment_dirs(root)
            .unwrap()
            .into_iter()
            .find(|path| !before.contains(path))
            .unwrap()
    }

    fn write_schema8_segment(root: &Path, timestamp_ms: u64, seed: u64) -> PathBuf {
        let before = discover_segment_dirs(root).unwrap();
        let config = SegmentWriterConfig::new(root, Duration::from_secs(10))
            .with_deterministic_segment_ids(seed)
            .with_storage_schema(SegmentStorageSchema::Schema8);
        let mut writer = SegmentWriter::new(config).unwrap();
        writer
            .record_sample(
                SeriesRef::new(u32::try_from(seed).unwrap()),
                timestamp_ms,
                timestamp_ms as f64,
            )
            .unwrap();
        writer.flush().unwrap();
        discover_segment_dirs(root)
            .unwrap()
            .into_iter()
            .find(|path| !before.contains(path))
            .unwrap()
    }

    fn manifest_segment(segment_dir: &Path) -> ManifestSegment {
        let segment_id = segment_dir.file_name().unwrap().to_str().unwrap();
        let parsed = SegmentId::parse_dir_name(segment_id).unwrap();
        ManifestSegment::new(
            segment_id.to_owned(),
            parsed.start_ms(),
            parsed.end_ms(),
            None,
        )
        .unwrap()
    }

    fn copy_segment_into_root(segment_dir: &Path, root: &Path) -> PathBuf {
        let copied = root.join(segment_dir.file_name().unwrap());
        fs::create_dir(&copied).unwrap();
        for entry in fs::read_dir(segment_dir).unwrap() {
            let entry = entry.unwrap();
            assert!(entry.file_type().unwrap().is_file());
            fs::copy(entry.path(), copied.join(entry.file_name())).unwrap();
        }
        copied
    }

    #[test]
    fn default_store_policy_is_strict_schema8() {
        assert_eq!(
            SegmentStoreOpenOptions::default().storage_schema_policy,
            SegmentStoreSchemaPolicy::StrictSchema8
        );
    }

    #[test]
    fn explicit_strict_schema7_opens_a_schema7_store() {
        let tempdir = tempfile::tempdir().unwrap();
        write_segment(tempdir.path(), 1_000, true, 1);

        SegmentStoreReader::open_with_options(
            tempdir.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .unwrap();
    }

    #[test]
    fn default_schema8_opens_schema8_and_schema_policies_cross_reject() {
        let schema8_dir = tempfile::tempdir().unwrap();
        write_schema8_segment(schema8_dir.path(), 1_000, 1);
        SegmentStoreReader::open(schema8_dir.path()).unwrap();

        let schema8_options = SegmentStoreOpenOptions {
            storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
            ..SegmentStoreOpenOptions::default()
        };
        SegmentStoreReader::open_with_options(schema8_dir.path(), schema8_options).unwrap();

        let schema7_error = SegmentStoreReader::open_with_options(
            schema8_dir.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema7,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .err()
        .expect("strict schema 7 must reject schema 8");
        assert_eq!(schema7_error.kind(), io::ErrorKind::InvalidData);

        let schema7_dir = tempfile::tempdir().unwrap();
        write_segment(schema7_dir.path(), 1_000, true, 1);
        let schema8_error =
            SegmentStoreReader::open_with_options(schema7_dir.path(), schema8_options)
                .err()
                .expect("strict schema 8 must reject schema 7");
        assert_eq!(schema8_error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn strict_schema8_mixed_store_preflights_all_footers_before_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let schema8_source = tempfile::tempdir().unwrap();
        let schema7_source = tempfile::tempdir().unwrap();
        let schema8 = copy_segment_into_root(
            &write_schema8_segment(schema8_source.path(), 1_000, 1),
            tempdir.path(),
        );
        copy_segment_into_root(
            &write_segment(schema7_source.path(), 11_000, true, 2),
            tempdir.path(),
        );
        fs::write(schema8.join(SegmentFile::MetaJson.filename()), b"{").unwrap();

        let error = SegmentStoreReader::open_with_options(
            tempdir.path(),
            SegmentStoreOpenOptions {
                storage_schema_policy: SegmentStoreSchemaPolicy::StrictSchema8,
                ..SegmentStoreOpenOptions::default()
            },
        )
        .err()
        .expect("mixed schema-7/8 store must fail footer preflight");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("preflight"));
        assert!(error.to_string().contains("schema version"));
    }

    #[cfg(unix)]
    #[test]
    fn default_schema8_store_preflight_does_not_follow_footer_symlinks() {
        use std::os::unix::fs::symlink;

        let tempdir = tempfile::tempdir().unwrap();
        let segment_dir = write_schema8_segment(tempdir.path(), 1_000, 1);
        let footer_path = segment_dir.join(SegmentFile::Footer.filename());
        let target_path = segment_dir.join("footer-target.bin");
        fs::rename(&footer_path, &target_path).unwrap();
        symlink(&target_path, &footer_path).unwrap();

        let error = SegmentStoreReader::open(tempdir.path())
            .err()
            .expect("production footer preflight must not follow a symlink");

        assert!(error.to_string().contains("preflight"));
    }

    #[test]
    fn default_schema8_registers_meta_shape_before_parsing_bytes() {
        let tempdir = tempfile::tempdir().unwrap();
        let segment_dir = write_schema8_segment(tempdir.path(), 1_000, 1);
        let meta_path = segment_dir.join(SegmentFile::MetaJson.filename());
        fs::write(&meta_path, vec![b'{'; SEGMENT_META_MAX_BYTES as usize + 1]).unwrap();
        let options = SegmentStoreOpenOptions::default();
        let runtime = open_metadata_runtime(options.metadata_governor).unwrap();

        let error = SegmentReader::open_with_options(&segment_dir, options, runtime.clone())
            .err()
            .expect("registered length mismatch must win before JSON parsing");

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("length changed"));
        let snapshot = runtime.snapshot();
        assert_eq!(snapshot.reads.issued.calls, 0);
        assert_eq!(snapshot.cache.registered_artifacts, 0);
    }

    #[test]
    fn schema6_policy_forces_complete_footer_validation() {
        let tempdir = tempfile::tempdir().unwrap();
        let segment_dir = write_segment(tempdir.path(), 1_000, false, 1);
        let chunks_path = segment_dir.join(SegmentFile::Chunks.filename());
        fs::OpenOptions::new()
            .append(true)
            .open(chunks_path)
            .unwrap()
            .write_all(&[0])
            .unwrap();

        let error = SegmentStoreReader::open_with_options(tempdir.path(), schema6_options())
            .err()
            .expect("schema-6 A/B must authenticate every tracked file");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("complete validation"));
    }

    #[test]
    fn direct_store_preflights_every_footer_before_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let schema6_source = tempfile::tempdir().unwrap();
        let schema7_source = tempfile::tempdir().unwrap();
        let schema6 = copy_segment_into_root(
            &write_segment(schema6_source.path(), 1_000, false, 1),
            tempdir.path(),
        );
        copy_segment_into_root(
            &write_segment(schema7_source.path(), 11_000, true, 2),
            tempdir.path(),
        );
        fs::write(schema6.join(SegmentFile::MetaJson.filename()), b"{").unwrap();

        let error = SegmentStoreReader::open_with_options(tempdir.path(), schema6_options())
            .err()
            .expect("mixed store must fail footer preflight");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("preflight"));
        assert!(error.to_string().contains("schema version"));
    }

    #[test]
    fn manifest_store_preflights_every_footer_before_metadata() {
        let tempdir = tempfile::tempdir().unwrap();
        let schema6_source = tempfile::tempdir().unwrap();
        let schema7_source = tempfile::tempdir().unwrap();
        let schema6 = copy_segment_into_root(
            &write_segment(schema6_source.path(), 1_000, false, 1),
            tempdir.path(),
        );
        let schema7 = copy_segment_into_root(
            &write_segment(schema7_source.path(), 11_000, true, 2),
            tempdir.path(),
        );
        let inventory = ManifestInventory {
            segments: vec![manifest_segment(&schema6), manifest_segment(&schema7)],
        };
        fs::write(schema6.join(SegmentFile::MetaJson.filename()), b"{").unwrap();

        let error = SegmentStoreReader::open_manifest_inventory_with_options(
            tempdir.path(),
            &inventory,
            schema6_options(),
        )
        .err()
        .expect("mixed manifest must fail footer preflight");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("preflight"));
        assert!(error.to_string().contains("schema version"));
    }
}
