use super::*;

pub(super) fn render_benchmark_markdown(
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) -> String {
    let totals = benchmark_totals(report);
    let mut markdown = String::new();

    render_configuration(&mut markdown, config, report);
    render_query_limits(&mut markdown, config);
    render_query_phases(&mut markdown, report);
    render_query_totals(&mut markdown, config, report, &totals);
    render_session_activity(&mut markdown, config, report);
    render_query_summary(&mut markdown, report);
    render_query_results(&mut markdown, report);
    render_query_stage_runs(&mut markdown, report);
    render_range_scalar_cache_runs(&mut markdown, &report.results);
    render_range_execution_runs(&mut markdown, &report.results);
    render_metadata_runtime_runs(&mut markdown, &report.results);
    render_query_read_profiles(&mut markdown, report);
    render_query_label_materialization(&mut markdown, report);
    render_query_payload_locality(&mut markdown, report);

    markdown
}

fn render_configuration(
    markdown: &mut String,
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) {
    markdown.push_str("# Chronoxide Sealed Query Benchmark\n\n");
    markdown.push_str(&format!(
        "- Generated At: {}\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    markdown.push_str(&format!(
        "- Segments Directory: `{}`\n",
        config.segments_dir.display()
    ));
    markdown.push_str(&format!(
        "- Segment Corpus Fingerprint SHA-256: `{}`\n",
        report.corpus_fingerprint
    ));
    markdown.push_str(&format!(
        "- Segment Corpus Fingerprint Duration: {}\n",
        format_duration(report.corpus_fingerprint_duration)
    ));
    markdown.push_str(&format!(
        "- Time Range: {}..{}\n\n",
        config.start_ms,
        format_end_ms(config.end_ms)
    ));
    markdown.push_str(&format!(
        "- Evaluation Mode: {}\n\n",
        query_benchmark_mode_name(config.mode)
    ));
    markdown.push_str(&format!(
        "- Chunk Read Mode: {}\n\n",
        config.chunk_read_mode.name()
    ));
    markdown.push_str(&format!(
        "- Chunk Read Queue Depth: {}\n\n",
        config.chunk_read_queue_depth
    ));
    markdown.push_str(&format!(
        "- Chunk Payload Coalesce Max Gap Bytes: {}\n\n",
        config.chunk_payload_coalesce_max_gap_bytes
    ));
    markdown.push_str(&format!(
        "- Experimental Cross-Segment Chunk Reads: {}\n\n",
        report.experimental_cross_segment_chunk_reads
    ));
    markdown.push_str(&format!(
        "- Label Materialization: {}\n\n",
        report.label_materialization.name()
    ));
    markdown.push_str(&format!(
        "- Query Label Storage: {}\n\n",
        report.label_storage.name()
    ));
    markdown.push_str(&format!(
        "- Query Label Arena Max Bytes: {}\n\n",
        config.query_label_arena_max_bytes
    ));
    markdown.push_str(&format!(
        "- Storage Layout: {}\n\n",
        report.storage_layout.name()
    ));
    markdown.push_str(&format!(
        "- Query Instrumentation: {}\n\n",
        report.query_instrumentation.name()
    ));
    markdown.push_str(&format!(
        "- Range Execution Mode: {}\n\n",
        report.range_execution_mode.name()
    ));
    if report.range_execution_mode == RangeExecutionModeArg::OnePassAssumeScalar {
        markdown.push_str("`one-pass-assume-scalar` is a diagnostic comparator, not a production executor. PromQL syntax cannot prove scalar-only storage, and the current union decode is not protected by a pre-allocation retained-memory governor.\n\n");
    }
    markdown.push_str("Fine-grained stage timing is observer-heavy. Use `off` runs for latency comparisons and separate `detailed` runs for attribution; do not compare their wall times as equivalent measurements. Post-query fingerprinting is timed separately and is outside the query wall. This CLI does not perform API response serialization; API serialization remains a separately measured API-layer boundary.\n\n");
    markdown.push_str(&format!(
        "- Requested Segment Footer Validation: {}\n\n",
        config.validate_segment_footers
    ));
    markdown.push_str(&format!(
        "- Effective Segment Footer Validation: {}\n\n",
        config.validate_segment_footers || report.storage_layout.forces_footer_validation()
    ));
    if let QueryBenchmarkMode::Range { step_ms } = config.mode {
        markdown.push_str(&format!("- Range Step: {step_ms} ms\n\n"));
        markdown.push_str(&format!(
            "- Range Scalar Cache Max Bytes: {}\n\n",
            config
                .range_scalar_cache_max_bytes
                .unwrap_or(DEFAULT_RANGE_SCALAR_CACHE_BUDGET_BYTES)
        ));
        markdown.push_str(&format!(
            "- Scheduled Evaluations Per Run: {}\n\n",
            scheduled_range_evaluations(config.start_ms, config.end_ms, step_ms)
        ));
    }
    markdown.push_str(&format!(
        "- Benchmark Repeats: {}\n\n",
        config.benchmark_repeats
    ));
    markdown.push_str("- Session-local cold means the first run in a fresh Chronoxide query session. Query sessions use shared store caches, so later cold runs can benefit from earlier queries; the benchmark does not flush or bypass the operating-system page cache.\n\n");
    markdown.push_str(&format!(
        "- Prewarm Query Contexts: {}\n\n",
        config.prewarm_query_contexts
    ));
    markdown.push_str(&format!(
        "- Prefetch Query Data: {}\n\n",
        config.prefetch_query_data
    ));
}

fn render_query_limits(markdown: &mut String, config: &QueryBenchmarkConfig) {
    markdown.push_str("## Query Limits\n\n");
    markdown.push_str("| Limit | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| query_max_series_matched | {} |\n",
        format_query_limit(config.limits.max_matched_series)
    ));
    markdown.push_str(&format!(
        "| query_max_projected_series | {} |\n",
        format_query_limit(config.limits.max_projected_series)
    ));
    markdown.push_str(&format!(
        "| query_max_chunks_read | {} |\n",
        format_query_limit(config.limits.max_chunk_reads)
    ));
    markdown.push_str(&format!(
        "| query_max_bytes_read | {} |\n",
        format_query_limit(config.limits.max_bytes_read)
    ));
    markdown.push_str(&format!(
        "| query_max_samples | {} |\n",
        format_query_limit(config.limits.max_samples_decoded)
    ));
    markdown.push_str(&format!(
        "| regex_max_expanded_values | {} |\n\n",
        format_query_limit(config.limits.max_regex_values_examined)
    ));
}

fn render_query_phases(markdown: &mut String, report: &QueryBenchmarkReport) {
    markdown.push_str("## Query Phases\n\n");
    markdown.push_str("| Phase | Duration |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Store Open | {} |\n",
        format_duration(report.store_open)
    ));
    markdown.push_str(&format!(
        "| Query Session Open | {} |\n",
        format_duration(report.query_session_open)
    ));
    markdown.push_str(&format!(
        "| Query Context Prewarm | {} |\n",
        format_duration(report.query_context_prewarm)
    ));
    markdown.push_str(&format!(
        "| Query Data Prefetch | {} |\n",
        format_duration(report.query_data_prefetch)
    ));
    markdown.push_str(&format!(
        "| PromQL Queries | {} |\n",
        format_duration(report.promql_queries)
    ));
    markdown.push_str(&format!(
        "| Post-Query Fingerprints | {} |\n\n",
        format_duration(report.post_query_fingerprints)
    ));
}

fn render_query_totals(
    markdown: &mut String,
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
    totals: &QueryBenchmarkTotals,
) {
    markdown.push_str("## Query Totals\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Queries | {} |\n", config.queries.len()));
    markdown.push_str(&format!("| Query Runs | {} |\n", report.results.len()));
    markdown.push_str(&format!(
        "| Cold Runs | {} |\n",
        report
            .results
            .iter()
            .filter(|result| result.run_kind == QueryBenchmarkRunKind::Cold)
            .count()
    ));
    markdown.push_str(&format!(
        "| Warm Runs | {} |\n",
        report
            .results
            .iter()
            .filter(|result| result.run_kind == QueryBenchmarkRunKind::Warm)
            .count()
    ));
    markdown.push_str(&format!(
        "| Segments Considered | {} |\n",
        totals.stats.segments_considered
    ));
    markdown.push_str(&format!(
        "| Segments Skipped By Time | {} |\n",
        totals.stats.segments_skipped_by_time
    ));
    markdown.push_str(&format!(
        "| Segments Skipped By Missing Equality | {} |\n",
        totals.stats.segments_skipped_by_missing_equality
    ));
    markdown.push_str(&format!(
        "| Segments Skipped By Matcher Time Range | {} |\n",
        totals.stats.segments_skipped_by_matcher_time_range
    ));
    markdown.push_str(&format!(
        "| Segments Queried | {} |\n",
        totals.stats.segments_queried
    ));
    markdown.push_str(&format!("| Result Series | {} |\n", totals.result_series));
    markdown.push_str(&format!("| Result Samples | {} |\n", totals.result_samples));
    markdown.push_str(&format!(
        "| Matched Series | {} |\n",
        totals.stats.matched_series
    ));
    markdown.push_str(&format!(
        "| Projected Series | {} |\n",
        totals.stats.projected_series
    ));
    markdown.push_str(&format!("| Chunk Reads | {} |\n", totals.stats.chunk_reads));
    markdown.push_str(&format!("| Bytes Read | {} |\n", totals.stats.bytes_read));
    markdown.push_str(&format!(
        "| Payload Used Bytes | {} |\n",
        totals.payload_used_bytes
    ));
    markdown.push_str(&format!(
        "| Payload Read Bytes | {} |\n",
        totals.payload_read_bytes
    ));
    markdown.push_str(&format!(
        "| Payload Read / Used | {} |\n",
        format_payload_read_amplification(totals.payload_read_bytes, totals.payload_used_bytes)
    ));
    markdown.push_str(&format!(
        "| Index Postings Reads | {} |\n",
        totals.stats.index_postings_reads
    ));
    markdown.push_str(&format!(
        "| Index Postings Bytes Read | {} |\n",
        totals.stats.index_postings_bytes_read
    ));
    markdown.push_str(&format!(
        "| Samples Decoded | {} |\n",
        totals.stats.samples_decoded
    ));
    markdown.push_str(&format!(
        "| Typed Scalar Chunks Decoded | {} |\n",
        totals.stats.typed_scalar_chunks_decoded
    ));
    markdown.push_str(&format!(
        "| Typed Full Chunks Decoded | {} |\n",
        totals.stats.typed_full_chunks_decoded
    ));
    markdown.push_str(&format!(
        "| Regex Values Examined | {} |\n\n",
        totals.stats.regex_values_examined
    ));
    markdown.push_str("Payload used bytes are the exact encoded chunk ranges selected by measured queries. Payload read bytes are the coalesced `chunks.bin` spans requested by the query reader; they are measured before operating-system caching and do not measure storage-device traffic.\n\n");
}

fn render_session_activity(
    markdown: &mut String,
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) {
    markdown.push_str("## Session File Opens\n\n");
    markdown.push_str("| File | Opens |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Index Routing | {} |\n",
        report.session_stats.index_routing_opens
    ));
    markdown.push_str(&format!(
        "| Segment Contexts | {} |\n",
        report.session_stats.segment_context_opens
    ));
    markdown.push_str(&format!(
        "| Symbols | {} |\n",
        report.session_stats.symbols_bin_opens
    ));
    markdown.push_str(&format!(
        "| Indexes | {} |\n",
        report.session_stats.indexes_puffin_opens
    ));
    markdown.push_str(&format!(
        "| Series | {} |\n",
        report.session_stats.series_bin_opens
    ));
    markdown.push_str(&format!(
        "| Chunk Index | {} |\n",
        report.session_stats.chunk_index_bin_opens
    ));
    markdown.push_str(&format!(
        "| Chunks | {} |\n\n",
        report.session_stats.chunks_bin_opens
    ));
    render_profile_table(markdown, "Session Read Profile", report.session_profile);

    if config.prewarm_query_contexts {
        markdown.push_str("## Query Context Prewarm File Opens\n\n");
        markdown.push_str("| File | Opens |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Index Routing | {} |\n",
            report.query_context_prewarm_stats_delta.index_routing_opens
        ));
        markdown.push_str(&format!(
            "| Segment Contexts | {} |\n",
            report
                .query_context_prewarm_stats_delta
                .segment_context_opens
        ));
        markdown.push_str(&format!(
            "| Symbols | {} |\n",
            report.query_context_prewarm_stats_delta.symbols_bin_opens
        ));
        markdown.push_str(&format!(
            "| Indexes | {} |\n",
            report
                .query_context_prewarm_stats_delta
                .indexes_puffin_opens
        ));
        markdown.push_str(&format!(
            "| Series | {} |\n",
            report.query_context_prewarm_stats_delta.series_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunk Index | {} |\n",
            report
                .query_context_prewarm_stats_delta
                .chunk_index_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunks | {} |\n\n",
            report.query_context_prewarm_stats_delta.chunks_bin_opens
        ));
        render_profile_table(
            markdown,
            "Query Context Prewarm Read Profile",
            report.query_context_prewarm_profile_delta,
        );
    }

    if config.prefetch_query_data {
        markdown.push_str("## Query Data Prefetch\n\n");
        markdown.push_str("| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Segments Considered | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_considered
        ));
        markdown.push_str(&format!(
            "| Segments Skipped By Time | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_skipped_by_time
        ));
        markdown.push_str(&format!(
            "| Segments Skipped By Missing Equality | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_skipped_by_missing_equality
        ));
        markdown.push_str(&format!(
            "| Segments Skipped By Matcher Time Range | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_skipped_by_matcher_time_range
        ));
        markdown.push_str(&format!(
            "| Segments Prefetched | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .segments_queried
        ));
        markdown.push_str(&format!(
            "| Matched Series | {} |\n",
            report.query_data_prefetch_stats.query_stats.matched_series
        ));
        markdown.push_str(&format!(
            "| Series Entries Read | {} |\n",
            report.query_data_prefetch_stats.series_entries_read
        ));
        markdown.push_str(&format!(
            "| Chunk Index Reads | {} |\n",
            report.query_data_prefetch_stats.chunk_index_reads
        ));
        markdown.push_str(&format!(
            "| Chunk Index Bytes Read | {} |\n",
            report.query_data_prefetch_stats.chunk_index_bytes_read
        ));
        markdown.push_str(&format!(
            "| Chunk Prefetch Reads | {} |\n",
            report.query_data_prefetch_stats.query_stats.chunk_reads
        ));
        markdown.push_str(&format!(
            "| Chunk Prefetch Bytes | {} |\n",
            report.query_data_prefetch_stats.query_stats.bytes_read
        ));
        markdown.push_str(&format!(
            "| Index Postings Reads | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .index_postings_reads
        ));
        markdown.push_str(&format!(
            "| Index Postings Bytes Read | {} |\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .index_postings_bytes_read
        ));
        markdown.push_str(&format!(
            "| Regex Values Examined | {} |\n\n",
            report
                .query_data_prefetch_stats
                .query_stats
                .regex_values_examined
        ));

        markdown.push_str("## Query Data Prefetch File Opens\n\n");
        markdown.push_str("| File | Opens |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Index Routing | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .index_routing_opens
        ));
        markdown.push_str(&format!(
            "| Segment Contexts | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .segment_context_opens
        ));
        markdown.push_str(&format!(
            "| Symbols | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .symbols_bin_opens
        ));
        markdown.push_str(&format!(
            "| Indexes | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .indexes_puffin_opens
        ));
        markdown.push_str(&format!(
            "| Series | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .series_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunk Index | {} |\n",
            report
                .query_data_prefetch_session_stats_delta
                .chunk_index_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunks | {} |\n\n",
            report
                .query_data_prefetch_session_stats_delta
                .chunks_bin_opens
        ));
        render_profile_table(
            markdown,
            "Query Data Prefetch Read Profile",
            report.query_data_prefetch_profile_delta,
        );
    }
}

fn render_query_summary(markdown: &mut String, report: &QueryBenchmarkReport) {
    markdown.push_str("## Cold/Warm Query Summary\n\n");
    markdown.push_str("| Query | Cold Runs | Warm Runs | Cold Duration | Warm Mean | Warm Median | Warm Min | Warm Max | Result Series | Result Samples |\n");
    markdown.push_str("| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for (query, summary) in benchmark_run_summaries(report) {
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&query),
            summary.cold_runs,
            summary.warm_runs,
            format_optional_duration(summary.cold_duration),
            format_optional_duration(summary.warm_mean_duration()),
            format_optional_duration(summary.warm_median_duration()),
            format_optional_duration(summary.warm_min_duration),
            format_optional_duration(summary.warm_max_duration),
            summary.result_series,
            summary.result_samples
        ));
    }
    markdown.push('\n');
}

fn render_query_results(markdown: &mut String, report: &QueryBenchmarkReport) {
    markdown.push_str("## Query Results\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Query Session Open | Duration | semantic_fingerprint_sha256 | context_opens_delta | symbols_opens_delta | series_opens_delta | chunk_index_opens_delta | chunks_opens_delta | routing_opens_delta | indexes_opens_delta | segments_considered | segments_skipped_by_time | segments_skipped_by_missing_equality | segments_skipped_by_matcher_time_range | segments_queried | result_series | result_samples | matched_series | projected_series | chunk_reads | bytes_read | index_postings_reads | index_postings_bytes_read | samples_decoded | typed_scalar_chunks_decoded | typed_full_chunks_decoded | regex_values_examined | payload_used_bytes | payload_read_bytes | payload_read_over_used |\n");
    markdown.push_str(
        "| --- | --- | ---: | ---: | ---: | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n",
    );
    for result in &report.results {
        let payload_used_bytes = result.session_profile_delta.chunk_payload_bytes;
        let payload_read_bytes = result.session_profile_delta.chunk_payload_physical_bytes;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            format_duration(result.query_session_open),
            format_duration(result.duration),
            result.semantic_fingerprint,
            result.session_stats_delta.segment_context_opens,
            result.session_stats_delta.symbols_bin_opens,
            result.session_stats_delta.series_bin_opens,
            result.session_stats_delta.chunk_index_bin_opens,
            result.session_stats_delta.chunks_bin_opens,
            result.session_stats_delta.index_routing_opens,
            result.session_stats_delta.indexes_puffin_opens,
            result.stats.segments_considered,
            result.stats.segments_skipped_by_time,
            result.stats.segments_skipped_by_missing_equality,
            result.stats.segments_skipped_by_matcher_time_range,
            result.stats.segments_queried,
            result.result_series,
            result.result_samples,
            result.stats.matched_series,
            result.stats.projected_series,
            result.stats.chunk_reads,
            result.stats.bytes_read,
            result.stats.index_postings_reads,
            result.stats.index_postings_bytes_read,
            result.stats.samples_decoded,
            result.stats.typed_scalar_chunks_decoded,
            result.stats.typed_full_chunks_decoded,
            result.stats.regex_values_examined,
            payload_used_bytes,
            payload_read_bytes,
            format_payload_read_amplification(payload_read_bytes, payload_used_bytes)
        ));
    }
}

fn render_query_read_profiles(markdown: &mut String, report: &QueryBenchmarkReport) {
    markdown.push_str("\n## Query Result Read Profiles\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | routing_open_delta | context_open_delta | indexes_open_delta | symbols_read_delta | series_open_delta | chunk_index_open_delta | chunks_open_delta | routing_read_delta | postings_read_delta | metric_series_ranges_read_delta | series_entry_read_delta | chunk_index_range_read_delta | chunk_read_delta | routing_opened_file_size_bytes_delta | indexes_opened_file_size_bytes_delta | symbols_opened_file_size_bytes_delta | series_opened_file_size_bytes_delta | chunk_index_opened_file_size_bytes_delta | chunks_opened_file_size_bytes_delta | routing_index_bytes_delta | postings_bytes_delta | metric_series_ranges_bytes_delta | series_entries_read_delta | series_entry_read_batches_delta | series_entry_bytes_delta | chunk_index_range_bytes_delta | chunk_payload_bytes_delta | chunk_payload_physical_reads_delta | chunk_payload_physical_bytes_delta |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in &report.results {
        let profile = result.session_profile_delta;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            format_duration(profile.index_routing_open),
            format_duration(profile.segment_context_open),
            format_duration(profile.indexes_open),
            format_duration(profile.symbols_read),
            format_duration(profile.series_open),
            format_duration(profile.chunk_index_open),
            format_duration(profile.chunks_open),
            format_duration(profile.routing_index_read),
            format_duration(profile.exact_postings_read),
            format_duration(profile.metric_series_ranges_read),
            format_duration(profile.series_entry_read),
            format_duration(profile.chunk_index_range_read),
            format_duration(profile.chunk_read),
            profile.index_routing_file_bytes,
            profile.indexes_file_bytes,
            profile.symbols_file_bytes,
            profile.series_file_bytes,
            profile.chunk_index_file_bytes,
            profile.chunks_file_bytes,
            profile.routing_index_bytes,
            profile.exact_postings_bytes,
            profile.metric_series_ranges_bytes,
            profile.series_entries_read,
            profile.series_entry_read_batches,
            profile.series_entry_bytes,
            profile.chunk_index_range_bytes,
            profile.chunk_payload_bytes,
            profile.chunk_payload_physical_reads,
            profile.chunk_payload_physical_bytes
        ));
    }

    render_query_result_index_positional_reads(markdown, &report.results);
    render_query_result_symbol_reads(markdown, &report.results);
}

fn render_query_label_materialization(markdown: &mut String, report: &QueryBenchmarkReport) {
    markdown.push_str("\n## Query Result Label Materialization\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Rows Integrity-Checked | Pairs Integrity-Checked | Full Rows | Selective Rows | Pairs Materialized | Pairs Omitted | Content Bytes Materialized |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in &report.results {
        let profile = result.session_profile_delta;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            profile.label_rows_integrity_checked,
            profile.label_pairs_integrity_checked,
            profile.label_rows_full_materialized,
            profile.label_rows_selectively_materialized,
            profile.label_pairs_materialized,
            profile.label_pairs_omitted,
            profile.label_content_bytes_materialized,
        ));
    }

    render_query_label_storage(markdown, &report.results);
}

fn render_query_payload_locality(markdown: &mut String, report: &QueryBenchmarkReport) {
    markdown.push_str("\n## Query Result Chunk Payload Locality\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | payload_read_ranges | forward_gaps | forward_gap_bytes | backward_jumps | contiguous_runs | contiguous_span_bytes | coalesced_4k_runs | coalesced_4k_span_bytes | coalesced_64k_runs | coalesced_64k_span_bytes | sorted_contiguous_runs | sorted_contiguous_span_bytes | sorted_coalesced_4k_runs | sorted_coalesced_4k_span_bytes | sorted_coalesced_64k_runs | sorted_coalesced_64k_span_bytes |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in &report.results {
        let locality = result.session_profile_delta.chunk_payload_locality;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            locality.reads,
            locality.forward_gaps,
            locality.forward_gap_bytes,
            locality.backward_jumps,
            locality.contiguous_runs,
            locality.contiguous_span_bytes,
            locality.coalesced_4k_runs,
            locality.coalesced_4k_span_bytes,
            locality.coalesced_64k_runs,
            locality.coalesced_64k_span_bytes,
            locality.sorted_contiguous_runs,
            locality.sorted_contiguous_span_bytes,
            locality.sorted_coalesced_4k_runs,
            locality.sorted_coalesced_4k_span_bytes,
            locality.sorted_coalesced_64k_runs,
            locality.sorted_coalesced_64k_span_bytes
        ));
    }
}
