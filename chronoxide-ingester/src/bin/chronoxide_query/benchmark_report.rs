fn render_benchmark_markdown(
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) -> String {
    let totals = benchmark_totals(report);
    let mut markdown = String::new();

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
        "- Storage Layout: {}\n\n",
        report.storage_layout.name()
    ));
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
        "| PromQL Queries | {} |\n\n",
        format_duration(report.promql_queries)
    ));

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
    render_profile_table(
        &mut markdown,
        "Session Read Profile",
        report.session_profile,
    );

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
            &mut markdown,
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
            &mut markdown,
            "Query Data Prefetch Read Profile",
            report.query_data_prefetch_profile_delta,
        );
    }

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

    render_range_scalar_cache_runs(&mut markdown, &report.results);

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

    render_query_result_index_positional_reads(&mut markdown, &report.results);
    render_query_result_symbol_reads(&mut markdown, &report.results);

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

    render_query_label_storage(&mut markdown, &report.results);

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

    markdown
}

fn render_query_label_storage(markdown: &mut String, results: &[QueryBenchmarkResult]) {
    markdown.push_str("\n## Experimental Query Label Storage\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Label Sets | Atom Lookups | Atom Hits | Atom Misses | Unique Content Bytes |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in results {
        let stats = result.label_storage_delta;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            stats.label_sets,
            stats.atom_lookups,
            stats.atom_hits,
            stats.atom_misses,
            stats.unique_content_bytes,
        ));
    }
}

fn render_range_scalar_cache_runs(markdown: &mut String, results: &[QueryBenchmarkResult]) {
    if !results
        .iter()
        .any(|result| result.range_scalar_cache.is_some())
    {
        return;
    }

    markdown.push_str("\n## Range Scalar Cache Runs\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | configured_budget_bytes | governor_lease_bytes | governor_refused | allocation_refused | layout_overflow | entry_arena_charge_bytes | sample_arena_charge_bytes | hits | misses | admitted_entries | streaming_budget_bypasses | unsupported_bypasses | logical_hit_bytes | logical_miss_or_bypass_bytes | peak_retained_charge_bytes | retained_charge_after_finalize | process_governor_limit_bytes | process_governor_current_leased_bytes | process_governor_peak_leased_bytes |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in results {
        let Some(cache) = result.range_scalar_cache else {
            continue;
        };
        let summary = cache.summary;
        let governor = cache.process_governor;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            summary.configured_budget_bytes,
            summary.governor_lease_bytes,
            summary.governor_refused,
            summary.allocation_refused,
            summary.layout_overflow,
            summary.entry_arena_charge_bytes,
            summary.sample_arena_charge_bytes,
            summary.hits,
            summary.misses,
            summary.admitted_entries,
            summary.streaming_budget_bypasses,
            summary.unsupported_bypasses,
            summary.logical_hit_bytes,
            summary.logical_miss_or_bypass_bytes,
            summary.peak_retained_charge_bytes,
            summary.retained_charge_after_finalize,
            governor.limit_bytes,
            governor.current_leased_bytes,
            governor.peak_leased_bytes,
        ));
    }
}

fn render_profile_table(markdown: &mut String, title: &str, profile: SegmentStoreQueryProfile) {
    if !markdown.ends_with("\n\n") {
        markdown.push('\n');
    }
    markdown.push_str(&format!("## {title}\n\n"));
    markdown.push_str("Opened file size bytes are summed file lengths observed when a file is opened. Logical read bytes are explicit byte ranges requested by the query path. Physical chunk payload spans are the coalesced ranges issued by the query reader before OS caching effects.\n\n");
    let split_title = title.strip_suffix(" Read Profile").unwrap_or(title);
    markdown.push_str(&format!("## {split_title} Opened File Sizes\n\n"));
    markdown.push_str("| Stage | Duration | Opened File Size Bytes |\n");
    markdown.push_str("| --- | ---: | ---: |\n");
    markdown.push_str(&format!(
        "| Index Routing Open | {} | {} |\n",
        format_duration(profile.index_routing_open),
        profile.index_routing_file_bytes
    ));
    markdown.push_str(&format!(
        "| Segment Context Open | {} | 0 |\n",
        format_duration(profile.segment_context_open)
    ));
    markdown.push_str(&format!(
        "| indexes.puffin | {} | {} |\n",
        format_duration(profile.indexes_open),
        profile.indexes_file_bytes
    ));
    markdown.push_str(&format!(
        "| symbols.bin | {} | {} |\n",
        format_duration(profile.symbols_read),
        profile.symbols_file_bytes
    ));
    markdown.push_str(&format!(
        "| series.bin | {} | {} |\n",
        format_duration(profile.series_open),
        profile.series_file_bytes
    ));
    markdown.push_str(&format!(
        "| chunk_index.bin | {} | {} |\n",
        format_duration(profile.chunk_index_open),
        profile.chunk_index_file_bytes
    ));
    markdown.push_str(&format!(
        "| chunks.bin | {} | {} |\n",
        format_duration(profile.chunks_open),
        profile.chunks_file_bytes
    ));

    markdown.push_str(&format!("\n## {split_title} Logical Read Bytes\n\n"));
    markdown.push_str("| Stage | Duration | Read Bytes | Count |\n");
    markdown.push_str("| --- | ---: | ---: | ---: |\n");
    markdown.push_str(&format!(
        "| Routing Index Blob | {} | {} | - |\n",
        format_duration(profile.routing_index_read),
        profile.routing_index_bytes
    ));
    markdown.push_str(&format!(
        "| Exact Postings | {} | {} | - |\n",
        format_duration(profile.exact_postings_read),
        profile.exact_postings_bytes
    ));
    markdown.push_str(&format!(
        "| Metric Series Ranges | {} | {} | - |\n",
        format_duration(profile.metric_series_ranges_read),
        profile.metric_series_ranges_bytes
    ));
    markdown.push_str(&format!(
        "| Series Entries | {} | {} | {} |\n",
        format_duration(profile.series_entry_read),
        profile.series_entry_bytes,
        profile.series_entries_read
    ));
    markdown.push_str(&format!(
        "| Series Entry Batches | - | - | {} |\n",
        profile.series_entry_read_batches
    ));
    markdown.push_str(&format!(
        "| Chunk Index Ranges | {} | {} | - |\n",
        format_duration(profile.chunk_index_range_read),
        profile.chunk_index_range_bytes
    ));
    markdown.push_str(&format!(
        "| Chunk Payloads | {} | {} | - |\n\n",
        format_duration(profile.chunk_read),
        profile.chunk_payload_bytes
    ));

    render_index_positional_read_table(
        markdown,
        &format!("{split_title} Index Positional Reads"),
        profile.index_read_stats,
    );
    render_symbol_read_table(
        markdown,
        &format!("{split_title} Symbol Reads And Page Cache"),
        profile.symbol_read_stats,
        profile.symbol_resources,
    );

    markdown.push_str(&format!(
        "## {split_title} Physical Chunk Payload Spans\n\n"
    ));
    markdown.push_str("| Stage | Duration | Span Bytes | Span Reads |\n");
    markdown.push_str("| --- | ---: | ---: | ---: |\n");
    markdown.push_str(&format!(
        "| Chunk Payload Spans | {} | {} | {} |\n\n",
        format_duration(profile.chunk_read),
        profile.chunk_payload_physical_bytes,
        profile.chunk_payload_physical_reads
    ));

    let locality = profile.chunk_payload_locality;
    markdown.push_str(&format!("## {split_title} Chunk Payload Locality\n\n"));
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Payload Read Ranges | {} |\n", locality.reads));
    markdown.push_str(&format!("| Forward Gaps | {} |\n", locality.forward_gaps));
    markdown.push_str(&format!(
        "| Forward Gap Bytes | {} |\n",
        locality.forward_gap_bytes
    ));
    markdown.push_str(&format!(
        "| Backward Jumps | {} |\n",
        locality.backward_jumps
    ));
    markdown.push_str(&format!(
        "| Strict Contiguous Runs | {} |\n",
        locality.contiguous_runs
    ));
    markdown.push_str(&format!(
        "| Strict Contiguous Span Bytes | {} |\n",
        locality.contiguous_span_bytes
    ));
    markdown.push_str(&format!(
        "| Coalesced 4KiB Runs | {} |\n",
        locality.coalesced_4k_runs
    ));
    markdown.push_str(&format!(
        "| Coalesced 4KiB Span Bytes | {} |\n",
        locality.coalesced_4k_span_bytes
    ));
    markdown.push_str(&format!(
        "| Coalesced 64KiB Runs | {} |\n",
        locality.coalesced_64k_runs
    ));
    markdown.push_str(&format!(
        "| Coalesced 64KiB Span Bytes | {} |\n",
        locality.coalesced_64k_span_bytes
    ));
    markdown.push_str(&format!(
        "| Sorted Strict Contiguous Runs | {} |\n",
        locality.sorted_contiguous_runs
    ));
    markdown.push_str(&format!(
        "| Sorted Strict Contiguous Span Bytes | {} |\n",
        locality.sorted_contiguous_span_bytes
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 4KiB Runs | {} |\n",
        locality.sorted_coalesced_4k_runs
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 4KiB Span Bytes | {} |\n",
        locality.sorted_coalesced_4k_span_bytes
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 64KiB Runs | {} |\n",
        locality.sorted_coalesced_64k_runs
    ));
    markdown.push_str(&format!(
        "| Sorted Coalesced 64KiB Span Bytes | {} |\n\n",
        locality.sorted_coalesced_64k_span_bytes
    ));

    let scheduler = profile.chunk_read_scheduler;
    markdown.push_str(&format!("## {split_title} Chunk Read Scheduler\n\n"));
    markdown.push_str("Scheduler counters are profile-only and do not change PromQL `QueryStats`. Submission depth counts backend submissions; logical requests and physical spans describe the shared plan before decoding.\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Executions | {} |\n", scheduler.executions));
    markdown.push_str(&format!(
        "| Pread Decisions | {} |\n",
        scheduler.pread_decisions
    ));
    markdown.push_str(&format!(
        "| io_uring Decisions | {} |\n",
        scheduler.io_uring_decisions
    ));
    markdown.push_str(&format!(
        "| Logical Requests | {} |\n",
        scheduler.logical_requests
    ));
    markdown.push_str(&format!(
        "| Physical Spans | {} |\n",
        scheduler.physical_spans
    ));
    markdown.push_str(&format!(
        "| Backend Submissions | {} |\n",
        scheduler.backend_submissions
    ));
    markdown.push_str(&format!(
        "| SQEs Submitted | {} |\n",
        scheduler.sqes_submitted
    ));
    markdown.push_str(&format!(
        "| Submission Depth Sum | {} |\n",
        scheduler.submission_depth_sum
    ));
    let mean_submission_depth = if scheduler.backend_submissions == 0 {
        "—".to_string()
    } else {
        format!(
            "{:.3}",
            scheduler.submission_depth_sum as f64 / scheduler.backend_submissions as f64
        )
    };
    markdown.push_str(&format!(
        "| Mean Submission Depth | {mean_submission_depth} |\n"
    ));
    markdown.push_str(&format!(
        "| Maximum Submission Depth | {} |\n",
        scheduler.submission_depth_max
    ));
    markdown.push_str(&format!(
        "| Depth 1 Submissions | {} |\n",
        scheduler.submission_depth_1
    ));
    markdown.push_str(&format!(
        "| Depth 2-3 Submissions | {} |\n",
        scheduler.submission_depth_2_3
    ));
    markdown.push_str(&format!(
        "| Depth 4-7 Submissions | {} |\n",
        scheduler.submission_depth_4_7
    ));
    markdown.push_str(&format!(
        "| Depth 8+ Submissions | {} |\n",
        scheduler.submission_depth_8_plus
    ));
    markdown.push_str(&format!(
        "| Total In-Flight Bytes | {} |\n",
        scheduler.in_flight_bytes
    ));
    markdown.push_str(&format!(
        "| Peak In-Flight Bytes | {} |\n\n",
        scheduler.peak_in_flight_bytes
    ));
}

fn render_index_positional_read_table(
    markdown: &mut String,
    title: &str,
    stats: SegmentIndexReadStats,
) {
    ensure_markdown_section_spacing(markdown);
    markdown.push_str(&format!("## {title}\n\n"));
    markdown.push_str("These counts are successful positional-read requests and the bytes requested by them, not physical syscalls.\n\n");
    markdown.push_str("| Category | Successful Positional-Read Requests | Requested Bytes |\n");
    markdown.push_str("| --- | ---: | ---: |\n");
    for (category, count) in index_positional_read_rows(stats) {
        markdown.push_str(&format!(
            "| {category} | {} | {} |\n",
            count.calls, count.bytes
        ));
    }
    markdown.push('\n');
}

fn render_query_result_index_positional_reads(
    markdown: &mut String,
    results: &[QueryBenchmarkResult],
) {
    ensure_markdown_section_spacing(markdown);
    markdown.push_str("## Query Result Index Positional Reads\n\n");
    markdown.push_str("These counts are successful positional-read requests and the bytes requested by them, not physical syscalls.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Category | Successful Positional-Read Requests | Requested Bytes |\n");
    markdown.push_str("| --- | --- | ---: | --- | ---: | ---: |\n");
    for result in results {
        for (category, count) in
            index_positional_read_rows(result.session_profile_delta.index_read_stats)
        {
            markdown.push_str(&format!(
                "| `{}` | {} | {} | {category} | {} | {} |\n",
                markdown_escape_inline(&result.query),
                run_kind_name(result.run_kind),
                result.run_index,
                count.calls,
                count.bytes
            ));
        }
    }
    markdown.push('\n');
}

fn render_symbol_read_table(
    markdown: &mut String,
    title: &str,
    stats: SegmentSymbolReadStats,
    resources: SegmentStoreSymbolResources,
) {
    ensure_markdown_section_spacing(markdown);
    markdown.push_str(&format!("## {title}\n\n"));
    markdown.push_str("Legacy eager, root, and page counts are successful positional-read requests and requested bytes, not physical syscalls or storage-device traffic. Read and cache-event values are phase deltas. Resource values are one deduplicated post-phase store snapshot: roots, eager dictionaries, retained files, and page caches are counted once per shared reader state even when several sessions clone it. The page-cache maximum is still the sum of fixed per-reader capacities, not a global governor.\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Legacy Eager Read Requests | {} |\n",
        stats.legacy_eager.calls
    ));
    markdown.push_str(&format!(
        "| Legacy Eager Read Bytes | {} |\n",
        stats.legacy_eager.bytes
    ));
    markdown.push_str(&format!(
        "| Logical Values Returned | {} |\n",
        stats.logical_returned.calls
    ));
    markdown.push_str(&format!(
        "| Logical UTF-8 Bytes Returned | {} |\n",
        stats.logical_returned.bytes
    ));
    markdown.push_str(&format!("| Root Read Requests | {} |\n", stats.root.calls));
    markdown.push_str(&format!("| Root Read Bytes | {} |\n", stats.root.bytes));
    markdown.push_str(&format!("| Page Read Requests | {} |\n", stats.page.calls));
    markdown.push_str(&format!("| Page Read Bytes | {} |\n", stats.page.bytes));
    markdown.push_str(&format!(
        "| Page Read / Logical UTF-8 Amplification | {} |\n",
        format_payload_read_amplification(stats.page.bytes, stats.logical_returned.bytes)
    ));
    markdown.push_str(&format!(
        "| Successful Page Validations | {} |\n",
        stats.page_validation.calls
    ));
    markdown.push_str(&format!(
        "| Successfully Validated Page Bytes | {} |\n",
        stats.page_validation.bytes
    ));
    markdown.push_str(&format!(
        "| Page Validation Duration | {} |\n",
        format_duration(Duration::from_nanos(stats.page_validation_ns))
    ));
    markdown.push_str(&format!(
        "| Touched Corrupt Pages | {} |\n",
        stats.touched_corrupt_pages
    ));
    markdown.push_str(&format!(
        "| Page Cache Hits | {} |\n",
        stats.page_cache_hits
    ));
    markdown.push_str(&format!(
        "| Page Cache Misses | {} |\n",
        stats.page_cache_misses
    ));
    markdown.push_str(&format!(
        "| Page Cache Evictions | {} |\n",
        stats.page_cache_evictions
    ));
    markdown.push_str(&format!(
        "| Retained Symbol Readers | {} |\n",
        resources.retained_readers
    ));
    markdown.push_str(&format!(
        "| Retained Symbol Open Files | {} |\n",
        resources.retained_open_files
    ));
    markdown.push_str(&format!(
        "| Retained Symbol Source File Bytes | {} |\n",
        resources.source_file_bytes
    ));
    markdown.push_str(&format!(
        "| Encoded Root Bytes | {} |\n",
        resources.root_encoded_bytes
    ));
    markdown.push_str(&format!(
        "| Retained Root Charge Bytes | {} |\n",
        resources.root_retained_charge_bytes
    ));
    markdown.push_str(&format!(
        "| Retained Eager Dictionary Charge Bytes | {} |\n",
        resources.eager_dictionary_retained_charge_bytes
    ));
    markdown.push_str(&format!(
        "| Page Cache Charge Bytes | {} |\n",
        resources.page_cache_charge_bytes
    ));
    markdown.push_str(&format!(
        "| Page Cache Max Bytes | {} |\n",
        resources.page_cache_max_bytes
    ));
    markdown.push_str(&format!(
        "| Total Retained Symbol Charge Bytes | {} |\n",
        resources.total_retained_charge_bytes()
    ));
    markdown.push_str(&format!(
        "| Resource Snapshot Errors | {} |\n\n",
        resources.snapshot_errors
    ));
}

fn render_query_result_symbol_reads(markdown: &mut String, results: &[QueryBenchmarkResult]) {
    ensure_markdown_section_spacing(markdown);
    markdown.push_str("## Query Result Symbol Reads And Page Cache\n\n");
    markdown.push_str("Read and cache-event counters are per-run deltas. Resource fields are deduplicated post-run store snapshots and include resources retained by earlier sessions.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Legacy Eager Read Requests Delta | Legacy Eager Read Bytes Delta | Logical Values Returned Delta | Logical UTF-8 Bytes Returned Delta | Root Read Requests Delta | Root Read Bytes Delta | Page Read Requests Delta | Page Read Bytes Delta | Page Read / Logical UTF-8 Amplification | Successful Page Validations Delta | Successfully Validated Page Bytes Delta | Page Validation Nanoseconds Delta | Touched Corrupt Pages Delta | Page Cache Hits Delta | Page Cache Misses Delta | Page Cache Evictions Delta | Retained Symbol Readers After Run | Retained Symbol Open Files After Run | Encoded Root Bytes After Run | Retained Root Charge Bytes After Run | Retained Eager Dictionary Charge Bytes After Run | Page Cache Charge Bytes After Run | Page Cache Max Bytes After Run | Total Retained Symbol Charge Bytes After Run | Resource Snapshot Errors |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in results {
        let profile = result.session_profile_delta;
        let stats = profile.symbol_read_stats;
        let resources = profile.symbol_resources;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            stats.legacy_eager.calls,
            stats.legacy_eager.bytes,
            stats.logical_returned.calls,
            stats.logical_returned.bytes,
            stats.root.calls,
            stats.root.bytes,
            stats.page.calls,
            stats.page.bytes,
            format_payload_read_amplification(stats.page.bytes, stats.logical_returned.bytes),
            stats.page_validation.calls,
            stats.page_validation.bytes,
            stats.page_validation_ns,
            stats.touched_corrupt_pages,
            stats.page_cache_hits,
            stats.page_cache_misses,
            stats.page_cache_evictions,
            resources.retained_readers,
            resources.retained_open_files,
            resources.root_encoded_bytes,
            resources.root_retained_charge_bytes,
            resources.eager_dictionary_retained_charge_bytes,
            resources.page_cache_charge_bytes,
            resources.page_cache_max_bytes,
            resources.total_retained_charge_bytes(),
            resources.snapshot_errors,
        ));
    }
    markdown.push('\n');
}

fn index_positional_read_rows(
    stats: SegmentIndexReadStats,
) -> [(&'static str, SegmentIndexReadCount); 7] {
    [
        ("Root", stats.root),
        ("Routing", stats.routing),
        ("Exact Directory", stats.exact_directory),
        ("Exact Page", stats.exact_page),
        ("Auxiliary Directory", stats.auxiliary_directory),
        ("Payload", stats.payload),
        (
            "Total",
            SegmentIndexReadCount {
                calls: stats.total_calls(),
                bytes: stats.total_bytes(),
            },
        ),
    ]
}

fn ensure_markdown_section_spacing(markdown: &mut String) {
    if markdown.is_empty() || markdown.ends_with("\n\n") {
        return;
    }
    if !markdown.ends_with('\n') {
        markdown.push('\n');
    }
    markdown.push('\n');
}

fn add_query_data_prefetch_stats(total: &mut QueryDataPrefetchStats, next: QueryDataPrefetchStats) {
    add_query_stats(&mut total.query_stats, next.query_stats);
    total.series_entries_read = total
        .series_entries_read
        .saturating_add(next.series_entries_read);
    total.chunk_index_reads = total
        .chunk_index_reads
        .saturating_add(next.chunk_index_reads);
    total.chunk_index_bytes_read = total
        .chunk_index_bytes_read
        .saturating_add(next.chunk_index_bytes_read);
}

#[derive(Debug, Clone, Default, PartialEq)]
struct QueryBenchmarkTotals {
    result_series: u64,
    result_samples: u64,
    payload_used_bytes: u64,
    payload_read_bytes: u64,
    stats: QueryStats,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct QueryBenchmarkRunSummary {
    cold_runs: u64,
    warm_runs: u64,
    cold_duration: Option<Duration>,
    warm_total_duration: Duration,
    warm_durations: Vec<Duration>,
    warm_min_duration: Option<Duration>,
    warm_max_duration: Option<Duration>,
    result_series: u64,
    result_samples: u64,
}

impl QueryBenchmarkRunSummary {
    fn warm_mean_duration(&self) -> Option<Duration> {
        if self.warm_runs == 0 {
            return None;
        }
        Some(duration_div(self.warm_total_duration, self.warm_runs))
    }

    fn warm_median_duration(&self) -> Option<Duration> {
        median_duration(self.warm_durations.clone())
    }
}

fn benchmark_totals(report: &QueryBenchmarkReport) -> QueryBenchmarkTotals {
    let mut totals = QueryBenchmarkTotals::default();
    for result in &report.results {
        totals.result_series = totals.result_series.saturating_add(result.result_series);
        totals.result_samples = totals.result_samples.saturating_add(result.result_samples);
        totals.payload_used_bytes = totals
            .payload_used_bytes
            .saturating_add(result.session_profile_delta.chunk_payload_bytes);
        totals.payload_read_bytes = totals
            .payload_read_bytes
            .saturating_add(result.session_profile_delta.chunk_payload_physical_bytes);
        add_query_stats(&mut totals.stats, result.stats);
    }
    totals
}

fn benchmark_run_summaries(
    report: &QueryBenchmarkReport,
) -> BTreeMap<String, QueryBenchmarkRunSummary> {
    let mut summaries = BTreeMap::new();
    for result in &report.results {
        let summary = summaries
            .entry(result.query.clone())
            .or_insert_with(QueryBenchmarkRunSummary::default);
        match result.run_kind {
            QueryBenchmarkRunKind::Cold => {
                summary.cold_runs = summary.cold_runs.saturating_add(1);
                summary.cold_duration = Some(result.duration);
                summary.result_series = result.result_series;
                summary.result_samples = result.result_samples;
            }
            QueryBenchmarkRunKind::Warm => {
                summary.warm_runs = summary.warm_runs.saturating_add(1);
                summary.warm_total_duration =
                    summary.warm_total_duration.saturating_add(result.duration);
                summary.warm_durations.push(result.duration);
                summary.warm_min_duration = Some(
                    summary
                        .warm_min_duration
                        .map(|duration| duration.min(result.duration))
                        .unwrap_or(result.duration),
                );
                summary.warm_max_duration = Some(
                    summary
                        .warm_max_duration
                        .map(|duration| duration.max(result.duration))
                        .unwrap_or(result.duration),
                );
                if summary.result_series == 0 {
                    summary.result_series = result.result_series;
                }
                if summary.result_samples == 0 {
                    summary.result_samples = result.result_samples;
                }
            }
        }
    }
    summaries
}

fn run_kind_name(kind: QueryBenchmarkRunKind) -> &'static str {
    match kind {
        QueryBenchmarkRunKind::Cold => "Cold",
        QueryBenchmarkRunKind::Warm => "Warm",
    }
}

fn raw_run_kind_name(kind: QueryBenchmarkRunKind) -> &'static str {
    match kind {
        QueryBenchmarkRunKind::Cold => "cold",
        QueryBenchmarkRunKind::Warm => "warm",
    }
}

fn query_benchmark_mode_name(mode: QueryBenchmarkMode) -> &'static str {
    match mode {
        QueryBenchmarkMode::Instant => "instant",
        QueryBenchmarkMode::Range { .. } => "query_range",
    }
}

fn scheduled_range_evaluations(start_ms: u64, end_ms: u64, step_ms: u64) -> u128 {
    u128::from(end_ms - start_ms) / u128::from(step_ms) + 1
}

fn duration_div(duration: Duration, divisor: u64) -> Duration {
    if divisor == 0 {
        return Duration::ZERO;
    }
    let nanos = duration.as_nanos() / u128::from(divisor);
    Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

fn median_duration(mut values: Vec<Duration>) -> Option<Duration> {
    if values.is_empty() {
        return None;
    }
    values.sort_unstable();
    let middle = values.len() / 2;
    if values.len() % 2 == 1 {
        return Some(values[middle]);
    }

    let lower = values[middle - 1];
    let upper = values[middle];
    Some(lower.saturating_add(upper.saturating_sub(lower) / 2))
}

fn format_optional_duration(duration: Option<Duration>) -> String {
    duration
        .map(format_duration)
        .unwrap_or_else(|| "n/a".to_string())
}

fn format_payload_read_amplification(read_bytes: u64, used_bytes: u64) -> String {
    if used_bytes == 0 {
        return "—".to_string();
    }
    format!("{:.3}x", read_bytes as f64 / used_bytes as f64)
}

fn add_session_stats(
    total: &mut SegmentStoreQuerySessionStats,
    next: SegmentStoreQuerySessionStats,
) {
    total.index_routing_opens = total
        .index_routing_opens
        .saturating_add(next.index_routing_opens);
    total.segment_context_opens = total
        .segment_context_opens
        .saturating_add(next.segment_context_opens);
    total.symbols_bin_opens = total
        .symbols_bin_opens
        .saturating_add(next.symbols_bin_opens);
    total.indexes_puffin_opens = total
        .indexes_puffin_opens
        .saturating_add(next.indexes_puffin_opens);
    total.series_bin_opens = total.series_bin_opens.saturating_add(next.series_bin_opens);
    total.chunk_index_bin_opens = total
        .chunk_index_bin_opens
        .saturating_add(next.chunk_index_bin_opens);
    total.chunks_bin_opens = total.chunks_bin_opens.saturating_add(next.chunks_bin_opens);
}

fn add_session_profile(total: &mut SegmentStoreQueryProfile, next: SegmentStoreQueryProfile) {
    total.index_routing_open = total
        .index_routing_open
        .saturating_add(next.index_routing_open);
    total.segment_context_open = total
        .segment_context_open
        .saturating_add(next.segment_context_open);
    total.indexes_open = total.indexes_open.saturating_add(next.indexes_open);
    total.symbols_read = total.symbols_read.saturating_add(next.symbols_read);
    total.series_open = total.series_open.saturating_add(next.series_open);
    total.chunk_index_open = total.chunk_index_open.saturating_add(next.chunk_index_open);
    total.chunks_open = total.chunks_open.saturating_add(next.chunks_open);
    total.routing_index_read = total
        .routing_index_read
        .saturating_add(next.routing_index_read);
    total.exact_postings_read = total
        .exact_postings_read
        .saturating_add(next.exact_postings_read);
    total.metric_series_ranges_read = total
        .metric_series_ranges_read
        .saturating_add(next.metric_series_ranges_read);
    total.series_entry_read = total
        .series_entry_read
        .saturating_add(next.series_entry_read);
    total.chunk_index_range_read = total
        .chunk_index_range_read
        .saturating_add(next.chunk_index_range_read);
    total.chunk_read = total.chunk_read.saturating_add(next.chunk_read);
    total.index_routing_file_bytes = total
        .index_routing_file_bytes
        .saturating_add(next.index_routing_file_bytes);
    total.indexes_file_bytes = total
        .indexes_file_bytes
        .saturating_add(next.indexes_file_bytes);
    total.symbols_file_bytes = total
        .symbols_file_bytes
        .saturating_add(next.symbols_file_bytes);
    total.series_file_bytes = total
        .series_file_bytes
        .saturating_add(next.series_file_bytes);
    total.chunk_index_file_bytes = total
        .chunk_index_file_bytes
        .saturating_add(next.chunk_index_file_bytes);
    total.chunks_file_bytes = total
        .chunks_file_bytes
        .saturating_add(next.chunks_file_bytes);
    total.routing_index_bytes = total
        .routing_index_bytes
        .saturating_add(next.routing_index_bytes);
    total.exact_postings_bytes = total
        .exact_postings_bytes
        .saturating_add(next.exact_postings_bytes);
    total.metric_series_ranges_bytes = total
        .metric_series_ranges_bytes
        .saturating_add(next.metric_series_ranges_bytes);
    total.series_entries_read = total
        .series_entries_read
        .saturating_add(next.series_entries_read);
    total.series_entry_read_batches = total
        .series_entry_read_batches
        .saturating_add(next.series_entry_read_batches);
    total.series_entry_bytes = total
        .series_entry_bytes
        .saturating_add(next.series_entry_bytes);
    total.label_rows_integrity_checked = total
        .label_rows_integrity_checked
        .saturating_add(next.label_rows_integrity_checked);
    total.label_pairs_integrity_checked = total
        .label_pairs_integrity_checked
        .saturating_add(next.label_pairs_integrity_checked);
    total.label_rows_full_materialized = total
        .label_rows_full_materialized
        .saturating_add(next.label_rows_full_materialized);
    total.label_rows_selectively_materialized = total
        .label_rows_selectively_materialized
        .saturating_add(next.label_rows_selectively_materialized);
    total.label_pairs_materialized = total
        .label_pairs_materialized
        .saturating_add(next.label_pairs_materialized);
    total.label_pairs_omitted = total
        .label_pairs_omitted
        .saturating_add(next.label_pairs_omitted);
    total.label_content_bytes_materialized = total
        .label_content_bytes_materialized
        .saturating_add(next.label_content_bytes_materialized);
    total.chunk_index_range_bytes = total
        .chunk_index_range_bytes
        .saturating_add(next.chunk_index_range_bytes);
    total.chunk_payload_bytes = total
        .chunk_payload_bytes
        .saturating_add(next.chunk_payload_bytes);
    total.chunk_payload_physical_reads = total
        .chunk_payload_physical_reads
        .saturating_add(next.chunk_payload_physical_reads);
    total.chunk_payload_physical_bytes = total
        .chunk_payload_physical_bytes
        .saturating_add(next.chunk_payload_physical_bytes);
    total.index_read_stats = total.index_read_stats.saturating_add(next.index_read_stats);
    total.symbol_read_stats = total
        .symbol_read_stats
        .saturating_add(next.symbol_read_stats);
    total.symbol_resources = next.symbol_resources;
    total
        .chunk_payload_locality
        .add(next.chunk_payload_locality);
    total.chunk_read_scheduler.add(next.chunk_read_scheduler);
}

fn add_query_stats(total: &mut QueryStats, next: QueryStats) {
    total.segments_considered = total
        .segments_considered
        .saturating_add(next.segments_considered);
    total.segments_skipped_by_time = total
        .segments_skipped_by_time
        .saturating_add(next.segments_skipped_by_time);
    total.segments_skipped_by_missing_equality = total
        .segments_skipped_by_missing_equality
        .saturating_add(next.segments_skipped_by_missing_equality);
    total.segments_skipped_by_matcher_time_range = total
        .segments_skipped_by_matcher_time_range
        .saturating_add(next.segments_skipped_by_matcher_time_range);
    total.segments_queried = total.segments_queried.saturating_add(next.segments_queried);
    total.matched_series = total.matched_series.saturating_add(next.matched_series);
    total.projected_series = total.projected_series.saturating_add(next.projected_series);
    total.chunk_reads = total.chunk_reads.saturating_add(next.chunk_reads);
    total.bytes_read = total.bytes_read.saturating_add(next.bytes_read);
    total.index_postings_reads = total
        .index_postings_reads
        .saturating_add(next.index_postings_reads);
    total.index_postings_bytes_read = total
        .index_postings_bytes_read
        .saturating_add(next.index_postings_bytes_read);
    total.samples_decoded = total.samples_decoded.saturating_add(next.samples_decoded);
    total.typed_scalar_chunks_decoded = total
        .typed_scalar_chunks_decoded
        .saturating_add(next.typed_scalar_chunks_decoded);
    total.typed_full_chunks_decoded = total
        .typed_full_chunks_decoded
        .saturating_add(next.typed_full_chunks_decoded);
    total.regex_values_examined = total
        .regex_values_examined
        .saturating_add(next.regex_values_examined);
}
