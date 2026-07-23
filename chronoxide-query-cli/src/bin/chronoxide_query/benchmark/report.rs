use super::super::common::{
    format_duration, format_end_ms, format_query_limit, markdown_escape_inline,
};
use super::raw::range_execution_mode_name;
use super::*;

mod document;

pub(super) fn render_benchmark_markdown(
    config: &QueryBenchmarkConfig,
    report: &QueryBenchmarkReport,
) -> String {
    document::render_benchmark_markdown(config, report)
}

pub(super) fn render_query_label_storage(markdown: &mut String, results: &[QueryBenchmarkResult]) {
    markdown.push_str("\n## Experimental Query Label Storage\n\n");
    markdown.push_str("Legacy atom counters cover the `shared-atoms` comparator. Compact counters cover `compact-ids`; source-symbol translations and atom lookups must each equal their respective hit-plus-miss totals.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Label Sets | Atom Lookups | Atom Hits | Atom Misses | Unique Content Bytes | Compact Label Sets | Compact Pairs | Source Symbol Translations | Translation Hits | Translation Misses | Compact Atom Lookups | Compact Atom Hits | Compact Atom Misses | Compact Unique Strings | Compact Unique Content Bytes |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in results {
        let stats = result.label_storage_delta;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            stats.label_sets,
            stats.atom_lookups,
            stats.atom_hits,
            stats.atom_misses,
            stats.unique_content_bytes,
            stats.compact_label_sets,
            stats.compact_pairs,
            stats.compact_source_symbol_translations,
            stats.compact_source_symbol_translation_hits,
            stats.compact_source_symbol_translation_misses,
            stats.compact_atom_lookups,
            stats.compact_atom_hits,
            stats.compact_atom_misses,
            stats.compact_unique_strings,
            stats.compact_unique_content_bytes,
        ));
    }

    markdown.push_str("\nArena accounting is a portable admission model of requested live allocation bytes, sampled at the end of each run; it is not allocator `usable_size`. Allocator metadata, implementation-specific capacity growth, and size-class slack are excluded and must be assessed with process RSS. `Atom Bytes` includes the arena `Arc`/root, the fixed atom directory, atom chunks, and aligned `Arc<str>` allocations. `Label Block Bytes` includes one shared `Arc`/label object and its boxed compact-pair payload; clones add no charge. `Hash Directory Bytes` is a deliberately conservative capacity envelope consisting of a fixed first-table reserve plus per-admission charges. `Translation Bytes` includes a conservative translation-list capacity envelope plus exact page directories and admitted pages. Compact results retain no owned-string compatibility slice; explicit `to_vec()` copies are caller-owned, so Compatibility Materializations must remain zero. `Retained Bytes` must equal both `Current Bytes` and the sum of those four categories.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Budget Bytes | Current Bytes | Peak Bytes | Atom Bytes | Label Block Bytes | Hash Directory Bytes | Translation Bytes | Retained Bytes | Admission Refusals | Compatibility Materializations |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in results {
        let stats = result.label_storage_delta;
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            stats.compact_arena_budget_bytes,
            stats.compact_arena_current_bytes,
            stats.compact_arena_peak_bytes,
            stats.compact_atom_bytes,
            stats.compact_pair_bytes,
            stats.compact_hash_directory_bytes,
            stats.compact_translation_bytes,
            stats.compact_retained_bytes,
            stats.compact_arena_admission_refusals,
            stats.compact_compatibility_materializations,
        ));
    }
}

fn render_query_stage_runs(markdown: &mut String, report: &QueryBenchmarkReport) {
    markdown.push_str("\n## Exclusive Query Stage Attribution\n\n");
    markdown.push_str("The stage columns are mutually exclusive leaf timers; older read-profile timers below are inclusive diagnostics and must not be added here. `Payload Decode / Projection / Result Processing (Combined)` is intentionally broad: the current timer includes decode, projection, range-cache/result processing, and associated label work. `Unclassified` is the timed query wall minus the exclusive total.\n\n");
    match report.query_instrumentation {
        QueryInstrumentationArg::Off => markdown.push_str("Instrumentation is `off`: stage fields are zero and the full query wall is reported as unclassified. This is the mode for latency comparisons.\n\n"),
        QueryInstrumentationArg::Detailed => markdown.push_str("Instrumentation is `detailed`: every run is validated so the exclusive total does not exceed its timed query wall. These observer-instrumented wall times are diagnostic, not latency-comparison results.\n\n"),
    }
    markdown.push_str("For non-cross execution, the payload-read leaf includes coalescing-plan construction, governed file acquisition, scheduling, and reads. Cross-segment execution measures the scheduler/read portion after planning. Treat this as a combined read-pipeline stage; use the scheduler and byte counters for finer interpretation.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Timed Query Wall | Post-Query Fingerprints | Canonical Row Decode | Candidate Selection | Metadata Visit Overhead | Symbol Lookup | Symbol Resolution | Canonical Identity | Matcher Evaluation | Label Construction | Locator Planning | Payload Read Pipeline (Combined) | Payload Decode / Projection / Result Processing (Combined) | Source Merge | PromQL Grouping / Evaluation | Result Construction | Exclusive Total | Unclassified |\n");
    markdown.push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for result in &report.results {
        let stages = result.session_profile_delta.stages;
        let exclusive_total = stages.total_exclusive();
        let unclassified = result.duration.saturating_sub(exclusive_total);
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            format_duration(result.duration),
            format_duration(result.post_query_fingerprint),
            format_duration(stages.canonical_row_decode),
            format_duration(stages.candidate_selection),
            format_duration(stages.metadata_visit_overhead),
            format_duration(stages.symbol_lookup),
            format_duration(stages.symbol_resolution),
            format_duration(stages.canonical_identity),
            format_duration(stages.matcher_evaluation),
            format_duration(stages.label_construction),
            format_duration(stages.locator_planning),
            format_duration(stages.payload_io),
            format_duration(stages.payload_decode),
            format_duration(stages.source_merge),
            format_duration(stages.promql_grouping_evaluation),
            format_duration(stages.result_construction),
            format_duration(exclusive_total),
            format_duration(unclassified),
        ));
    }
}

pub(super) fn render_range_scalar_cache_runs(
    markdown: &mut String,
    results: &[QueryBenchmarkResult],
) {
    if !results
        .iter()
        .any(|result| result.range_scalar_cache.is_some())
    {
        return;
    }

    markdown.push_str("\n## Range Scalar Cache Runs\n\n");
    markdown.push_str("`process_governor_current_leased_bytes` is a point-in-time process gauge sampled after the range query finalizes (normally zero for that completed lease). `process_governor_lifetime_peak_leased_bytes` is the process-lifetime high-water mark, not a per-run peak or delta.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | configured_budget_bytes | governor_lease_bytes | governor_refused | allocation_refused | layout_overflow | entry_arena_charge_bytes | sample_arena_charge_bytes | hits | misses | admitted_entries | streaming_budget_bypasses | unsupported_bypasses | logical_hit_bytes | logical_miss_or_bypass_bytes | peak_retained_charge_bytes | retained_charge_after_finalize | process_governor_limit_bytes | process_governor_current_leased_bytes | process_governor_lifetime_peak_leased_bytes |\n");
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

fn render_range_execution_runs(markdown: &mut String, results: &[QueryBenchmarkResult]) {
    if !results
        .iter()
        .any(|result| result.range_execution.is_some())
    {
        return;
    }

    markdown.push_str("\n## Range Execution Runs\n\n");
    markdown.push_str("One-pass retained bytes are a post-decode estimate, not an admission budget. `preallocation_governed=false` means source vectors were allocated before the comparator could account for them; `retained_bytes_after_finalize` must return to zero. Repeated and one-pass `QueryStats` use different logical accounting scopes and are classified rather than required to match.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Requested | Effective | Fallback | Terminal | Evaluations | Union Start | Union End | Source Series | Source Samples | Estimated Retained Peak Bytes | Retained After Finalize | Preallocation Governed | Cache Bypassed |\n");
    markdown.push_str("| --- | --- | ---: | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n");
    for result in results {
        let Some(summary) = result.range_execution else {
            continue;
        };
        markdown.push_str(&format!(
            "| `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            markdown_escape_inline(&result.query),
            run_kind_name(result.run_kind),
            result.run_index,
            range_execution_mode_name(summary.requested_mode),
            range_execution_mode_name(summary.effective_mode),
            summary
                .fallback_reason
                .map_or("-", |reason| reason.as_str()),
            summary
                .terminal_reason
                .map_or("-", |reason| reason.as_str()),
            summary.evaluation_count,
            summary
                .union_start_ms
                .map_or_else(|| "-".to_owned(), |value| value.to_string()),
            summary
                .union_end_ms
                .map_or_else(|| "-".to_owned(), |value| value.to_string()),
            summary.source_series,
            summary.source_samples,
            summary.estimated_retained_bytes_peak,
            summary.retained_bytes_after_finalize,
            summary.preallocation_governed,
            summary.cache_bypassed,
        ));
    }
}

fn render_metadata_runtime_runs(markdown: &mut String, results: &[QueryBenchmarkResult]) {
    markdown.push_str("\n## Query Result Metadata Runtime Counter Deltas\n\n");
    markdown.push_str("Counter deltas are saturating differences between snapshots taken immediately before and after the timed query; snapshot collection is outside the measured duration. Runtime components are copied sequentially, so a boundary is not an atomic cross-component snapshot. `successful_loads` means completed metadata loads; it is not an admission count.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Component | Counter | Delta |\n");
    markdown.push_str("| --- | --- | ---: | --- | --- | ---: |\n");
    for result in results {
        let counters = &result.metadata_runtime.counters_delta;
        let cache = &counters.cache;
        for (counter, value) in [
            ("hits", cache.hits),
            ("misses", cache.misses),
            ("evictions", cache.evictions),
            ("single_flight_waits", cache.single_flight_waits),
            ("successful_loads", cache.successful_loads),
            ("failed_loads", cache.failed_loads),
            ("corruption_detections", cache.corruption_detections),
            ("corruption_hits", cache.corruption_hits),
            ("resident_admissions", cache.resident_admissions),
            (
                "resident_admission_refusals",
                cache.resident_admission_refusals,
            ),
            (
                "resident_admission_bypasses",
                cache.resident_admission_bypasses,
            ),
        ] {
            render_metadata_runtime_counter_row(markdown, result, "cache", counter, value);
        }
        for class in &cache.class_admissions {
            for (counter, value) in [
                ("resident_admissions", class.resident_admissions),
                (
                    "resident_admission_refusals",
                    class.resident_admission_refusals,
                ),
                (
                    "resident_admission_bypasses",
                    class.resident_admission_bypasses,
                ),
            ] {
                render_metadata_runtime_counter_row(
                    markdown,
                    result,
                    &format!("cache.class.{}", class.class),
                    counter,
                    value,
                );
            }
        }
        let governor = &counters.governor;
        for (counter, value) in [
            ("retained_refusals", governor.retained_refusals),
            ("in_flight_refusals", governor.in_flight_refusals),
        ] {
            render_metadata_runtime_counter_row(markdown, result, "governor", counter, value);
        }
        let files = &counters.file_manager;
        for (counter, value) in [
            ("preflight_calls", files.preflight_calls),
            ("successful_preflights", files.successful_preflights),
            ("preflight_failures", files.preflight_failures),
            ("acquire_calls", files.acquire_calls),
            ("successful_acquires", files.successful_acquires),
            ("requested_handles", files.requested_handles),
            ("deduplicated_handles", files.deduplicated_handles),
            ("descriptor_opens", files.descriptor_opens),
            ("descriptor_closes", files.descriptor_closes),
            ("descriptor_reuses", files.descriptor_reuses),
            ("lease_clones", files.lease_clones),
            ("idle_evictions", files.idle_evictions),
            ("capacity_waits", files.capacity_waits),
            ("capacity_refusals", files.capacity_refusals),
            ("open_failures", files.open_failures),
            ("structural_replacements", files.structural_replacements),
            ("acquisition_rollbacks", files.acquisition_rollbacks),
        ] {
            render_metadata_runtime_counter_row(markdown, result, "file_manager", counter, value);
        }
    }

    markdown.push_str("\n## Query Result Metadata Read Deltas\n\n");
    markdown.push_str("The total, by-file, and by-class rows are independent projections of the same issued metadata reads and must not be added together. Cache hits issue no reads.\n\n");
    markdown
        .push_str("| Query | Run Kind | Run Index | Projection | Dimension | Calls | Bytes |\n");
    markdown.push_str("| --- | --- | ---: | --- | --- | ---: | ---: |\n");
    for result in results {
        let reads = &result.metadata_runtime.counters_delta.reads;
        render_metadata_runtime_read_row(markdown, result, "total", "issued", reads.issued);
        render_metadata_runtime_read_row(
            markdown,
            result,
            "unclassified",
            "unclassified",
            reads.unclassified,
        );
        for entry in &reads.by_file {
            render_metadata_runtime_read_row(
                markdown,
                result,
                "file",
                entry.file,
                QueryBenchmarkMetadataReadCount {
                    calls: entry.calls,
                    bytes: entry.bytes,
                },
            );
        }
        for entry in &reads.by_class {
            render_metadata_runtime_read_row(
                markdown,
                result,
                "class",
                entry.class,
                QueryBenchmarkMetadataReadCount {
                    calls: entry.calls,
                    bytes: entry.bytes,
                },
            );
        }
    }

    markdown.push_str("\n## Query Result Metadata Runtime Start Gauges\n\n");
    markdown.push_str("These are point-in-time values from the snapshot immediately before each timed query. They capture the initial retained-cache and file-descriptor state, are not deltas, and must not be summed across runs.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Component | Gauge | Value |\n");
    markdown.push_str("| --- | --- | ---: | --- | --- | ---: |\n");
    for result in results {
        render_metadata_runtime_gauges(markdown, result, &result.metadata_runtime.start_gauges);
    }

    markdown.push_str("\n## Query Result Metadata Runtime End Gauges\n\n");
    markdown.push_str("These are point-in-time values from the snapshot immediately after each timed query. They are not deltas and must not be summed across runs.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Component | Gauge | Value |\n");
    markdown.push_str("| --- | --- | ---: | --- | --- | ---: |\n");
    for result in results {
        render_metadata_runtime_gauges(markdown, result, &result.metadata_runtime.end_gauges);
    }

    markdown.push_str("\n## Query Result Metadata Runtime Lifetime Peaks After Run\n\n");
    markdown.push_str("These are store-lifetime high-water marks observed after each query. They are neither per-run deltas nor per-run peaks.\n\n");
    markdown.push_str("| Query | Run Kind | Run Index | Component | Peak | Value |\n");
    markdown.push_str("| --- | --- | ---: | --- | --- | ---: |\n");
    for result in results {
        let peaks = &result.metadata_runtime.lifetime_peaks_after_run;
        for class in &peaks.cache_class_charges {
            render_metadata_runtime_gauge_row(
                markdown,
                result,
                &format!("cache.class.{}", class.class),
                "peak_in_flight_bytes",
                class.peak_in_flight_bytes,
            );
            render_metadata_runtime_gauge_row(
                markdown,
                result,
                &format!("cache.class.{}", class.class),
                "peak_retained_bytes",
                class.peak_retained_bytes,
            );
        }
        let governor = &peaks.governor;
        render_metadata_runtime_gauge_row(
            markdown,
            result,
            "governor",
            "peak_retained_bytes",
            governor.peak_retained_bytes,
        );
        render_metadata_runtime_gauge_row(
            markdown,
            result,
            "governor",
            "peak_in_flight_bytes",
            governor.peak_in_flight_bytes,
        );
        for usage in &governor.usage_charges {
            render_metadata_runtime_gauge_row(
                markdown,
                result,
                &format!("governor.usage.{}", usage.usage),
                "peak_in_flight_bytes",
                usage.peak_in_flight_bytes,
            );
            render_metadata_runtime_gauge_row(
                markdown,
                result,
                &format!("governor.usage.{}", usage.usage),
                "peak_retained_bytes",
                usage.peak_retained_bytes,
            );
        }
        let files = &peaks.file_manager;
        for (peak, value) in [
            ("peak_open_files", files.peak_open_files),
            ("peak_occupied_open_slots", files.peak_occupied_open_slots),
            ("peak_active_open_files", files.peak_active_open_files),
            ("peak_cached_open_files", files.peak_cached_open_files),
            ("peak_active_leases", files.peak_active_leases),
            ("peak_preflighting_files", files.peak_preflighting_files),
        ] {
            render_metadata_runtime_gauge_row(
                markdown,
                result,
                "file_manager",
                peak,
                u64::from(value),
            );
        }
    }
}

fn render_metadata_runtime_counter_row(
    markdown: &mut String,
    result: &QueryBenchmarkResult,
    component: &str,
    counter: &str,
    value: u64,
) {
    markdown.push_str(&format!(
        "| `{}` | {} | {} | `{component}` | `{counter}` | {value} |\n",
        markdown_escape_inline(&result.query),
        run_kind_name(result.run_kind),
        result.run_index,
    ));
}

fn render_metadata_runtime_gauges(
    markdown: &mut String,
    result: &QueryBenchmarkResult,
    gauges: &QueryBenchmarkMetadataRuntimeGauges,
) {
    let cache = &gauges.cache;
    for (gauge, value) in [
        ("resident_entries", cache.resident_entries),
        ("live_allocations", cache.live_allocations),
        ("active_loads", cache.active_loads),
        ("registered_artifacts", cache.registered_artifacts),
        ("ledger_reserved_bytes", cache.ledger_reserved_bytes),
        ("ledger_in_flight_bytes", cache.ledger_in_flight_bytes),
        ("ledger_retained_bytes", cache.ledger_retained_bytes),
        ("sticky_artifacts", cache.sticky_artifacts),
        ("sticky_charged_bytes", cache.sticky_charged_bytes),
    ] {
        render_metadata_runtime_gauge_row(markdown, result, "cache", gauge, value);
    }
    for class in &cache.class_charges {
        render_metadata_runtime_gauge_row(
            markdown,
            result,
            &format!("cache.class.{}", class.class),
            "in_flight_bytes",
            class.in_flight_bytes,
        );
        render_metadata_runtime_gauge_row(
            markdown,
            result,
            &format!("cache.class.{}", class.class),
            "retained_bytes",
            class.retained_bytes,
        );
    }
    let governor = &gauges.governor;
    for (gauge, value) in [
        ("retained_max_bytes", governor.retained_max_bytes),
        ("in_flight_max_bytes", governor.in_flight_max_bytes),
        ("retained_bytes", governor.retained_bytes),
        ("in_flight_bytes", governor.in_flight_bytes),
    ] {
        render_metadata_runtime_gauge_row(markdown, result, "governor", gauge, value);
    }
    for usage in &governor.usage_charges {
        render_metadata_runtime_gauge_row(
            markdown,
            result,
            &format!("governor.usage.{}", usage.usage),
            "in_flight_bytes",
            usage.in_flight_bytes,
        );
        render_metadata_runtime_gauge_row(
            markdown,
            result,
            &format!("governor.usage.{}", usage.usage),
            "retained_bytes",
            usage.retained_bytes,
        );
    }
    let files = &gauges.file_manager;
    for (gauge, value) in [
        ("max_open_files", files.max_open_files),
        ("max_cached_open_files", files.max_cached_open_files),
        ("open_files", files.open_files),
        ("occupied_open_slots", files.occupied_open_slots),
        ("active_open_files", files.active_open_files),
        ("cached_open_files", files.cached_open_files),
        ("opening_files", files.opening_files),
        ("pending_open_files", files.pending_open_files),
        ("preflighting_files", files.preflighting_files),
        ("closing_files", files.closing_files),
        ("active_leases", files.active_leases),
    ] {
        render_metadata_runtime_gauge_row(
            markdown,
            result,
            "file_manager",
            gauge,
            u64::from(value),
        );
    }
}

fn render_metadata_runtime_gauge_row(
    markdown: &mut String,
    result: &QueryBenchmarkResult,
    component: &str,
    gauge: &str,
    value: u64,
) {
    markdown.push_str(&format!(
        "| `{}` | {} | {} | `{component}` | `{gauge}` | {value} |\n",
        markdown_escape_inline(&result.query),
        run_kind_name(result.run_kind),
        result.run_index,
    ));
}

fn render_metadata_runtime_read_row(
    markdown: &mut String,
    result: &QueryBenchmarkResult,
    projection: &str,
    dimension: &str,
    count: QueryBenchmarkMetadataReadCount,
) {
    markdown.push_str(&format!(
        "| `{}` | {} | {} | `{projection}` | `{dimension}` | {} | {} |\n",
        markdown_escape_inline(&result.query),
        run_kind_name(result.run_kind),
        result.run_index,
        count.calls,
        count.bytes,
    ));
}

pub(super) fn render_profile_table(
    markdown: &mut String,
    title: &str,
    profile: SegmentStoreQueryProfile,
) {
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
        "| Total Physical Bytes Executed | {} |\n",
        scheduler.total_physical_bytes_executed
    ));
    markdown.push_str(&format!(
        "| Peak In-Flight Bytes | {} |\n\n",
        scheduler.peak_in_flight_bytes
    ));
}

pub(super) fn render_index_positional_read_table(
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

pub(super) fn render_query_result_index_positional_reads(
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

pub(super) fn add_query_data_prefetch_stats(
    total: &mut QueryDataPrefetchStats,
    next: QueryDataPrefetchStats,
) {
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

pub(super) fn raw_run_kind_name(kind: QueryBenchmarkRunKind) -> &'static str {
    match kind {
        QueryBenchmarkRunKind::Cold => "cold",
        QueryBenchmarkRunKind::Warm => "warm",
    }
}

pub(super) fn query_benchmark_mode_name(mode: QueryBenchmarkMode) -> &'static str {
    match mode {
        QueryBenchmarkMode::Instant => "instant",
        QueryBenchmarkMode::Range { .. } => "query_range",
    }
}

pub(super) fn scheduled_range_evaluations(start_ms: u64, end_ms: u64, step_ms: u64) -> u128 {
    u128::from(end_ms - start_ms) / u128::from(step_ms) + 1
}

fn duration_div(duration: Duration, divisor: u64) -> Duration {
    if divisor == 0 {
        return Duration::ZERO;
    }
    let nanos = duration.as_nanos() / u128::from(divisor);
    Duration::from_nanos(nanos.min(u128::from(u64::MAX)) as u64)
}

pub(super) fn median_duration(mut values: Vec<Duration>) -> Option<Duration> {
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

pub(super) fn format_payload_read_amplification(read_bytes: u64, used_bytes: u64) -> String {
    if used_bytes == 0 {
        return "—".to_string();
    }
    format!("{:.3}x", read_bytes as f64 / used_bytes as f64)
}

pub(super) fn add_session_stats(
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

pub(super) fn add_session_profile(
    total: &mut SegmentStoreQueryProfile,
    next: SegmentStoreQueryProfile,
) {
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
    total.stages.canonical_row_decode = total
        .stages
        .canonical_row_decode
        .saturating_add(next.stages.canonical_row_decode);
    total.stages.candidate_selection = total
        .stages
        .candidate_selection
        .saturating_add(next.stages.candidate_selection);
    total.stages.metadata_visit_overhead = total
        .stages
        .metadata_visit_overhead
        .saturating_add(next.stages.metadata_visit_overhead);
    total.stages.symbol_lookup = total
        .stages
        .symbol_lookup
        .saturating_add(next.stages.symbol_lookup);
    total.stages.symbol_resolution = total
        .stages
        .symbol_resolution
        .saturating_add(next.stages.symbol_resolution);
    total.stages.canonical_identity = total
        .stages
        .canonical_identity
        .saturating_add(next.stages.canonical_identity);
    total.stages.matcher_evaluation = total
        .stages
        .matcher_evaluation
        .saturating_add(next.stages.matcher_evaluation);
    total.stages.label_construction = total
        .stages
        .label_construction
        .saturating_add(next.stages.label_construction);
    total.stages.locator_planning = total
        .stages
        .locator_planning
        .saturating_add(next.stages.locator_planning);
    total.stages.payload_io = total
        .stages
        .payload_io
        .saturating_add(next.stages.payload_io);
    total.stages.payload_decode = total
        .stages
        .payload_decode
        .saturating_add(next.stages.payload_decode);
    total.stages.source_merge = total
        .stages
        .source_merge
        .saturating_add(next.stages.source_merge);
    total.stages.promql_grouping_evaluation = total
        .stages
        .promql_grouping_evaluation
        .saturating_add(next.stages.promql_grouping_evaluation);
    total.stages.result_construction = total
        .stages
        .result_construction
        .saturating_add(next.stages.result_construction);
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
