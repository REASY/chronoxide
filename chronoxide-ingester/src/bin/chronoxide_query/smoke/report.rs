use super::super::benchmark::render_profile_table;
use super::super::common::{format_duration, format_end_ms, markdown_escape_inline};
use super::*;

pub(in super::super) fn render_markdown(
    config: &QuerySmokeConfig,
    storage_layout: StorageLayoutArg,
    report: &SegmentStoreSmokeReport,
    verification: Option<&QueryReadbackVerification>,
    diagnostics: Option<&QuerySmokeDiagnostics>,
) -> String {
    let mut markdown = String::new();

    markdown.push_str("# Chronoxide Query Smoke Report\n\n");
    markdown.push_str(&format!(
        "- Generated At: {}\n",
        Utc::now().format("%Y-%m-%d %H:%M:%S UTC")
    ));
    markdown.push_str(&format!(
        "- Segments Directory: `{}`\n",
        config.segments_dir.display()
    ));
    markdown.push_str(&format!(
        "- Time Range: {}..{}\n",
        config.start_ms,
        format_end_ms(config.end_ms)
    ));
    markdown.push_str(&format!(
        "- Sample Limit Per Kind: {}\n\n",
        config.sample_limit_per_kind
    ));
    markdown.push_str(&format!("- Storage Layout: {}\n\n", storage_layout.name()));
    markdown.push_str(&format!(
        "- Requested Segment Footer Validation: {}\n\n",
        config.validate_segment_footers
    ));
    markdown.push_str(&format!(
        "- Effective Segment Footer Validation: {}\n\n",
        config.validate_segment_footers || storage_layout.forces_footer_validation()
    ));

    markdown.push_str("## Segment Totals\n\n");
    markdown.push_str("| Metric | Value |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!("| Segments | {} |\n", report.totals.segments));
    markdown.push_str(&format!(
        "| Segment Datapoints | {} |\n",
        report.totals.datapoints
    ));
    markdown.push_str(&format!("| Segment Series | {} |\n", report.totals.series));
    markdown.push_str(&format!("| Chunks | {} |\n", report.totals.chunks));
    markdown.push_str(&format!(
        "| Chunk Bytes | {} |\n\n",
        report.totals.chunk_bytes
    ));

    markdown.push_str("## Chunk Kinds\n\n");
    markdown.push_str("| Kind | Chunks | Chunk Bytes |\n");
    markdown.push_str("| --- | ---: | ---: |\n");
    for kind in [
        ChunkKind::Float,
        ChunkKind::Int64,
        ChunkKind::Histogram,
        ChunkKind::ExponentialHistogram,
        ChunkKind::Summary,
    ] {
        let stats = kind_stats(report, kind);
        markdown.push_str(&format!(
            "| {} | {} | {} |\n",
            kind_name(kind),
            stats.chunks,
            stats.chunk_bytes
        ));
    }
    markdown.push('\n');

    markdown.push_str("## Sampled Native Series\n\n");
    markdown.push_str(
        "| Kind | Metric | Segment | Series Ref | Samples | Time Range Ms | Chunk Bytes | Labels |\n",
    );
    markdown.push_str("| --- | --- | --- | ---: | ---: | --- | ---: | --- |\n");
    for sample in &report.sample_series {
        markdown.push_str(&format!(
            "| {} | `{}` | `{}` | {} | {} | {}..{} | {} | `{}` |\n",
            kind_name(sample.kind),
            markdown_escape_inline(sample_metric_name(&sample.labels)),
            markdown_escape_inline(&sample.segment_id),
            sample.series_ref,
            sample.samples,
            sample.min_time_ms,
            sample.max_time_ms,
            sample.chunk_bytes,
            markdown_escape_inline(&format_labels(&sample.labels))
        ));
    }
    markdown.push('\n');

    markdown.push_str("## PromQL Readbacks\n\n");
    markdown.push_str("| Kind | Query | result_series | result_samples | matched_series | projected_series | chunk_reads | bytes_read | samples_decoded | typed_scalar_chunks_decoded | typed_full_chunks_decoded |\n");
    markdown
        .push_str("| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |\n");
    for query in &report.queries {
        markdown.push_str(&format!(
            "| {} | `{}` | {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
            kind_name(query.kind),
            markdown_escape_inline(&query.query),
            query.result_series,
            query.result_samples,
            query.matched_series,
            query.projected_series,
            query.chunk_reads,
            query.bytes_read,
            query.samples_decoded,
            query.typed_scalar_chunks_decoded,
            query.typed_full_chunks_decoded
        ));
    }

    if let Some(verification) = verification {
        markdown.push_str("\n## Readback Verification\n\n");
        markdown.push_str("| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Checked Queries | {} |\n",
            verification.checked_queries
        ));
        markdown.push_str(&format!(
            "| Mismatches | {} |\n",
            verification.mismatches.len()
        ));

        if !verification.mismatches.is_empty() {
            markdown.push_str("\n| Query | Expected Missing Samples | Actual Samples |\n");
            markdown.push_str("| --- | --- | --- |\n");
            for mismatch in &verification.mismatches {
                markdown.push_str(&format!(
                    "| `{}` | `{}` | `{}` |\n",
                    markdown_escape_inline(&mismatch.query),
                    markdown_escape_inline(&format_samples(&mismatch.missing_expected_samples)),
                    markdown_escape_inline(&format_samples(&mismatch.actual_samples))
                ));
            }
        }
    }

    if let Some(diagnostics) = diagnostics {
        append_query_diagnostics(&mut markdown, diagnostics);
    }

    markdown
}

fn append_query_diagnostics(markdown: &mut String, diagnostics: &QuerySmokeDiagnostics) {
    markdown.push_str("\n## Query Diagnostics\n\n");
    markdown.push_str("| Phase | Duration |\n");
    markdown.push_str("| --- | ---: |\n");
    markdown.push_str(&format!(
        "| Store Open | {} |\n",
        format_duration(diagnostics.store_open)
    ));
    markdown.push_str(&format!(
        "| Smoke Verify | {} |\n",
        format_duration(diagnostics.smoke_verify)
    ));

    if let Some(readback) = &diagnostics.readback {
        markdown.push_str(&format!(
            "| Collect Expected Readbacks | {} |\n",
            format_duration(readback.collect_expected_readbacks)
        ));
        markdown.push_str(&format!(
            "| Readback Store Open | {} |\n",
            format_duration(readback.store_open)
        ));
        markdown.push_str(&format!(
            "| Query Session Open | {} |\n",
            format_duration(readback.query_session_open)
        ));
        markdown.push_str(&format!(
            "| Readback PromQL Queries | {} |\n",
            format_duration(readback.promql_queries)
        ));

        markdown.push_str("\n| Metric | Value |\n");
        markdown.push_str("| --- | ---: |\n");
        markdown.push_str(&format!(
            "| Expected Readback Queries | {} |\n",
            readback.expected_queries
        ));
        markdown.push_str(&format!(
            "| Executed Readback Queries | {} |\n",
            readback.executed_queries
        ));
        markdown.push_str(&format!(
            "| Skipped Readback Queries | {} |\n",
            readback.skipped_queries
        ));
        markdown.push_str(&format!(
            "| Isolation Check Skips | {} |\n",
            readback.isolation_check_skips
        ));
        markdown.push_str(&format!(
            "| Multi-Step Range Readbacks Expected | {} |\n",
            readback.multi_step_range_expected_queries
        ));
        markdown.push_str(&format!(
            "| Multi-Step Range Readbacks Executed | {} |\n",
            readback.multi_step_range_executed_queries
        ));
        markdown.push_str(&format!(
            "| Multi-Step Range Readbacks Skipped | {} |\n",
            readback.multi_step_range_skipped_queries
        ));
        markdown.push_str(&format!(
            "| Index Routing Opens | {} |\n",
            readback.session_stats.index_routing_opens
        ));
        markdown.push_str(&format!(
            "| Segment Context Opens | {} |\n",
            readback.session_stats.segment_context_opens
        ));
        markdown.push_str(&format!(
            "| Symbols Opens | {} |\n",
            readback.session_stats.symbols_bin_opens
        ));
        markdown.push_str(&format!(
            "| Indexes Opens | {} |\n",
            readback.session_stats.indexes_puffin_opens
        ));
        markdown.push_str(&format!(
            "| Series Opens | {} |\n",
            readback.session_stats.series_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunk Index Opens | {} |\n",
            readback.session_stats.chunk_index_bin_opens
        ));
        markdown.push_str(&format!(
            "| Chunks Opens | {} |\n",
            readback.session_stats.chunks_bin_opens
        ));
        if !readback.skip_reasons.is_empty() {
            markdown.push_str("\n| Readback Skip Reason | Queries |\n");
            markdown.push_str("| --- | ---: |\n");
            for (reason, queries) in &readback.skip_reasons {
                markdown.push_str(&format!(
                    "| {} | {} |\n",
                    markdown_escape_inline(reason),
                    queries
                ));
            }
        }
        render_profile_table(
            markdown,
            "Readback Query Session Read Profile",
            readback.session_profile,
        );
    }
}

fn kind_stats(report: &SegmentStoreSmokeReport, kind: ChunkKind) -> SegmentStoreSmokeKindStats {
    match kind {
        ChunkKind::Float => report.totals.by_kind.float,
        ChunkKind::Int64 => report.totals.by_kind.int64,
        ChunkKind::Histogram => report.totals.by_kind.histogram,
        ChunkKind::ExponentialHistogram => report.totals.by_kind.exponential_histogram,
        ChunkKind::Summary => report.totals.by_kind.summary,
    }
}

fn kind_name(kind: ChunkKind) -> &'static str {
    match kind {
        ChunkKind::Float => "Float",
        ChunkKind::Int64 => "Int64",
        ChunkKind::Histogram => "Histogram",
        ChunkKind::ExponentialHistogram => "ExponentialHistogram",
        ChunkKind::Summary => "Summary",
    }
}

fn sample_metric_name(labels: &[(String, String)]) -> &str {
    labels
        .iter()
        .find_map(|(name, value)| (name == METRIC_NAME_LABEL).then_some(value.as_str()))
        .unwrap_or("<missing>")
}

fn format_labels(labels: &[(String, String)]) -> String {
    labels
        .iter()
        .map(|(name, value)| format!("{name}={value}"))
        .collect::<Vec<_>>()
        .join(",")
}

fn format_samples(samples: &[(u64, f64)]) -> String {
    samples
        .iter()
        .map(|(ts, value)| format!("({ts}, {value:?})"))
        .collect::<Vec<_>>()
        .join(", ")
}
