use super::*;

pub(super) fn data_type_counts_markdown(
    metric_types: &OtlpDataTypeCounts,
    observed_datapoint_types: &OtlpDataTypeCounts,
    accepted_datapoint_types: &OtlpDataTypeCounts,
) -> String {
    let mut md = String::new();
    md.push_str("## OTLP Data Type Counts\n\n");
    md.push_str("| Type | Metric Records | Observed Datapoints | Accepted Datapoints |\n");
    md.push_str("|---|---:|---:|---:|\n");
    for (label, metric_records, observed_datapoints, accepted_datapoints) in [
        (
            "Gauge",
            metric_types.gauge,
            observed_datapoint_types.gauge,
            accepted_datapoint_types.gauge,
        ),
        (
            "Sum",
            metric_types.sum,
            observed_datapoint_types.sum,
            accepted_datapoint_types.sum,
        ),
        (
            "Histogram",
            metric_types.histogram,
            observed_datapoint_types.histogram,
            accepted_datapoint_types.histogram,
        ),
        (
            "Exponential Histogram",
            metric_types.exponential_histogram,
            observed_datapoint_types.exponential_histogram,
            accepted_datapoint_types.exponential_histogram,
        ),
        (
            "Summary",
            metric_types.summary,
            observed_datapoint_types.summary,
            accepted_datapoint_types.summary,
        ),
    ] {
        md.push_str(&format!(
            "| {} | {} | {} | {} |\n",
            label, metric_records, observed_datapoints, accepted_datapoints
        ));
    }
    md.push('\n');
    md
}

pub(super) fn datapoint_policy_counts_markdown(
    totals: &DatapointPolicyCounts,
    window: &DatapointPolicyCounts,
) -> String {
    let mut md = String::new();
    md.push_str("## Datapoint Policy Counts\n\n");
    md.push_str("| Outcome | Total | Window |\n");
    md.push_str("|---|---:|---:|\n");
    for (label, total, window) in [
        (
            "Observed",
            totals.accepted.saturating_add(totals.rejected()),
            window.accepted.saturating_add(window.rejected()),
        ),
        ("Time-Policy Accepted", totals.accepted, window.accepted),
        (
            "Dropped Too Old",
            totals.dropped_too_old,
            window.dropped_too_old,
        ),
        (
            "Dropped Too Future",
            totals.dropped_too_future,
            window.dropped_too_future,
        ),
        (
            "Missing Timestamp",
            totals.missing_timestamp,
            window.missing_timestamp,
        ),
        ("Rejected Total", totals.rejected(), window.rejected()),
    ] {
        md.push_str(&format!("| {} | {} | {} |\n", label, total, window));
    }
    md.push('\n');
    md
}

pub(super) fn datapoint_storage_counts_markdown(
    totals: &DatapointStorageCounts,
    window: &DatapointStorageCounts,
    policy_totals: &DatapointPolicyCounts,
    policy_window: &DatapointPolicyCounts,
) -> String {
    let mut md = String::new();
    md.push_str("## Datapoint Storage Counts\n\n");
    md.push_str("Recorded samples are datapoints successfully accepted by the head storage path. Missing number values are time-accepted Gauge/Sum datapoints without an OTLP numeric value.\n\n");
    md.push_str("| Outcome | Total | Window |\n|---|---:|---:|\n");
    for (label, total, window) in [
        (
            "Time-Policy Accepted",
            policy_totals.accepted,
            policy_window.accepted,
        ),
        (
            "Recorded Samples",
            totals.recorded_samples,
            window.recorded_samples,
        ),
        (
            "Missing Number Value",
            totals.missing_number_values,
            window.missing_number_values,
        ),
        (
            "Accepted Not Recorded",
            policy_totals
                .accepted
                .saturating_sub(totals.recorded_samples),
            policy_window
                .accepted
                .saturating_sub(window.recorded_samples),
        ),
    ] {
        md.push_str(&format!("| {} | {} | {} |\n", label, total, window));
    }
    md.push('\n');
    md
}

pub(super) fn event_time_skew_markdown(skew: &EventTimeSkewSnapshot) -> String {
    if skew.all.is_none() {
        return String::new();
    }

    let mut md = String::new();
    md.push_str("## Event Time Skew\n\n");
    md.push_str(
        "Signed milliseconds between OTLP datapoint event time and capture time (`event_ms - captured_at_ms`). Negative values mean event time was before capture.\n\n",
    );
    md.push_str("| Metric | Count | Mean | StdDev | Min | Max | P50 | P75 | P95 | P99 |\n");
    md.push_str("|---|---|---|---|---|---|---|---|---|---|\n");
    if let Some(dist) = skew.all {
        md.push_str(&dist.to_markdown_row("All Timestamped"));
    }
    if let Some(dist) = skew.accepted {
        md.push_str(&dist.to_markdown_row("Accepted"));
    }
    if let Some(dist) = skew.dropped_too_old {
        md.push_str(&dist.to_markdown_row("Dropped Too Old"));
    }
    if let Some(dist) = skew.dropped_too_future {
        md.push_str(&dist.to_markdown_row("Dropped Too Future"));
    }
    md.push('\n');
    md
}
