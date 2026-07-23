use super::*;
use chronoxide_core::labels::{DefaultSymbolTable, KeySetDictEncodedLabelSetStore, KeyValueRef};

#[test]
fn latency_samples_dp_seen_counts_datapoints() {
    let mut samples = LatencySamples::new();

    samples.record(
        Duration::from_millis(10),
        Duration::from_millis(1),
        Duration::from_millis(9),
        0,
    );
    assert_eq!(samples.msg_seen, 1);
    assert_eq!(samples.dp_seen, 0);
    assert_eq!(samples.msg_sample_count(), 1);
    assert_eq!(samples.dp_sample_count(), 0);

    samples.record(
        Duration::from_millis(10),
        Duration::from_millis(1),
        Duration::from_millis(9),
        5,
    );
    assert_eq!(samples.msg_seen, 2);
    assert_eq!(samples.dp_seen, 5);
    // We record one DP latency sample per message (mean per datapoint), not per datapoint.
    assert_eq!(samples.dp_sample_count(), 1);
}

#[test]
fn per_key_value_stats_report_groups_by_metric_and_sorts_columns() {
    let mut store: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
        KeySetDictEncodedLabelSetStore::default();

    let labels1 = [
        KeyValueRef::from(("__name__", "m1")),
        KeyValueRef::from(("a", "1")),
        KeyValueRef::from(("b", "1")),
    ];
    let labels2 = [
        KeyValueRef::from(("__name__", "m1")),
        KeyValueRef::from(("a", "2")),
        KeyValueRef::from(("b", "1")),
    ];
    let labels3 = [
        KeyValueRef::from(("__name__", "m2")),
        KeyValueRef::from(("a", "1")),
        KeyValueRef::from(("c", "x")),
    ];

    store.intern(&labels1).unwrap();
    store.intern(&labels2).unwrap();
    store.intern(&labels3).unwrap();

    let report = per_key_value_stats_from_store(&store, None);
    assert_eq!(report.series_total, 3);
    assert_eq!(report.series_scanned, 3);

    let top_metrics = report.top_metrics.expect("top metrics present");
    let columns: Vec<&str> = top_metrics.columns.iter().map(|c| c.key.as_ref()).collect();
    assert_eq!(columns, vec!["a", "b", "c"]);

    let a_hash = hash_u64(b"a");
    let b_hash = hash_u64(b"b");
    let c_hash = hash_u64(b"c");

    // Metric ranking: m1 and m2 tie on score; metric name breaks the tie -> m1 first.
    assert_eq!(top_metrics.rows.len(), 3);

    assert_eq!(top_metrics.rows[0].metric_rank, 1);
    assert_eq!(top_metrics.rows[0].series_rank, 1);
    assert_eq!(top_metrics.rows[0].metric_name.as_ref(), "m1");
    assert!(
        top_metrics.rows[0]
            .key_hashes
            .binary_search(&a_hash)
            .is_ok()
    );
    assert!(
        top_metrics.rows[0]
            .key_hashes
            .binary_search(&b_hash)
            .is_ok()
    );
    assert!(
        top_metrics.rows[0]
            .key_hashes
            .binary_search(&c_hash)
            .is_err()
    );

    assert_eq!(top_metrics.rows[1].metric_rank, 1);
    assert_eq!(top_metrics.rows[1].series_rank, 2);
    assert_eq!(top_metrics.rows[1].metric_name.as_ref(), "m1");
    assert!(
        top_metrics.rows[1]
            .key_hashes
            .binary_search(&a_hash)
            .is_ok()
    );
    assert!(
        top_metrics.rows[1]
            .key_hashes
            .binary_search(&b_hash)
            .is_ok()
    );
    assert!(
        top_metrics.rows[1]
            .key_hashes
            .binary_search(&c_hash)
            .is_err()
    );

    assert_eq!(top_metrics.rows[2].metric_rank, 2);
    assert_eq!(top_metrics.rows[2].series_rank, 1);
    assert_eq!(top_metrics.rows[2].metric_name.as_ref(), "m2");
    assert!(
        top_metrics.rows[2]
            .key_hashes
            .binary_search(&a_hash)
            .is_ok()
    );
    assert!(
        top_metrics.rows[2]
            .key_hashes
            .binary_search(&b_hash)
            .is_err()
    );
    assert!(
        top_metrics.rows[2]
            .key_hashes
            .binary_search(&c_hash)
            .is_ok()
    );

    // Per-key stats should include "__name__" and should compute exact top values for key "a".
    let by_series_key_names: Vec<&str> = report
        .top_keys_by_series_coverage
        .iter()
        .map(|row| row.key.as_ref())
        .collect();
    assert!(by_series_key_names.contains(&"__name__"));
    assert!(by_series_key_names.contains(&"a"));

    let a_row = report
        .top_keys_by_series_coverage
        .iter()
        .find(|row| row.key.as_ref() == "a")
        .expect("row for key a");
    assert_eq!(a_row.series_with_key, 3);
    assert_eq!(a_row.distinct_values_display.as_ref(), "2");
    assert_eq!(a_row.top_values[0].sample.as_ref(), "1");
    assert_eq!(a_row.top_values[0].count, 2);
    assert_eq!(a_row.top_values[1].sample.as_ref(), "2");
    assert_eq!(a_row.top_values[1].count, 1);
}

#[test]
fn label_tag_stats_from_store_uses_canonical_labelsets() {
    let mut store: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
        KeySetDictEncodedLabelSetStore::default();

    let labels1 = [
        KeyValueRef::from(("__name__", "m1")),
        KeyValueRef::from(("a", "1")),
        KeyValueRef::from(("b", "cc")),
    ];
    let labels2 = [
        KeyValueRef::from(("__name__", "m1")),
        KeyValueRef::from(("a", "22")),
    ];

    store.intern(&labels1).unwrap();
    store.intern(&labels2).unwrap();

    let stats = label_tag_stats_from_store(&store, None);
    let labels_dist = stats.labels.summarize().expect("labels dist");

    assert_eq!(labels_dist.count, 2);
    assert_eq!(labels_dist.min, 2);
    assert_eq!(labels_dist.max, 3);

    // Total key bytes: "__name__"(8) + "a"(1) + "b"(1) + "__name__"(8) + "a"(1) = 19
    let key_total_bytes = 19u64;
    // Total value bytes: "m1"(2) + "1"(1) + "cc"(2) + "m1"(2) + "22"(2) = 9
    let value_total_bytes = 9u64;

    let key_total_dist = stats
        .key_total_bytes_per_series
        .summarize()
        .expect("key bytes dist");
    let value_total_dist = stats
        .value_total_bytes_per_series
        .summarize()
        .expect("value bytes dist");

    assert_eq!(key_total_dist.count, 2);
    assert_eq!(key_total_dist.min + key_total_dist.max, key_total_bytes);
    assert_eq!(value_total_dist.count, 2);
    assert_eq!(
        value_total_dist.min + value_total_dist.max,
        value_total_bytes
    );
}
