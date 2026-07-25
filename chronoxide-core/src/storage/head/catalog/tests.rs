use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::labels::{
    KeyValueRef, LabelSetStore, METRIC_NAME_LABEL, VersionedFlatInternedLabelSetStore,
};
use crate::otlp_labelset::{CanonicalLabelSet, OtlpLabelSetInterner, intern_labelset};
use crate::promql::{
    canonicalize_labelset, normalize_label_name, normalize_metric_name, series_id,
};
use crate::storage::segment::{LabelMatcher, QueryLimits, SegmentSelector};
use opentelemetry_proto::tonic::common::v1::any_value::Value as OtlpValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue, KeyValue};

use super::super::{
    CounterResetHint, ExponentialHistogramBuckets, ExponentialHistogramValue, FloatEncoding,
    FrozenHeadReadView, HeadBuffer, HeadConfig, HeadReadView, HistogramValue, IntEncoding,
    MetadataAccumulator, OtlpAggregationTemporality, QueryBudget, SampleValue, TypedSampleMetadata,
};
use super::*;

fn head_config() -> HeadConfig {
    HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_out_of_order_time_window(Duration::from_secs(30))
    .with_compact_numeric_series(false)
}

fn intern(labels: &mut VersionedFlatInternedLabelSetStore, values: &[(&str, &str)]) -> SeriesRef {
    let values = values
        .iter()
        .copied()
        .map(KeyValueRef::from)
        .collect::<Vec<_>>();
    labels.intern(&values).unwrap()
}

fn sample_store(samples: &[(SeriesRef, u64, f64)]) -> LiveSampleStore {
    sample_value_store(
        samples
            .iter()
            .map(|(series, timestamp_ms, value)| {
                (*series, *timestamp_ms, SampleValue::Float(*value))
            })
            .collect(),
    )
}

fn sample_value_store(samples: Vec<(SeriesRef, u64, SampleValue)>) -> LiveSampleStore {
    let mut head = HeadBuffer::new(head_config()).unwrap();
    for (series, timestamp_ms, value) in samples {
        assert!(
            head.record_sample_with_outcome(series, timestamp_ms, value)
                .unwrap()
                .recorded
        );
    }
    let fragments = head.try_freeze_for_publication().unwrap();
    FrozenHeadReadView::from_owned(fragments)
        .sample_store()
        .clone()
}

fn initial_catalog(
    labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
    samples: &LiveSampleStore,
    generation: u64,
) -> LiveSeriesCatalog {
    let mut builder = LiveSeriesCatalogBuilder::new(labels, generation).unwrap();
    builder.reconcile_sample_store(samples).unwrap();
    let catalog = builder.finish().unwrap();
    catalog.validate_internal().unwrap();
    catalog
}

fn next_catalog(
    previous: &LiveSeriesCatalog,
    labels: Arc<VersionedFlatInternedLabelSetSnapshot>,
    samples: &LiveSampleStore,
    generation: u64,
) -> LiveSeriesCatalog {
    let mut builder = LiveSeriesCatalogBuilder::from_catalog(previous, labels, generation).unwrap();
    builder.reconcile_sample_store(samples).unwrap();
    let catalog = builder.finish().unwrap();
    catalog.validate_internal().unwrap();
    catalog
}

fn live_view(samples: LiveSampleStore, catalog: LiveSeriesCatalog) -> Arc<HeadReadView> {
    let generation = catalog.generation();
    Arc::new(
        HeadReadView::new_live(
            Arc::new(FrozenHeadReadView::from_sample_store(samples)),
            Arc::new(catalog),
            generation,
        )
        .unwrap(),
    )
}

fn query(
    view: &HeadReadView,
    selector: &SegmentSelector,
    start_ms: u64,
    end_ms: u64,
    limits: QueryLimits,
) -> (
    Vec<crate::storage::segment::SegmentQueryResult>,
    crate::storage::segment::QueryStats,
) {
    let mut budget = QueryBudget::new(limits);
    let results = view
        .query_selector_with_budget(selector, start_ms, end_ms, &mut budget)
        .unwrap();
    (results, budget.stats())
}

fn result_hosts(results: &[crate::storage::segment::SegmentQueryResult]) -> Vec<String> {
    let mut hosts = results
        .iter()
        .filter_map(|result| {
            result
                .labels
                .pairs()
                .find_map(|(name, value)| (name == "host").then(|| value.to_string()))
        })
        .collect::<Vec<_>>();
    hosts.sort();
    hosts
}

#[test]
fn exact_negative_regex_and_missing_label_semantics_match_the_reference_path() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let east = intern(
        &mut labels,
        &[
            (METRIC_NAME_LABEL, "cpu_usage"),
            ("host", "a"),
            ("zone", "east"),
        ],
    );
    let missing = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "b")],
    );
    let west = intern(
        &mut labels,
        &[
            (METRIC_NAME_LABEL, "cpu_usage"),
            ("host", "c"),
            ("zone", "west"),
        ],
    );
    let samples = sample_store(&[
        (east, 1_000, 1.0),
        (missing, 1_100, 2.0),
        (west, 1_200, 3.0),
    ]);
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let reference = HeadReadView::new(
        Arc::new(FrozenHeadReadView::from_sample_store(samples.clone())),
        Arc::clone(&snapshot),
    )
    .unwrap();
    let live = live_view(
        samples.clone(),
        initial_catalog(Arc::clone(&snapshot), &samples, 1),
    );

    let selectors = [
        SegmentSelector::new(vec![LabelMatcher::eq("host", "a")]),
        SegmentSelector::new(vec![LabelMatcher::not_eq("host", "a")]),
        SegmentSelector::new(vec![LabelMatcher::regex("host", "a|c")]),
        SegmentSelector::new(vec![LabelMatcher::not_regex("host", "a|c")]),
        SegmentSelector::new(vec![LabelMatcher::eq("zone", "")]),
        SegmentSelector::new(vec![LabelMatcher::not_eq("zone", "")]),
        SegmentSelector::new(vec![LabelMatcher::regex("zone", ".*")]),
        SegmentSelector::new(vec![LabelMatcher::not_regex("zone", ".+")]),
    ];
    for selector in selectors {
        let (expected, expected_stats) =
            query(&reference, &selector, 0, 2_000, QueryLimits::unlimited());
        let (actual, actual_stats) = query(&live, &selector, 0, 2_000, QueryLimits::unlimited());
        assert_eq!(actual, expected, "selector={selector:?}");
        assert_eq!(
            actual_stats.regex_values_examined, expected_stats.regex_values_examined,
            "selector={selector:?}"
        );
    }

    let (east_result, _) = query(
        &live,
        &SegmentSelector::new(vec![LabelMatcher::eq("host", "a")]),
        0,
        2_000,
        QueryLimits::unlimited(),
    );
    assert_eq!(
        east_result[0].series_id,
        series_id(&canonicalize_labelset(
            "cpu_usage",
            &[("host", "a"), ("zone", "east")],
        ))
    );
}

#[test]
fn catalog_only_and_out_of_range_values_consume_no_regex_or_metadata_budget() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    for index in 0..2_000 {
        let _ = intern(
            &mut labels,
            &[
                (METRIC_NAME_LABEL, "ghost_metric"),
                ("host", &format!("ghost-{index:04}")),
            ],
        );
    }
    let in_range = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "in-range")],
    );
    let old = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "old")],
    );
    let samples = sample_store(&[(in_range, 20_000, 1.0), (old, 1_000, 2.0)]);
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let view = live_view(samples.clone(), initial_catalog(snapshot, &samples, 1));
    let (results, stats) = query(
        &view,
        &SegmentSelector::new(vec![LabelMatcher::regex("host", ".*")]),
        19_000,
        21_000,
        QueryLimits {
            max_regex_values_examined: Some(1),
            ..QueryLimits::unlimited()
        },
    );
    assert_eq!(result_hosts(&results), vec!["in-range"]);
    assert_eq!(stats.regex_values_examined, 1);

    let mut metadata = MetadataAccumulator::default();
    view.collect_metadata(19_000, 21_000, &mut metadata)
        .unwrap();
    assert_eq!(metadata.metric_names(), vec!["cpu_usage"]);
    assert_eq!(metadata.label_values("host"), vec!["in-range"]);
}

#[test]
fn old_revision_stays_pinned_while_higher_visible_refs_and_sparse_rows_publish() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let first = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "requests_total"), ("host", "first")],
    );
    let first_snapshot = Arc::new(labels.snapshot().unwrap());
    let first_samples = sample_store(&[(first, 1_000, 1.0)]);
    let first_catalog = initial_catalog(Arc::clone(&first_snapshot), &first_samples, 1);
    let first_view = live_view(first_samples, first_catalog.clone());

    let _catalog_only = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "ghost_metric"), ("host", "ghost")],
    );
    let higher = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "requests_total"), ("host", "higher")],
    );
    let second_snapshot = Arc::new(labels.snapshot().unwrap());
    let second_samples = sample_store(&[(first, 1_000, 1.0), (higher, 1_100, 2.0)]);
    let second_catalog = next_catalog(&first_catalog, second_snapshot, &second_samples, 2);
    let second_view = live_view(second_samples, second_catalog);

    assert_eq!(first_view.catalog_revision(), 1);
    assert_eq!(second_view.catalog_revision(), 3);
    let selector = SegmentSelector::metric("requests_total");
    assert_eq!(
        result_hosts(&query(&first_view, &selector, 0, 2_000, QueryLimits::unlimited(),).0),
        vec!["first"]
    );
    assert_eq!(
        result_hosts(&query(&second_view, &selector, 0, 2_000, QueryLimits::unlimited(),).0),
        vec!["first", "higher"]
    );
}

#[test]
fn regex_results_and_budget_are_independent_of_publication_schedule() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let refs = ["alpha", "beta", "gamma"].map(|host| {
        intern(
            &mut labels,
            &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", host)],
        )
    });
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let one = sample_store(&[(refs[0], 1_000, 1.0)]);
    let two = sample_store(&[(refs[0], 1_000, 1.0), (refs[1], 1_100, 2.0)]);
    let all = sample_store(&[
        (refs[0], 1_000, 1.0),
        (refs[1], 1_100, 2.0),
        (refs[2], 1_200, 3.0),
    ]);

    let direct = live_view(all.clone(), initial_catalog(Arc::clone(&snapshot), &all, 1));
    let first = initial_catalog(Arc::clone(&snapshot), &one, 1);
    let second = next_catalog(&first, Arc::clone(&snapshot), &two, 2);
    let incremental = live_view(
        all.clone(),
        next_catalog(&second, Arc::clone(&snapshot), &all, 3),
    );

    let selector = SegmentSelector::new(vec![LabelMatcher::regex("host", "a.*|b.*|g.*")]);
    let direct_result = query(&direct, &selector, 0, 2_000, QueryLimits::unlimited());
    let incremental_result = query(&incremental, &selector, 0, 2_000, QueryLimits::unlimited());
    assert_eq!(incremental_result, direct_result);
    assert_eq!(direct_result.1.regex_values_examined, 3);
}

#[test]
fn handoff_retirement_is_reclaimable_and_can_reactivate_the_global_ref() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let first = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "first")],
    );
    let second = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "second")],
    );
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let both = sample_store(&[(first, 1_000, 1.0), (second, 1_100, 2.0)]);
    let old = initial_catalog(Arc::clone(&snapshot), &both, 1);
    let old_root = Arc::downgrade(old.active.root.as_ref().unwrap());

    let second_only = sample_store(&[(second, 1_100, 2.0)]);
    let retired = next_catalog(&old, Arc::clone(&snapshot), &second_only, 2);
    assert_eq!(retired.active_series_refs().unwrap(), vec![second]);
    assert!(old_root.upgrade().is_some());
    drop(old);
    assert!(old_root.upgrade().is_none());

    let reactivated = next_catalog(&retired, snapshot, &both, 3);
    assert_eq!(
        reactivated.active_series_refs().unwrap(),
        vec![first, second]
    );
}

#[test]
fn empty_successor_drops_active_indexes_without_invalidating_a_pinned_predecessor() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let first = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "first")],
    );
    let second = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "second")],
    );
    let predecessor_snapshot = Arc::new(labels.snapshot().unwrap());
    let predecessor_samples = sample_store(&[(first, 1_000, 1.0), (second, 1_100, 2.0)]);
    let predecessor = initial_catalog(predecessor_snapshot, &predecessor_samples, 1);
    let predecessor_root = Arc::downgrade(
        predecessor
            .active
            .root
            .as_ref()
            .expect("the predecessor must have an active-series root"),
    );
    let pinned_predecessor = live_view(predecessor_samples, predecessor.clone());

    let catalog_only = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "catalog_only"), ("host", "not-active")],
    );
    let successor_snapshot = Arc::new(labels.snapshot().unwrap());
    let successor =
        LiveSeriesCatalogBuilder::empty_successor(&predecessor, Arc::clone(&successor_snapshot), 2)
            .unwrap()
            .finish()
            .unwrap();
    successor.validate_internal().unwrap();

    assert_eq!(successor.generation(), 2);
    assert_eq!(
        successor.revision(),
        u64::from(catalog_only.get()).saturating_add(1)
    );
    assert_eq!(successor.labels().revision(), successor_snapshot.revision());
    assert_eq!(successor.active_series_len(), 0);
    assert!(successor.active_series_refs().unwrap().is_empty());
    assert!(successor.active.root.is_none());
    assert!(successor.postings.root.is_none());
    assert!(successor.names.root.is_none());
    assert!(successor.values.root.is_none());

    let empty_samples = LiveSampleStore::default();
    successor.validate_sample_store(&empty_samples).unwrap();
    let empty_view = live_view(empty_samples, successor);
    assert!(empty_view.is_empty());
    let (empty_results, empty_stats) = query(
        &empty_view,
        &SegmentSelector::metric("cpu_usage"),
        0,
        2_000,
        QueryLimits::unlimited(),
    );
    assert!(empty_results.is_empty());
    assert_eq!(empty_stats.matched_series, 0);

    drop(predecessor);
    assert!(
        predecessor_root.upgrade().is_some(),
        "the pinned predecessor must retain its active catalog root"
    );
    let (predecessor_results, predecessor_stats) = query(
        &pinned_predecessor,
        &SegmentSelector::metric("cpu_usage"),
        0,
        2_000,
        QueryLimits::unlimited(),
    );
    assert_eq!(result_hosts(&predecessor_results), vec!["first", "second"]);
    assert_eq!(predecessor_stats.matched_series, 2);

    drop(pinned_predecessor);
    assert!(
        predecessor_root.upgrade().is_none(),
        "the empty successor must not retain the predecessor's active indexes"
    );
}

#[test]
fn empty_successor_rejects_generation_revision_and_lineage_mismatches() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let first = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "first")],
    );
    let earlier_snapshot = Arc::new(labels.snapshot().unwrap());
    let second = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "second")],
    );
    let current_snapshot = Arc::new(labels.snapshot().unwrap());
    let samples = sample_store(&[(first, 1_000, 1.0), (second, 1_100, 2.0)]);
    let predecessor = initial_catalog(Arc::clone(&current_snapshot), &samples, 7);

    let generation_error =
        LiveSeriesCatalogBuilder::empty_successor(&predecessor, Arc::clone(&current_snapshot), 9)
            .unwrap_err();
    assert!(
        generation_error
            .to_string()
            .contains("candidate generation 9 does not follow pinned generation 7")
    );

    let revision_error =
        LiveSeriesCatalogBuilder::empty_successor(&predecessor, earlier_snapshot, 8).unwrap_err();
    assert!(
        revision_error
            .to_string()
            .contains("catalog revision regressed from 2 to 1")
    );

    let mut unrelated = VersionedFlatInternedLabelSetStore::default();
    intern(
        &mut unrelated,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "first")],
    );
    intern(
        &mut unrelated,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "second")],
    );
    let unrelated_snapshot = Arc::new(unrelated.snapshot().unwrap());
    let lineage_error =
        LiveSeriesCatalogBuilder::empty_successor(&predecessor, unrelated_snapshot, 8).unwrap_err();
    assert!(
        lineage_error
            .to_string()
            .contains("different label-store lineage")
    );
}

#[test]
fn unrelated_revisions_generation_mismatch_and_sample_mismatch_fail_closed() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let series = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "a")],
    );
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let samples = sample_store(&[(series, 1_000, 1.0)]);
    let catalog = initial_catalog(Arc::clone(&snapshot), &samples, 4);

    let generation_error = HeadReadView::new_live(
        Arc::new(FrozenHeadReadView::from_sample_store(samples.clone())),
        Arc::new(catalog.clone()),
        5,
    )
    .unwrap_err();
    assert!(generation_error.to_string().contains("does not match"));

    let empty_samples = LiveSampleStore::default();
    let sample_error = HeadReadView::new_live(
        Arc::new(FrozenHeadReadView::from_sample_store(empty_samples)),
        Arc::new(catalog.clone()),
        4,
    )
    .unwrap_err();
    assert!(sample_error.to_string().contains("does not exactly match"));

    let later_ref = SeriesRef::new(series.get().checked_add(1).unwrap());
    let later_samples = sample_store(&[(later_ref, 1_100, 2.0)]);
    let revision_error = HeadReadView::new_live(
        Arc::new(FrozenHeadReadView::from_sample_store(later_samples)),
        Arc::new(catalog.clone()),
        4,
    )
    .unwrap_err();
    assert!(
        revision_error
            .to_string()
            .contains("samples require catalog revision 2")
    );

    let mut unrelated = VersionedFlatInternedLabelSetStore::default();
    let unrelated_snapshot = Arc::new(unrelated.snapshot().unwrap());
    let lineage_error =
        LiveSeriesCatalogBuilder::from_catalog(&catalog, unrelated_snapshot, 5).unwrap_err();
    assert!(lineage_error.to_string().contains("different"));
    let generation_error =
        LiveSeriesCatalogBuilder::from_catalog(&catalog, snapshot, 6).unwrap_err();
    assert!(generation_error.to_string().contains("does not follow"));
}

#[test]
fn raw_identity_names_are_validated_through_the_canonical_promql_projection() {
    let mut raw_labels = VersionedFlatInternedLabelSetStore::default();
    let raw_series = intern(
        &mut raw_labels,
        &[(METRIC_NAME_LABEL, "raw.metric"), ("host", "a")],
    );
    let raw_snapshot = Arc::new(raw_labels.snapshot().unwrap());
    let mut raw_identity = Vec::new();
    raw_snapshot
        .try_visit_labelset(raw_series, |key, value| {
            raw_identity.push((key.to_string(), value.to_string()));
        })
        .unwrap();
    assert!(
        raw_identity
            .iter()
            .any(|(key, value)| key == METRIC_NAME_LABEL && value == "raw.metric")
    );

    let samples = sample_store(&[(raw_series, 1_000, 1.0)]);
    let catalog = initial_catalog(Arc::clone(&raw_snapshot), &samples, 1);
    let projected = catalog.materialize_labels(raw_series).unwrap();
    let projected_metric_name = normalize_metric_name("raw.metric");
    assert!(
        projected
            .iter()
            .any(|(key, value)| key == METRIC_NAME_LABEL && value == &projected_metric_name),
        "the catalog validates and exposes the derived canonical row without changing raw identity"
    );
}

#[test]
fn pinned_catalog_queries_are_send_sync_and_deterministic_under_concurrency() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<LiveSeriesCatalog>();
    assert_send_sync::<HeadReadView>();

    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let refs = ["a", "b", "c", "d"].map(|host| {
        intern(
            &mut labels,
            &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", host)],
        )
    });
    let samples = sample_store(&[
        (refs[0], 1_000, 1.0),
        (refs[1], 1_100, 2.0),
        (refs[2], 1_200, 3.0),
        (refs[3], 1_300, 4.0),
    ]);
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let view = live_view(samples.clone(), initial_catalog(snapshot, &samples, 1));
    let workers = 8;
    let barrier = Arc::new(Barrier::new(workers));
    let mut threads = Vec::new();
    for _ in 0..workers {
        let view = Arc::clone(&view);
        let barrier = Arc::clone(&barrier);
        threads.push(thread::spawn(move || {
            barrier.wait();
            for _ in 0..100 {
                let selector = SegmentSelector::new(vec![LabelMatcher::regex("host", "[a-d]")]);
                let (results, stats) = query(&view, &selector, 0, 2_000, QueryLimits::unlimited());
                assert_eq!(result_hosts(&results), vec!["a", "b", "c", "d"]);
                assert_eq!(stats.matched_series, 4);
                assert_eq!(stats.regex_values_examined, 4);

                let mut metadata = MetadataAccumulator::default();
                view.collect_metadata(0, 2_000, &mut metadata).unwrap();
                assert_eq!(metadata.label_values("host"), vec!["a", "b", "c", "d"]);
            }
        }));
    }
    for worker in threads {
        worker.join().unwrap();
    }
}

#[test]
fn memory_estimate_distinguishes_shared_label_bytes_from_catalog_index_bytes() {
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let series = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "cpu_usage"), ("host", "a")],
    );
    let samples = sample_store(&[(series, 1_000, 1.0)]);
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let catalog = initial_catalog(snapshot, &samples, 1);
    let estimate = catalog.memory_estimate();
    assert!(estimate.shared_label_snapshot_bytes > 0);
    assert!(estimate.catalog_index_bytes_if_unshared > 0);
    assert_eq!(
        estimate.total_bytes_if_unshared,
        estimate
            .shared_label_snapshot_bytes
            .saturating_add(estimate.catalog_index_bytes_if_unshared)
    );
    assert_eq!(catalog.estimated_bytes(), estimate.total_bytes_if_unshared);
}

#[test]
fn empty_and_native_typed_live_views_use_the_same_catalog_cut() {
    let mut empty_labels = VersionedFlatInternedLabelSetStore::default();
    let empty_snapshot = Arc::new(empty_labels.snapshot().unwrap());
    let empty_samples = LiveSampleStore::default();
    let empty = live_view(
        empty_samples.clone(),
        initial_catalog(empty_snapshot, &empty_samples, 1),
    );
    assert!(empty.is_empty());
    assert!(
        query(
            &empty,
            &SegmentSelector::metric("anything"),
            0,
            1_000,
            QueryLimits::unlimited(),
        )
        .0
        .is_empty()
    );

    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let histogram_series = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "request_duration"), ("host", "hist")],
    );
    let exponential_series = intern(
        &mut labels,
        &[(METRIC_NAME_LABEL, "request_size"), ("host", "exp")],
    );
    let metadata = TypedSampleMetadata {
        start_time_ms: Some(100),
        flags: 7,
        temporality: OtlpAggregationTemporality::Cumulative,
        reset_hint: CounterResetHint::NotCounterReset,
    };
    let samples = sample_value_store(vec![
        (
            histogram_series,
            1_000,
            SampleValue::Histogram(HistogramValue {
                count: 3,
                sum: Some(6.0),
                min: Some(1.0),
                max: Some(3.0),
                metadata,
                explicit_bounds: vec![1.0],
                bucket_counts: vec![1, 2],
            }),
        ),
        (
            exponential_series,
            1_100,
            SampleValue::ExponentialHistogram(ExponentialHistogramValue {
                count: 4,
                sum: Some(8.0),
                min: Some(-1.0),
                max: Some(4.0),
                scale: 1,
                zero_threshold: 0.0,
                zero_count: 1,
                metadata,
                positive: ExponentialHistogramBuckets {
                    offset: 0,
                    counts: vec![2],
                },
                negative: ExponentialHistogramBuckets {
                    offset: -1,
                    counts: vec![1],
                },
            }),
        ),
    ]);
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let view = live_view(samples.clone(), initial_catalog(snapshot, &samples, 1));

    let mut budget = QueryBudget::unlimited();
    let histograms = view
        .query_native_histogram_with_budget(
            &SegmentSelector::metric("request_duration"),
            0,
            2_000,
            &mut budget,
        )
        .unwrap();
    assert_eq!(histograms.len(), 1);
    assert_eq!(histograms[0].samples.len(), 1);

    let mut budget = QueryBudget::unlimited();
    let exponentials = view
        .query_native_exponential_histogram_with_budget(
            &SegmentSelector::metric("request_size"),
            0,
            2_000,
            &mut budget,
        )
        .unwrap();
    assert_eq!(exponentials.len(), 1);
    assert_eq!(exponentials[0].samples.len(), 1);
}

#[test]
fn production_otlp_interning_publishes_canonical_promql_ids_before_catalog_build() {
    struct Adapter<'a>(&'a mut VersionedFlatInternedLabelSetStore);

    impl OtlpLabelSetInterner for Adapter<'_> {
        type Error = VersionedFlatLabelStoreError;

        fn on_skipped_non_scalar(&mut self) {}

        fn on_intern_error(&mut self, error: Self::Error) {
            panic!("unexpected intern error: {error}");
        }

        fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
            self.0.intern_prepared_otlp(labels)
        }
    }

    let attribute = KeyValue {
        key: "service.name".to_string(),
        value: Some(AnyValue {
            value: Some(OtlpValue::StringValue("api".to_string())),
        }),
        key_strindex: 0,
    };
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let mut scratch_values = Vec::new();
    let mut scratch_labels = Vec::new();
    let series = intern_labelset(
        &mut Adapter(&mut labels),
        &[attribute],
        "cpu.usage",
        &[],
        &mut scratch_values,
        &mut scratch_labels,
    )
    .unwrap();
    let samples = sample_store(&[(series, 1_000, 1.0)]);
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let catalog = initial_catalog(snapshot, &samples, 1);
    let materialized = catalog.materialize_labels(series).unwrap();
    assert_eq!(
        materialized,
        vec![
            (
                METRIC_NAME_LABEL.to_string(),
                normalize_metric_name("cpu.usage")
            ),
            (normalize_label_name("service.name"), "api".to_string()),
        ]
    );

    let view = live_view(samples, catalog);
    let (results, _) = query(
        &view,
        &SegmentSelector::metric("cpu.usage"),
        0,
        2_000,
        QueryLimits::unlimited(),
    );
    assert_eq!(results.len(), 1);
}

#[test]
fn canonical_identity_verification_detects_distinct_raw_rows_that_project_equal() {
    let raw_name = "a.label";
    let projected_name = normalize_label_name(raw_name);
    let mut labels = VersionedFlatInternedLabelSetStore::default();
    let raw = intern(
        &mut labels,
        &[
            (METRIC_NAME_LABEL, "collision_metric"),
            (raw_name, "same-value"),
        ],
    );
    let projected = intern(
        &mut labels,
        &[
            (METRIC_NAME_LABEL, "collision_metric"),
            (&projected_name, "same-value"),
        ],
    );
    let distinct = intern(
        &mut labels,
        &[
            (METRIC_NAME_LABEL, "collision_metric"),
            (&projected_name, "different-value"),
        ],
    );
    assert_ne!(raw, projected);

    let samples = sample_store(&[
        (raw, 1_000, 1.0),
        (projected, 1_100, 2.0),
        (distinct, 1_200, 3.0),
    ]);
    let snapshot = Arc::new(labels.snapshot().unwrap());
    let catalog = initial_catalog(snapshot, &samples, 1);

    assert_eq!(
        catalog.series_id(raw).unwrap(),
        catalog.series_id(projected).unwrap()
    );
    assert!(
        catalog
            .canonical_series_identity_eq(raw, projected)
            .unwrap()
    );
    assert!(!catalog.canonical_series_identity_eq(raw, distinct).unwrap());
}
