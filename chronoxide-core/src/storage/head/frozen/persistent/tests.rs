use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;

use crate::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeyValueRef, LabelSetStore, METRIC_NAME_LABEL,
};
use crate::storage::head::{FloatEncoding, HeadBuffer, HeadConfig, IntEncoding, SampleValue};
use crate::storage::segment::{LabelMatcher, QueryBudget, QueryLimits, SegmentSelector};

use super::*;

fn test_config() -> HeadConfig {
    HeadConfig::with_block_size(
        Duration::from_secs(10),
        2,
        FloatEncoding::Gorilla,
        IntEncoding::DeltaZigZag,
    )
    .with_compact_numeric_series(false)
}

fn frozen_float(
    series: SeriesRef,
    timestamp_ms: u64,
    value: f64,
    publication_sequence: u64,
) -> Arc<FrozenHeadFragment> {
    let mut head = HeadBuffer::new(test_config()).unwrap();
    head.record_sample(series, timestamp_ms, SampleValue::Float(value))
        .unwrap();
    let mut fragment = head.drain().unwrap().try_freeze().unwrap();
    fragment.set_publication_sequence(publication_sequence);
    Arc::new(fragment)
}

fn partition(topic: &str) -> LivePartitionKey {
    LivePartitionKey::new(topic, 7)
}

fn identity(topic: &str, fragment: &FrozenHeadFragment, sequence: u64) -> FrozenFragmentIdentity {
    let key = FrozenFragmentKey::new(
        partition(topic),
        fragment.start_ms(),
        fragment.end_ms(),
        fragment.lane(),
    )
    .unwrap();
    let order = RecordedSampleOrder::new(MessageSequence::new(sequence), 0);
    FrozenFragmentIdentity::new(key, order, order).unwrap()
}

fn labels(
    store: &mut FlatInternedLabelSetStore<DefaultSymbolTable>,
    values: &[(&str, &str)],
) -> SeriesRef {
    let values: Vec<_> = values.iter().copied().map(KeyValueRef::from).collect();
    store.intern(&values).unwrap()
}

#[test]
fn binary_levels_bound_visible_roots_and_preserve_every_leaf_in_order() {
    let series = SeriesRef::new(5);
    let payload = frozen_float(series, 1_000, 1.0, 1);
    let mut builder = LiveSampleStoreBuilder::new();

    for sequence in 1..=129 {
        builder
            .insert_fragment(
                identity("metrics", &payload, sequence),
                Arc::clone(&payload),
            )
            .unwrap();
    }
    let store = builder.finish();
    let key = LiveSampleKey::new(
        FrozenFragmentKey::new(
            partition("metrics"),
            payload.start_ms(),
            payload.end_ms(),
            payload.lane(),
        )
        .unwrap(),
        series,
        SampleKind::Float,
    );

    assert_eq!(store.fragment_count(), 129);
    assert_eq!(store.key_count(), 1);
    assert_eq!(store.visible_root_count(&key), Some(2));
    let stats = store.stats().unwrap();
    assert_eq!(stats.visible_roots, 2);
    assert_eq!(stats.leaves, 129);
    assert_eq!(stats.samples, 129);
    assert_eq!(stats.maximum_depth, 8);

    let runs = store.ordered_runs(0, 9_999).unwrap();
    assert_eq!(runs.len(), 129);
    assert!(runs.windows(2).all(|pair| pair[0].last < pair[1].first));
}

#[test]
fn topic_qualifies_equal_numeric_partitions_and_handoff_omits_only_exact_key() {
    let series = SeriesRef::new(3);
    let left = frozen_float(series, 1_000, 1.0, 1);
    let right = frozen_float(series, 1_000, 2.0, 2);
    let left_identity = identity("alpha", &left, 1);
    let right_identity = identity("beta", &right, 2);
    let left_key = LiveSampleKey::new(
        left_identity.fragment_key().clone(),
        series,
        SampleKind::Float,
    );
    let right_key = LiveSampleKey::new(
        right_identity.fragment_key().clone(),
        series,
        SampleKind::Float,
    );

    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(left_identity.clone(), Arc::clone(&left))
        .unwrap();
    builder
        .insert_fragment(right_identity, Arc::clone(&right))
        .unwrap();
    let both = builder.finish();
    assert_eq!(both.key_count(), 2);
    assert!(both.contains_key(&left_key));
    assert!(both.contains_key(&right_key));

    let mut retirement = LiveSampleStoreBuilder::from_store(&both);
    assert_eq!(
        retirement
            .remove_fragment_key(left_identity.fragment_key())
            .unwrap(),
        1
    );
    let after = retirement.finish();
    assert!(!after.contains_key(&left_key));
    assert!(after.contains_key(&right_key));
    assert_eq!(after.fragment_count(), 1);
}

#[test]
fn candidate_path_copies_changed_nodes_and_shares_unchanged_descriptors() {
    let first_series = SeriesRef::new(1);
    let second_series = SeriesRef::new(2);
    let first = frozen_float(first_series, 1_000, 1.0, 1);
    let second = frozen_float(second_series, 1_000, 2.0, 1);
    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(identity("metrics", &first, 1), Arc::clone(&first))
        .unwrap();
    builder
        .insert_fragment(identity("metrics", &second, 2), Arc::clone(&second))
        .unwrap();
    let generation_one = builder.finish();

    let second_key = LiveSampleKey::new(
        FrozenFragmentKey::new(
            partition("metrics"),
            second.start_ms(),
            second.end_ms(),
            second.lane(),
        )
        .unwrap(),
        second_series,
        SampleKind::Float,
    );
    let old_second_root = &generation_one.value(&second_key).unwrap().roots[0].1;

    let first_next = frozen_float(first_series, 2_000, 3.0, 2);
    let mut candidate = LiveSampleStoreBuilder::from_store(&generation_one);
    candidate
        .insert_fragment(identity("metrics", &first_next, 3), first_next)
        .unwrap();
    let generation_two = candidate.finish();
    let new_second_root = &generation_two.value(&second_key).unwrap().roots[0].1;

    assert!(Arc::ptr_eq(old_second_root, new_second_root));
    assert!(!Arc::ptr_eq(
        generation_one.root.as_ref().unwrap(),
        generation_two.root.as_ref().unwrap()
    ));
}

#[test]
fn retired_payload_reclaims_only_after_the_last_old_root_drops() {
    let series = SeriesRef::new(1);
    let fragment = frozen_float(series, 1_000, 1.0, 1);
    let weak = Arc::downgrade(&fragment);
    let fragment_identity = identity("metrics", &fragment, 1);
    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(fragment_identity.clone(), Arc::clone(&fragment))
        .unwrap();
    let generation_one = builder.finish();

    let mut candidate = LiveSampleStoreBuilder::from_store(&generation_one);
    candidate
        .remove_fragment_key(fragment_identity.fragment_key())
        .unwrap();
    let generation_two = candidate.finish();
    assert!(generation_two.is_empty());

    drop(fragment);
    assert!(weak.upgrade().is_some());
    drop(generation_one);
    assert!(weak.upgrade().is_none());
}

#[test]
fn exact_final_handoff_clears_candidate_without_invalidating_the_old_root() {
    let first_series = SeriesRef::new(1);
    let second_series = SeriesRef::new(2);
    let first = frozen_float(first_series, 1_000, 1.0, 1);
    let second = frozen_float(second_series, 2_000, 2.0, 2);
    let first_identity = identity("metrics", &first, 1);
    let second_identity = identity("metrics", &second, 2);
    let mut exact = vec![first_identity.clone(), second_identity.clone()];
    exact.sort_unstable();

    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(first_identity, Arc::clone(&first))
        .unwrap();
    builder
        .insert_fragment(second_identity, Arc::clone(&second))
        .unwrap();
    let generation_one = builder.finish();
    let required_revision = generation_one.required_catalog_revision();

    let mut candidate = LiveSampleStoreBuilder::from_store(&generation_one);
    candidate.clear_if_exact_fragments(&exact).unwrap();
    let generation_two = candidate.finish();

    assert!(generation_two.is_empty());
    assert_eq!(generation_two.fragment_count(), 0);
    assert_eq!(
        generation_two.required_catalog_revision(),
        required_revision,
        "an empty successor must retain the established label-snapshot floor"
    );
    assert_eq!(generation_one.fragment_count(), 2);
    assert_eq!(generation_one.ordered_runs(0, 9_999).unwrap().len(), 2);
}

#[test]
fn sample_store_exact_fragment_validation_requires_sorted_duplicate_free_identity() {
    let first = frozen_float(SeriesRef::new(1), 1_000, 1.0, 1);
    let second = frozen_float(SeriesRef::new(2), 2_000, 2.0, 2);
    let first_identity = identity("metrics", &first, 1);
    let second_identity = identity("metrics", &second, 2);
    let mut exact = vec![first_identity.clone(), second_identity.clone()];
    exact.sort_unstable();

    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(first_identity, Arc::clone(&first))
        .unwrap();
    builder
        .insert_fragment(second_identity, Arc::clone(&second))
        .unwrap();
    let store = builder.finish();

    store.validate_exact_fragment_identities(&exact).unwrap();

    let mut reversed = exact.clone();
    reversed.reverse();
    assert!(
        store
            .validate_exact_fragment_identities(&reversed)
            .unwrap_err()
            .to_string()
            .contains("does not exactly match")
    );
    let duplicate = vec![exact[0].clone(), exact[0].clone()];
    assert!(
        store
            .validate_exact_fragment_identities(&duplicate)
            .unwrap_err()
            .to_string()
            .contains("does not exactly match")
    );
}

#[test]
fn final_handoff_certificate_mismatch_fails_without_changing_the_candidate() {
    let series = SeriesRef::new(1);
    let fragment = frozen_float(series, 1_000, 1.0, 1);
    let fragment_identity = identity("metrics", &fragment, 1);
    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(fragment_identity, Arc::clone(&fragment))
        .unwrap();
    let generation_one = builder.finish();

    let mut candidate = LiveSampleStoreBuilder::from_store(&generation_one);
    let error = candidate.clear_if_exact_fragments(&[]).unwrap_err();
    assert!(
        error
            .to_string()
            .contains("does not exactly cover every committed fragment")
    );
    let unchanged = candidate.finish();
    assert_eq!(unchanged.fragment_count(), 1);
    assert_eq!(unchanged.ordered_runs(0, 9_999).unwrap().len(), 1);
}

#[test]
fn iterative_traversal_accepts_depth_64_and_rejects_depth_65_and_count_overflow() {
    let series = SeriesRef::new(9);
    let fragment = frozen_float(series, 1_000, 1.0, 1);
    let key = LiveSampleKey::new(
        FrozenFragmentKey::new(
            partition("metrics"),
            fragment.start_ms(),
            fragment.end_ms(),
            fragment.lane(),
        )
        .unwrap(),
        series,
        SampleKind::Float,
    );
    let descriptor_identity = Arc::new(DescriptorIdentity {
        key,
        codec: fragment
            .run_exact(series, SampleKind::Float)
            .unwrap()
            .encoded
            .codec_name(),
    });
    let run = fragment.run_exact(series, SampleKind::Float).unwrap();
    let leaf = |sequence| {
        let order = RecordedSampleOrder::new(MessageSequence::new(sequence), 0);
        DescriptorNode::leaf(
            Arc::clone(&descriptor_identity),
            Arc::clone(&fragment),
            run,
            RecordedSampleOrderRange::one(order),
        )
        .unwrap()
    };

    let mut root = leaf(1);
    for sequence in 2..=64 {
        root = DescriptorNode::concat(root, leaf(sequence)).unwrap();
    }
    assert_eq!(root.meta().depth, 64);
    let mut traversed = Vec::new();
    root.append_leaves(&mut traversed).unwrap();
    assert_eq!(traversed.len(), 64);
    assert!(DescriptorNode::concat(root, leaf(65)).is_err());

    let mut overflowing = leaf(100);
    match Arc::get_mut(&mut overflowing).unwrap() {
        DescriptorNode::Leaf(leaf) => leaf.meta.samples = u64::MAX,
        DescriptorNode::Concat(_) => unreachable!("the helper creates a leaf"),
    }
    assert!(DescriptorNode::concat(overflowing, leaf(101)).is_err());
}

#[test]
fn equal_timestamp_keeps_the_newer_descriptor_value() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "temperature"), ("sensor", "a")],
    );
    let older = frozen_float(series, 1_000, 1.0, 1);
    let newer = frozen_float(series, 1_000, 9.0, 2);
    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(identity("metrics", &older, 1), older)
        .unwrap();
    builder
        .insert_fragment(identity("metrics", &newer, 2), newer)
        .unwrap();
    let view = super::super::FrozenHeadReadView::from_sample_store(builder.finish());

    let result = view
        .query_selector(
            &label_store,
            &SegmentSelector::metric("temperature"),
            0,
            2_000,
        )
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].samples, vec![(1_000, 9.0)]);
}

#[test]
fn exact_subrange_presence_does_not_charge_a_sparse_regex_value() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let sparse = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "temperature"), ("sensor", "sparse")],
    );
    let present = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "temperature"), ("sensor", "present")],
    );
    let sparse_fragment = frozen_float(sparse, 100, 1.0, 1);
    let present_fragment = frozen_float(present, 900, 2.0, 2);
    let mut builder = LiveSampleStoreBuilder::new();
    builder
        .insert_fragment(identity("metrics", &sparse_fragment, 1), sparse_fragment)
        .unwrap();
    builder
        .insert_fragment(identity("metrics", &present_fragment, 2), present_fragment)
        .unwrap();
    let view = super::super::FrozenHeadReadView::from_sample_store(builder.finish());
    let selector =
        SegmentSelector::with_metric("temperature", vec![LabelMatcher::regex("sensor", ".*")]);
    let mut budget = QueryBudget::new(QueryLimits {
        max_regex_values_examined: Some(1),
        ..QueryLimits::unlimited()
    });

    let result = view
        .query_selector_with_budget(&label_store, &selector, 800, 1_000, &mut budget)
        .unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].samples, vec![(900, 2.0)]);
    assert_eq!(budget.stats().regex_values_examined, 1);
}

#[test]
fn slow_reader_remains_deterministic_across_publication_and_reclamation() {
    let mut label_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let series = labels(
        &mut label_store,
        &[(METRIC_NAME_LABEL, "parallel"), ("worker", "one")],
    );
    let first = frozen_float(series, 1_000, 1.0, 1);
    let second = frozen_float(series, 2_000, 2.0, 2);
    let mut initial = LiveSampleStoreBuilder::new();
    initial
        .insert_fragment(identity("metrics", &first, 1), first)
        .unwrap();
    let generation_one = initial.finish();
    let mut candidate = LiveSampleStoreBuilder::from_store(&generation_one);
    candidate
        .insert_fragment(identity("metrics", &second, 2), second)
        .unwrap();
    let generation_two = candidate.finish();

    let old_view = Arc::new(super::super::FrozenHeadReadView::from_sample_store(
        generation_one,
    ));
    let new_view = super::super::FrozenHeadReadView::from_sample_store(generation_two);
    let labels = Arc::new(label_store);
    let pinned = Arc::new(Barrier::new(2));
    let release = Arc::new(Barrier::new(2));
    let reader = {
        let view = Arc::clone(&old_view);
        let labels = Arc::clone(&labels);
        let pinned = Arc::clone(&pinned);
        let release = Arc::clone(&release);
        thread::spawn(move || {
            let first = view
                .query_selector(
                    labels.as_ref(),
                    &SegmentSelector::metric("parallel"),
                    0,
                    3_000,
                )
                .unwrap();
            assert_eq!(first[0].samples, vec![(1_000, 1.0)]);
            pinned.wait();
            release.wait();
            let second = view
                .query_selector(
                    labels.as_ref(),
                    &SegmentSelector::metric("parallel"),
                    0,
                    3_000,
                )
                .unwrap();
            assert_eq!(second, first);
        })
    };

    pinned.wait();
    let current = new_view
        .query_selector(
            labels.as_ref(),
            &SegmentSelector::metric("parallel"),
            0,
            3_000,
        )
        .unwrap();
    assert_eq!(current[0].samples, vec![(1_000, 1.0), (2_000, 2.0)]);
    release.wait();
    reader.join().unwrap();
}
