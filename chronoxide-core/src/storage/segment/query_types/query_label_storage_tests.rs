use super::labels::{
    COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN, COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES,
    COMPACT_QUERY_LABEL_OBJECT_BYTES, CompactQueryLabelArena, CompactQueryLabelAtomChunk,
    CompactQueryLabelPair, intern_query_label_atom, modeled_arc_allocation_bytes,
    modeled_arc_str_allocation_bytes,
};
use super::{
    DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES, QueryLabelInterner, QueryLabelStoragePolicy,
    QueryLabelStorageStats, QueryLabels, query_labels_series_id,
};
use crate::storage::segment::{METRIC_NAME_LABEL, segment_series_id};
use std::collections::HashSet;
use std::hash::{BuildHasherDefault, Hasher};
use std::io;
use std::sync::{Arc, OnceLock};

#[derive(Default)]
struct ConstantHasher;

impl Hasher for ConstantHasher {
    fn finish(&self) -> u64 {
        0
    }

    fn write(&mut self, _bytes: &[u8]) {}
}

fn labels(metric: &str, service: &str) -> Vec<(String, String)> {
    vec![
        (METRIC_NAME_LABEL.to_owned(), metric.to_owned()),
        ("service_name".to_owned(), service.to_owned()),
        ("synthetic".to_owned(), "+Inf".to_owned()),
    ]
}

#[test]
fn shared_query_labels_reuse_atoms_without_touching_owned_compatibility() {
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
    let first = interner.intern_labels(labels("requests_total", "api"));
    let second = interner.intern_labels(labels("errors_total", "api"));

    let first_ptrs = first.shared_atom_ptrs().expect("shared labels");
    let second_ptrs = second.shared_atom_ptrs().expect("shared labels");
    assert!(std::ptr::eq(first_ptrs[0].0, second_ptrs[0].0));
    assert!(std::ptr::eq(first_ptrs[1].0, second_ptrs[1].0));
    assert!(std::ptr::eq(first_ptrs[1].1, second_ptrs[1].1));
    assert!(std::ptr::eq(first_ptrs[2].0, second_ptrs[2].0));
    assert!(std::ptr::eq(first_ptrs[2].1, second_ptrs[2].1));

    assert_eq!(first.pairs().count(), 3);
    assert_eq!(first.to_vec(), labels("requests_total", "api"));
    assert!(!first.owned_compatibility_materialized());
    assert!(!second.owned_compatibility_materialized());
    assert_eq!(
        interner.stats(),
        QueryLabelStorageStats {
            label_sets: 2,
            atom_lookups: 12,
            atom_hits: 5,
            atom_misses: 7,
            unique_content_bytes: 62,
            ..QueryLabelStorageStats::default()
        }
    );

    assert_eq!(first.to_vec(), labels("requests_total", "api"));
    assert!(!first.owned_compatibility_materialized());
    assert!(!second.owned_compatibility_materialized());
}

#[test]
fn owned_query_labels_are_the_default_and_keep_the_legacy_representation() {
    let mut interner = QueryLabelInterner::default();
    assert_eq!(interner.policy(), QueryLabelStoragePolicy::OwnedStrings);
    let owned = interner.intern_labels(labels("requests_total", "api"));

    assert!(owned.shared_atom_ptrs().is_none());
    assert_eq!(owned.to_vec(), labels("requests_total", "api"));
    assert_eq!(
        interner.stats(),
        QueryLabelStorageStats {
            label_sets: 1,
            ..QueryLabelStorageStats::default()
        }
    );
}

#[test]
fn shared_labels_remain_valid_after_the_session_interner_is_dropped() {
    let labels = {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
        interner.intern_labels(labels("requests_total", "api"))
    };

    assert_eq!(
        labels.pairs().collect::<Vec<_>>(),
        vec![
            (METRIC_NAME_LABEL, "requests_total"),
            ("service_name", "api"),
            ("synthetic", "+Inf"),
        ]
    );
    assert!(!labels.owned_compatibility_materialized());
}

#[test]
fn atom_interning_resolves_content_under_forced_hash_collisions() {
    let mut atoms = HashSet::<Arc<str>, BuildHasherDefault<ConstantHasher>>::default();
    let (alpha, alpha_inserted) = intern_query_label_atom(&mut atoms, "alpha".to_owned());
    let (beta, beta_inserted) = intern_query_label_atom(&mut atoms, "beta".to_owned());
    let (alpha_again, alpha_again_inserted) =
        intern_query_label_atom(&mut atoms, "alpha".to_owned());

    assert!(alpha_inserted);
    assert!(beta_inserted);
    assert!(!alpha_again_inserted);
    assert_eq!(atoms.len(), 2);
    assert_ne!(alpha.as_ref(), beta.as_ref());
    assert!(Arc::ptr_eq(&alpha, &alpha_again));
}

#[test]
fn owned_and_shared_labels_have_identical_order_and_content_semantics() {
    let expected = labels("requests_total", "api");
    let owned = QueryLabels::from_vec(expected.clone());
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::SharedAtoms);
    let shared = interner.intern_labels(expected.clone());
    let different = interner.intern_labels(labels("requests_total", "worker"));

    assert_eq!(owned, shared);
    assert_eq!(owned.cmp(&shared), std::cmp::Ordering::Equal);
    assert_eq!(shared, expected);
    assert_ne!(shared, different);
    assert!(!shared.owned_compatibility_materialized());
}

#[test]
fn compact_label_pairs_are_exactly_eight_bytes() {
    assert_eq!(std::mem::size_of::<CompactQueryLabelPair>(), 8);
    assert_eq!(std::mem::align_of::<CompactQueryLabelPair>(), 4);
}

#[test]
fn compact_arena_base_and_label_blocks_charge_modeled_live_allocations() {
    let arena = Arc::new(CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap());
    let base = arena.snapshot();
    let expected_base = modeled_arc_allocation_bytes::<CompactQueryLabelArena>()
        + u64::try_from(
            arena
                .atom_chunks
                .len()
                .saturating_mul(std::mem::size_of::<OnceLock<CompactQueryLabelAtomChunk>>()),
        )
        .unwrap();
    assert_eq!(base.atom_bytes, expected_base);
    assert_eq!(
        base.hash_directory_bytes,
        COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES
    );
    assert_eq!(
        base.current_bytes,
        expected_base + COMPACT_QUERY_LABEL_HASH_TABLE_FIXED_RESERVE_BYTES
    );
    assert_eq!(base.peak_bytes, base.current_bytes);

    let labels = arena
        .intern_pairs(vec![
            (METRIC_NAME_LABEL.to_owned(), "requests_total".to_owned()),
            ("service".to_owned(), "api".to_owned()),
        ])
        .unwrap();
    let after_intern = arena.snapshot();
    let expected_label_block =
        COMPACT_QUERY_LABEL_OBJECT_BYTES + 2 * std::mem::size_of::<CompactQueryLabelPair>() as u64;
    assert_eq!(after_intern.pair_bytes, expected_label_block);

    let clone = labels.clone();
    drop(labels);
    assert_eq!(arena.snapshot().pair_bytes, expected_label_block);
    drop(clone);
    assert_eq!(arena.snapshot().pair_bytes, 0);
    assert_eq!(
        arena.snapshot().current_bytes,
        after_intern.current_bytes - expected_label_block
    );
}

#[test]
fn compact_atom_charge_includes_arc_str_tail_alignment() {
    let arena = Arc::new(CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap());
    let before = arena.snapshot();
    let labels = arena
        .intern_pairs(vec![("a".to_owned(), "bc".to_owned())])
        .unwrap();
    let after = arena.snapshot();
    let atom_chunk_bytes =
        (COMPACT_QUERY_LABEL_ATOM_CHUNK_LEN * std::mem::size_of::<OnceLock<Arc<str>>>()) as u64;
    let expected_atoms = atom_chunk_bytes
        + modeled_arc_str_allocation_bytes(1).unwrap()
        + modeled_arc_str_allocation_bytes(2).unwrap();
    assert_eq!(after.atom_bytes - before.atom_bytes, expected_atoms);
    for content_bytes in 0..=(2 * std::mem::align_of::<usize>() as u64) {
        let allocation = modeled_arc_str_allocation_bytes(content_bytes).unwrap();
        assert_eq!(allocation % std::mem::align_of::<usize>() as u64, 0);
        assert!(allocation >= 2 * std::mem::size_of::<usize>() as u64 + content_bytes);
    }
    drop(labels);
}

#[test]
fn compact_empty_label_set_still_charges_its_shared_object_once() {
    let arena = Arc::new(CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap());
    let labels = arena.intern_pairs(Vec::new()).unwrap();
    assert_eq!(
        arena.snapshot().pair_bytes,
        COMPACT_QUERY_LABEL_OBJECT_BYTES
    );
    drop(labels);
    assert_eq!(arena.snapshot().pair_bytes, 0);
}

#[test]
fn compact_labels_reuse_ids_without_retaining_owned_compatibility() {
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::CompactIds);
    let first = interner
        .try_intern_labels(labels("requests_total", "api"))
        .unwrap();
    let second = interner
        .try_intern_labels(labels("errors_total", "api"))
        .unwrap();

    assert!(first.uses_compact_ids());
    assert!(second.uses_compact_ids());
    assert_eq!(first.pairs().count(), 3);
    assert_eq!(first.to_vec(), labels("requests_total", "api"));
    assert!(!first.owned_compatibility_materialized());
    assert!(!second.owned_compatibility_materialized());
    let stats = interner.stats();
    assert_eq!(stats.compact_label_sets, 2);
    assert_eq!(stats.compact_pairs, 6);
    assert_eq!(stats.compact_atom_lookups, 12);
    assert_eq!(stats.compact_atom_hits + stats.compact_atom_misses, 12);
    assert_eq!(stats.compact_unique_strings, stats.compact_atom_misses);
    assert_eq!(
        stats.compact_arena_current_bytes,
        stats
            .compact_atom_bytes
            .saturating_add(stats.compact_pair_bytes)
            .saturating_add(stats.compact_hash_directory_bytes)
            .saturating_add(stats.compact_translation_bytes)
    );

    assert_eq!(first.to_vec(), labels("requests_total", "api"));
    assert!(!first.owned_compatibility_materialized());
    assert_eq!(interner.stats().compact_compatibility_materializations, 0);
    assert!(!second.owned_compatibility_materialized());
}

#[test]
fn compact_labels_keep_the_arena_alive_after_the_interner_is_dropped() {
    let labels = {
        let mut interner = QueryLabelInterner::default();
        interner.set_policy(QueryLabelStoragePolicy::CompactIds);
        interner
            .try_intern_labels(labels("requests_total", "api"))
            .unwrap()
    };

    assert_eq!(
        labels.pairs().collect::<Vec<_>>(),
        vec![
            (METRIC_NAME_LABEL, "requests_total"),
            ("service_name", "api"),
            ("synthetic", "+Inf"),
        ]
    );
    assert!(!labels.owned_compatibility_materialized());
}

#[test]
fn compact_labels_compare_by_content_across_independent_arenas() {
    let expected = labels("requests_total", "api");
    let mut left_interner = QueryLabelInterner::default();
    left_interner.set_policy(QueryLabelStoragePolicy::CompactIds);
    let left = left_interner.try_intern_labels(expected.clone()).unwrap();
    let mut right_interner = QueryLabelInterner::default();
    right_interner.set_policy(QueryLabelStoragePolicy::CompactIds);
    let right = right_interner.try_intern_labels(expected.clone()).unwrap();
    let different = right_interner
        .try_intern_labels(labels("requests_total", "worker"))
        .unwrap();

    assert_eq!(left, right);
    assert_eq!(left.cmp(&right), std::cmp::Ordering::Equal);
    assert_eq!(left, expected);
    assert_ne!(left, different);
    assert_eq!(query_labels_series_id(&left), segment_series_id(&expected));
    assert!(!left.owned_compatibility_materialized());
    assert!(!right.owned_compatibility_materialized());
}

#[test]
fn compact_labels_distinguish_missing_from_explicit_empty_values() {
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::CompactIds);
    let missing = interner.try_intern_labels(Vec::new()).unwrap();
    let explicit_empty = interner
        .try_intern_labels(vec![("zone".to_owned(), String::new())])
        .unwrap();

    assert!(missing.is_empty());
    assert_eq!(
        explicit_empty.pairs().collect::<Vec<_>>(),
        vec![("zone", "")]
    );
    assert_ne!(missing, explicit_empty);
}

#[test]
fn compact_labels_project_terminal_names_without_materializing_strings() {
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::CompactIds);
    let labels = interner
        .try_intern_labels(vec![
            (METRIC_NAME_LABEL.to_owned(), "requests_total".to_owned()),
            ("instance".to_owned(), "api-1".to_owned()),
            ("service".to_owned(), "api".to_owned()),
        ])
        .unwrap();
    let projected = labels.try_retain_names(&["service".to_owned()]).unwrap();

    assert_eq!(projected.pairs().collect::<Vec<_>>(), [("service", "api")]);
    assert!(!projected.owned_compatibility_materialized());
    let stats = interner.stats();
    assert_eq!(stats.compact_label_sets, 2);
    assert_eq!(stats.compact_pairs, 4);
    assert_eq!(stats.compact_compatibility_materializations, 0);
    assert_eq!(
        stats.compact_arena_current_bytes,
        stats
            .compact_atom_bytes
            .saturating_add(stats.compact_pair_bytes)
            .saturating_add(stats.compact_hash_directory_bytes)
            .saturating_add(stats.compact_translation_bytes)
    );
}

#[test]
fn compact_typed_scalar_projection_reuses_ids_and_derived_metric_atom() {
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::CompactIds);
    let source = interner
        .try_intern_labels(labels("requests", "api"))
        .unwrap();

    let first = interner
        .try_project_metric_suffix_labels(&source, "_count")
        .unwrap();
    let after_first = interner.stats();
    let second = interner
        .try_project_metric_suffix_labels(&source, "_count")
        .unwrap();
    let after_second = interner.stats();

    let expected = labels("requests_count", "api");
    assert_eq!(first, expected);
    assert_eq!(second, expected);
    assert_eq!(query_labels_series_id(&first), segment_series_id(&expected));
    assert!(source.uses_compact_ids());
    assert!(first.uses_compact_ids());
    assert!(second.uses_compact_ids());
    assert!(!source.owned_compatibility_materialized());
    assert!(!first.owned_compatibility_materialized());
    assert!(!second.owned_compatibility_materialized());
    assert_eq!(
        after_second.compact_atom_lookups, after_first.compact_atom_lookups,
        "the derived metric atom must be reused without another UTF-8 lookup",
    );
    assert_eq!(after_second.compact_label_sets, 3);
    assert_eq!(after_second.compact_pairs, 9);
    assert_eq!(after_second.compact_compatibility_materializations, 0);
    assert_eq!(
        after_second.compact_arena_current_bytes,
        after_second
            .compact_atom_bytes
            .saturating_add(after_second.compact_pair_bytes)
            .saturating_add(after_second.compact_hash_directory_bytes)
            .saturating_add(after_second.compact_translation_bytes)
    );
}

#[test]
fn compact_budget_refusal_occurs_before_atoms_and_never_falls_back() {
    let mut interner = QueryLabelInterner::default();
    interner.set_policy(QueryLabelStoragePolicy::CompactIds);
    interner.set_compact_arena_max_bytes(1).unwrap();
    assert_eq!(interner.stats().compact_arena_budget_bytes, 1);

    let error = interner
        .try_intern_labels(labels("requests_total", "api"))
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert_eq!(interner.policy(), QueryLabelStoragePolicy::CompactIds);
    let stats = interner.stats();
    assert_eq!(stats.compact_atom_lookups, 0);
    assert_eq!(stats.compact_atom_misses, 0);
    assert_eq!(stats.compact_label_sets, 0);
    assert_eq!(stats.compact_arena_current_bytes, 0);
    assert_eq!(stats.compact_arena_admission_refusals, 1);
}

#[test]
fn compact_hash_buckets_verify_full_content_under_collisions() {
    let arena = CompactQueryLabelArena::new(DEFAULT_QUERY_LABEL_ARENA_MAX_BYTES).unwrap();
    let mut state = arena.state.lock().unwrap();
    let alpha = arena
        .intern_locked_with_hash(&mut state, "alpha", 7)
        .unwrap();
    let beta = arena
        .intern_locked_with_hash(&mut state, "beta", 7)
        .unwrap();
    let alpha_again = arena
        .intern_locked_with_hash(&mut state, "alpha", 7)
        .unwrap();

    assert_ne!(alpha, beta);
    assert_eq!(alpha, alpha_again);
    assert_eq!(arena.resolve(alpha), "alpha");
    assert_eq!(arena.resolve(beta), "beta");
    assert_eq!(state.lookups, 3);
    assert_eq!(state.hits, 1);
    assert_eq!(state.misses, 2);
}
