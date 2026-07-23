use std::hash::{Hash, Hasher};

use super::super::normalizer::{normalize_label_key, normalize_label_value};
use super::flat::{
    DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY, FlatInternedLabelSetHash, InternedKeyValue,
    InternedKeyValueStorage, MAX_INTERNED_KEY_VALUE_PAGES, PagedInternedKeyValues, SeriesLoc,
    encode_interned_labelset_into,
};
use super::keyset::SeriesEntry;
use super::*;

use crate::labels::{
    ArenaSymbolTableError, DefaultSymbolTable, KeySetId, KeyValueRef, MAX_LABEL_NAME_BYTES,
    MAX_LABEL_VALUE_BYTES, SeriesRef, SymbolId, SymbolTable, SymbolTableError, SymbolTableStats,
    ValueCode,
};
use crate::otlp_labelset::{
    CanonicalLabelSet, OtlpLabelSetInterner, PreparedOtlpLabelSetScratch,
    PreparedOtlpResourceLabels, intern_prepared_labelset,
};
use opentelemetry_proto::tonic::common::v1::any_value::Value as AnyValue;
use opentelemetry_proto::tonic::common::v1::{AnyValue as OtlpAnyValue, KeyValue};

fn hash_labelset(labels: &[KeyValueRef<'_>]) -> u64 {
    debug_assert!(
        labels.windows(2).all(|pair| pair[0].key < pair[1].key),
        "LabelSet must be canonical (sorted by key, unique keys)"
    );
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for label in labels {
        let key_norm = normalize_label_key(label.key);
        let value_norm = normalize_label_value(label.value);
        key_norm.as_ref().hash(&mut hasher);
        value_norm.as_ref().hash(&mut hasher);
    }
    hasher.finish()
}

#[test]
fn buffer_stats_display_preserves_public_report_type_names() {
    let naive = NaiveLabelSetStore::default().buffer_stats().to_string();
    let flat_store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();
    let flat = flat_store.buffer_stats().to_string();
    let keyset_store: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
    let keyset = keyset_store.buffer_stats().to_string();
    let packed = keyset_store.seal_fixed_width().buffer_stats().to_string();

    assert!(
        naive
            .starts_with("type=chronoxide_core::labels::interners::NaiveLabelSetStoreBufferStats ")
    );
    assert!(flat.starts_with(
        "type=chronoxide_core::labels::interners::FlatInternedLabelSetStoreBufferStats "
    ));
    assert!(
        keyset.starts_with(
            "type=chronoxide_core::labels::interners::KeySetLabelSetStoreBufferStats "
        )
    );
    assert!(packed.starts_with(
        "type=chronoxide_core::labels::interners::PackedKeySetLabelSetStoreBufferStats "
    ));
}

fn decode(store: &impl LabelSetStore, series: SeriesRef) -> Vec<(String, String)> {
    let mut labels = Vec::new();
    store.visit_labelset(series, |key, value| {
        labels.push((key.to_string(), value.to_string()));
    });
    labels
}

fn owned_labels(labels: &[KeyValueRef<'_>]) -> Vec<(String, String)> {
    labels
        .iter()
        .map(|label| (label.key.to_string(), label.value.to_string()))
        .collect()
}

fn intern_with_hash(
    store: &mut FlatInternedLabelSetStore,
    labels: &[KeyValueRef<'_>],
    forced_hash: u64,
) -> SeriesRef {
    encode_interned_labelset_into::<false, _, _>(
        &mut store.symbols,
        &mut store.encoded_scratch,
        labels.iter().copied(),
        std::collections::hash_map::DefaultHasher::new(),
    )
    .unwrap();
    store.intern_encoded(forced_hash).unwrap()
}

#[test]
fn interned_dedup_interns_same_series() {
    let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();
    let labels = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-123")),
    ];

    let s1 = store.intern(&labels).unwrap();
    let s2 = store.intern(&labels).unwrap();

    assert_eq!(s1, s2);
    assert_eq!(store.len(), 1);
    assert_eq!(
        decode(&store, s1),
        labels
            .iter()
            .map(|l| (l.key.to_string(), l.value.to_string()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn interned_repeated_hits_reuse_encoded_scratch_without_growing_persistent_data() {
    let keys = (0..23)
        .map(|index| format!("label_{index:02}"))
        .collect::<Vec<_>>();
    let values = (0..23)
        .map(|index| format!("value_{index:02}"))
        .collect::<Vec<_>>();
    let mut long_labels = vec![KeyValueRef::from(("__name__", "metric"))];
    long_labels.extend(
        keys.iter()
            .zip(&values)
            .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str()))),
    );
    let short_labels = [KeyValueRef::from(("__name__", "other_metric"))];
    let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();

    let long_series = store.intern(&long_labels).unwrap();
    let short_series = store.intern(&short_labels).unwrap();
    let initial = store.buffer_stats();
    let scratch_pointer = store.encoded_scratch.as_ptr();

    assert_eq!(initial.series_len, 2);
    assert_eq!(initial.key_values_len, 25);
    assert_eq!(initial.encoded_scratch_len, 0);
    assert!(initial.encoded_scratch_cap >= long_labels.len());

    for _ in 0..128 {
        assert_eq!(store.intern(&short_labels).unwrap(), short_series);
        assert_eq!(store.intern(&long_labels).unwrap(), long_series);
    }

    let after = store.buffer_stats();
    assert_eq!(after.series_len, initial.series_len);
    assert_eq!(after.series_cap, initial.series_cap);
    assert_eq!(after.key_values_len, initial.key_values_len);
    assert_eq!(after.key_values_cap, initial.key_values_cap);
    assert_eq!(after.encoded_scratch_len, 0);
    assert_eq!(after.encoded_scratch_cap, initial.encoded_scratch_cap);
    assert_eq!(store.encoded_scratch.as_ptr(), scratch_pointer);
    assert_eq!(
        decode(&store, long_series),
        long_labels
            .iter()
            .map(|label| (label.key.to_string(), label.value.to_string()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn interned_id_fingerprint_preserves_store_behavior_for_deterministic_trace() {
    let default_store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();
    assert_eq!(default_store.labelset_hash_kind(), "interned_ids_ahash");
    assert_eq!(default_store.key_value_storage_kind(), "contiguous");

    let mut trace: Vec<Vec<(String, String)>> = vec![
        Vec::new(),
        vec![("__name__".into(), "metric".into())],
        vec![("a".into(), "bc".into())],
        vec![("ab".into(), "c".into())],
        vec![("a".into(), "b".into()), ("c".into(), "d".into())],
    ];

    let base = std::iter::once(("__name__".to_string(), "wide_metric".to_string()))
        .chain((0..23).map(|index| (format!("label_{index:02}"), format!("value_{index:02}"))))
        .collect::<Vec<_>>();
    trace.push(base.clone());
    for changed_index in [0, base.len() / 2, base.len() - 1] {
        let mut changed = base.clone();
        changed[changed_index].1.push_str("_changed");
        trace.push(changed);
    }

    let raw_key = format!("{}tail", "é".repeat(MAX_LABEL_NAME_BYTES));
    let raw_value = format!("{}tail", "界".repeat(MAX_LABEL_VALUE_BYTES));
    let normalized_key = normalize_label_key(&raw_key).into_owned();
    let normalized_value = normalize_label_value(&raw_value).into_owned();
    trace.push(vec![
        ("__name__".into(), "normalized".into()),
        (raw_key, raw_value),
    ]);
    trace.push(vec![
        ("__name__".into(), "normalized".into()),
        (normalized_key, normalized_value),
    ]);

    let initial_trace = trace.clone();
    trace.extend(initial_trace.into_iter().rev());

    let mut canonical_strings: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_canonical_string_labelset_hash();
    let mut siphash_ids: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_siphash_labelset_hash();
    let mut ahash_ids_a: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    ahash_ids_a.labelset_ahash = ahash::RandomState::with_seeds(1, 2, 3, 4);
    let mut ahash_ids_b: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    ahash_ids_b.labelset_ahash = ahash::RandomState::with_seeds(5, 6, 7, 8);
    for row in &trace {
        let labels = row
            .iter()
            .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str())))
            .collect::<Vec<_>>();
        let canonical_series = canonical_strings.intern(&labels).unwrap();
        let siphash_series = siphash_ids.intern(&labels).unwrap();
        let ahash_series_a = ahash_ids_a.intern(&labels).unwrap();
        let ahash_series_b = ahash_ids_b.intern(&labels).unwrap();

        let expected = decode(&canonical_strings, canonical_series);
        for (series, store) in [
            (siphash_series, &siphash_ids),
            (ahash_series_a, &ahash_ids_a),
            (ahash_series_b, &ahash_ids_b),
        ] {
            assert_eq!(series, canonical_series);
            assert_eq!(decode(store, series), expected);
        }
    }

    for store in [&siphash_ids, &ahash_ids_a, &ahash_ids_b] {
        assert_eq!(store.len(), canonical_strings.len());
        assert_eq!(store.symbols().len(), canonical_strings.symbols().len());
    }
    assert_eq!(
        canonical_strings.buffer_stats().labelset_hash,
        "canonical_strings"
    );
    assert_eq!(
        siphash_ids.buffer_stats().labelset_hash,
        "interned_ids_siphash"
    );
    for store in [&ahash_ids_a, &ahash_ids_b] {
        assert_eq!(store.buffer_stats().labelset_hash, "interned_ids_ahash");
        assert_eq!(store.buffer_stats().key_values_storage, "contiguous");
    }
    for stats in [
        canonical_strings.buffer_stats(),
        siphash_ids.buffer_stats(),
        ahash_ids_a.buffer_stats(),
        ahash_ids_b.buffer_stats(),
    ] {
        assert_eq!(stats.fingerprint_calls, trace.len() as u64);
        assert_eq!(
            stats.fingerprint_calls,
            stats.series_len as u64 + stats.equality_matches
        );
        assert_eq!(
            stats.equality_checks,
            stats.equality_matches + stats.equality_mismatches
        );
    }
}

#[test]
fn interned_id_fingerprint_matches_canonical_store_for_randomized_trace() {
    let mut state = 0x6a09_e667_f3bc_c909_u64;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state
    };
    let mut pool = vec![Vec::<(String, String)>::new()];
    for row_index in 0..127 {
        let label_count = 1 + (next() as usize % 12);
        let mut row = Vec::with_capacity(label_count);
        row.push(("__name__".into(), format!("metric_{:03}", next() % 29)));
        for label_index in 1..label_count {
            let value = if row_index % 19 == 0 && label_index == label_count - 1 {
                format!("{}tail", "界".repeat(MAX_LABEL_VALUE_BYTES))
            } else {
                format!("value_{:04}", next() % 257)
            };
            row.push((format!("label_{label_index:02}"), value));
        }
        pool.push(row);
    }

    let mut canonical_strings: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_canonical_string_labelset_hash();
    let mut siphash_ids: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_siphash_labelset_hash();
    let mut ahash_ids_a: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    ahash_ids_a.labelset_ahash = ahash::RandomState::with_seeds(11, 12, 13, 14);
    let mut ahash_ids_b: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    ahash_ids_b.labelset_ahash = ahash::RandomState::with_seeds(15, 16, 17, 18);
    for _ in 0..4_096 {
        let row = &pool[next() as usize % pool.len()];
        let labels = row
            .iter()
            .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str())))
            .collect::<Vec<_>>();
        let canonical_series = canonical_strings.intern(&labels).unwrap();
        let expected = decode(&canonical_strings, canonical_series);
        let siphash_series = siphash_ids.intern(&labels).unwrap();
        let ahash_series_a = ahash_ids_a.intern(&labels).unwrap();
        let ahash_series_b = ahash_ids_b.intern(&labels).unwrap();
        for (series, store) in [
            (siphash_series, &siphash_ids),
            (ahash_series_a, &ahash_ids_a),
            (ahash_series_b, &ahash_ids_b),
        ] {
            assert_eq!(series, canonical_series);
            assert_eq!(decode(store, series), expected);
        }
    }

    for store in [&siphash_ids, &ahash_ids_a, &ahash_ids_b] {
        assert_eq!(store.len(), canonical_strings.len());
        assert_eq!(store.symbols().len(), canonical_strings.symbols().len());
    }
}

#[test]
fn paged_interned_rows_do_not_cross_page_boundaries() {
    let mut store: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_key_value_page_capacity(4);
    let first_labels = [
        KeyValueRef::from(("__name__", "first")),
        KeyValueRef::from(("a", "one")),
        KeyValueRef::from(("b", "two")),
    ];
    let second_labels = [
        KeyValueRef::from(("__name__", "second")),
        KeyValueRef::from(("a", "three")),
    ];
    let third_labels = [
        KeyValueRef::from(("__name__", "third")),
        KeyValueRef::from(("a", "four")),
    ];

    let first = store.intern(&first_labels).unwrap();
    let second = store.intern(&second_labels).unwrap();
    let third = store.intern(&third_labels).unwrap();

    assert_eq!(first, SeriesRef::new(0));
    assert_eq!(second, SeriesRef::new(1));
    assert_eq!(third, SeriesRef::new(2));
    assert_eq!(
        store.series,
        [
            SeriesLoc::paged(0, 0, 3).unwrap(),
            SeriesLoc::paged(1, 0, 2).unwrap(),
            SeriesLoc::paged(1, 2, 2).unwrap(),
        ]
    );
    let InternedKeyValueStorage::Paged(values) = &store.key_values else {
        panic!("default test layout must be paged");
    };
    assert_eq!(
        values.pages.iter().map(Vec::len).collect::<Vec<_>>(),
        [3, 4]
    );
    assert_eq!(decode(&store, first), owned_labels(&first_labels));
    assert_eq!(decode(&store, second), owned_labels(&second_labels));
    assert_eq!(decode(&store, third), owned_labels(&third_labels));
    for (series, labels) in [(second, &second_labels[..]), (third, &third_labels[..])] {
        let borrowed = store
            .labelset_symbol_ids(series)
            .iter()
            .map(|(key, value)| {
                (
                    store.symbols().resolve(key).to_string(),
                    store.symbols().resolve(value).to_string(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(borrowed, owned_labels(labels));
    }

    let stats = store.buffer_stats();
    assert_eq!(stats.key_values_storage, "paged");
    assert_eq!(stats.key_values_pages, 2);
    assert_eq!(stats.key_values_len, 7);
    assert!(stats.key_values_cap >= 8);
    assert!(store.estimate_size_bytes() >= store.estimate_used_bytes());
}

#[test]
fn flat_interned_row_view_borrows_exact_symbol_ids_for_both_layouts() {
    let labels = [
        KeyValueRef::from(("__name__", "requests.total")),
        KeyValueRef::from(("instance", "backend-1")),
        KeyValueRef::from(("zone", "west")),
    ];

    for mut store in [
        FlatInternedLabelSetStore::<DefaultSymbolTable>::with_contiguous_key_values(),
        FlatInternedLabelSetStore::<DefaultSymbolTable>::with_key_value_page_capacity(4),
    ] {
        let series = store.intern(&labels).unwrap();
        let expected = labels
            .iter()
            .map(|label| {
                (
                    store.symbols().lookup(label.key).unwrap(),
                    store.symbols().lookup(label.value).unwrap(),
                )
            })
            .collect::<Vec<_>>();
        let row = store.labelset_symbol_ids(series);

        assert_eq!(row.len(), labels.len());
        assert!(!row.is_empty());
        assert_eq!(row.iter().collect::<Vec<_>>(), expected);
        for (index, pair) in expected.iter().enumerate() {
            assert_eq!(row.get(index), Some(*pair));
            assert_eq!(row.symbol_ids_at(index), *pair);
        }
        assert_eq!(row.get(expected.len()), None);
    }
}

#[test]
fn packed_series_location_retains_the_eight_byte_layout_and_full_u16_bounds() {
    assert_eq!(std::mem::size_of::<SeriesLoc>(), 8);

    let loc = SeriesLoc::paged(u16::MAX as usize, u16::MAX as usize, u32::MAX as usize)
        .expect("maximum packed page and offset are representable");
    assert_eq!(loc.offset, u32::MAX);
    assert_eq!(loc.len, u32::MAX);
    assert_eq!(loc.paged_parts(), (u16::MAX as usize, u16::MAX as usize));
    assert_eq!(
        SeriesLoc::paged(MAX_INTERNED_KEY_VALUE_PAGES, 0, 1).unwrap_err(),
        LabelSetStoreError::LocatorCapacityExceeded {
            layout: "paged",
            field: "page_index",
            value: MAX_INTERNED_KEY_VALUE_PAGES,
            max: u16::MAX as usize,
        }
    );
}

#[test]
fn paged_interned_append_uses_the_maximum_packed_offset() {
    let value = InternedKeyValue {
        key: SymbolId(0),
        value: SymbolId(1),
    };
    let mut values = PagedInternedKeyValues::default();
    values
        .pages
        .push(vec![value; DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY - 1]);
    values.len = DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY - 1;

    let loc = values.append_row(&[value]).unwrap();

    assert_eq!(loc.paged_parts(), (0, u16::MAX as usize));
    assert_eq!(values.row(loc), [value]);
    assert_eq!(
        values.pages[0].len(),
        DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY
    );
}

#[test]
fn paged_interned_page_limit_is_non_mutating_and_clears_store_scratch() {
    let value = InternedKeyValue {
        key: SymbolId(0),
        value: SymbolId(1),
    };
    let mut pages = Vec::with_capacity(MAX_INTERNED_KEY_VALUE_PAGES);
    pages.resize_with(MAX_INTERNED_KEY_VALUE_PAGES, Vec::new);
    let mut values = PagedInternedKeyValues {
        pages,
        len: 0,
        page_capacity: DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY,
    };
    let max_page_loc = values.append_row(&[value]).unwrap();
    assert_eq!(max_page_loc.paged_parts(), (u16::MAX as usize, 0));
    values.pages[MAX_INTERNED_KEY_VALUE_PAGES - 1]
        .resize(DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY, value);
    values.len = DEFAULT_INTERNED_KEY_VALUE_PAGE_CAPACITY;
    let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore {
        key_values: InternedKeyValueStorage::Paged(values),
        labelset_hash: FlatInternedLabelSetHash::InternedIdsAHash,
        ..FlatInternedLabelSetStore::default()
    };
    let before = store.buffer_stats();
    let labels = [KeyValueRef::from(("__name__", "does_not_fit"))];

    let error = store.intern(&labels).unwrap_err();

    assert_eq!(
        error,
        LabelSetStoreError::LocatorCapacityExceeded {
            layout: "paged",
            field: "page_index",
            value: MAX_INTERNED_KEY_VALUE_PAGES,
            max: u16::MAX as usize,
        }
    );
    let after = store.buffer_stats();
    assert_eq!(after.series_len, 0);
    assert_eq!(after.key_values_len, before.key_values_len);
    assert_eq!(after.key_values_cap, before.key_values_cap);
    assert_eq!(after.key_values_pages, before.key_values_pages);
    assert_eq!(after.encoded_scratch_len, 0);
    assert!(after.encoded_scratch_cap >= labels.len());
    assert!(store.by_hash.is_empty());
    assert!(store.by_hash_collisions.is_empty());
    assert_eq!(after.fingerprint_calls, 0);
    assert_eq!(after.fingerprint_label_pairs, 0);
    assert_eq!(after.equality_checks, 0);
    assert_eq!(after.equality_matches, 0);
    assert_eq!(after.equality_mismatches, 0);
}

#[test]
fn paged_interned_allocates_an_oversized_row_in_one_page() {
    let mut store: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_key_value_page_capacity(4);
    let oversized = [
        KeyValueRef::from(("__name__", "oversized")),
        KeyValueRef::from(("a", "one")),
        KeyValueRef::from(("b", "two")),
        KeyValueRef::from(("c", "three")),
        KeyValueRef::from(("d", "four")),
    ];
    let short = [KeyValueRef::from(("__name__", "short"))];

    let oversized_ref = store.intern(&oversized).unwrap();
    let short_ref = store.intern(&short).unwrap();

    assert_eq!(
        store.series,
        [
            SeriesLoc::paged(0, 0, 5).unwrap(),
            SeriesLoc::paged(1, 0, 1).unwrap(),
        ]
    );
    assert_eq!(decode(&store, oversized_ref), owned_labels(&oversized));
    assert_eq!(decode(&store, short_ref), owned_labels(&short));
    let stats = store.buffer_stats();
    assert_eq!(stats.key_values_len, 6);
    assert!(stats.key_values_cap >= 9);
    assert_eq!(stats.key_values_pages, 2);
}

#[test]
fn paged_and_contiguous_interning_preserve_collision_and_assignment_semantics() {
    let mut paged: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_key_value_page_capacity(3);
    let mut contiguous = FlatInternedLabelSetStore::with_contiguous_key_values();
    let first = [
        KeyValueRef::from(("__name__", "requests")),
        KeyValueRef::from(("pod", "one")),
    ];
    let second = [
        KeyValueRef::from(("__name__", "requests")),
        KeyValueRef::from(("pod", "two")),
    ];
    let third = [
        KeyValueRef::from(("__name__", "requests")),
        KeyValueRef::from(("namespace", "prod")),
        KeyValueRef::from(("pod", "three")),
    ];
    let empty = [];
    let forced_hash = 7;

    let paged_refs = [
        &first[..],
        &second,
        &third,
        &empty,
        &first,
        &second,
        &third,
        &empty,
    ]
    .map(|labels| intern_with_hash(&mut paged, labels, forced_hash));
    let contiguous_refs = [
        &first[..],
        &second,
        &third,
        &empty,
        &first,
        &second,
        &third,
        &empty,
    ]
    .map(|labels| intern_with_hash(&mut contiguous, labels, forced_hash));

    assert_eq!(
        paged_refs,
        [
            SeriesRef::new(0),
            SeriesRef::new(1),
            SeriesRef::new(2),
            SeriesRef::new(3),
            SeriesRef::new(0),
            SeriesRef::new(1),
            SeriesRef::new(2),
            SeriesRef::new(3),
        ]
    );
    assert_eq!(contiguous_refs, paged_refs);
    assert_eq!(
        paged.by_hash_collisions[&forced_hash],
        [SeriesRef::new(1), SeriesRef::new(2), SeriesRef::new(3)]
    );
    assert_eq!(
        contiguous.by_hash_collisions[&forced_hash],
        paged.by_hash_collisions[&forced_hash]
    );
    for series in paged_refs[..4].iter().copied() {
        assert_eq!(decode(&paged, series), decode(&contiguous, series));
    }
    for stats in [paged.buffer_stats(), contiguous.buffer_stats()] {
        assert_eq!(stats.fingerprint_calls, 8);
        assert_eq!(stats.equality_checks, 16);
        assert_eq!(stats.equality_matches, 4);
        assert_eq!(stats.equality_mismatches, 12);
        assert_eq!(stats.collision_inserts, 3);
    }
    assert_eq!(paged.buffer_stats().key_values_storage, "paged");
    assert_eq!(contiguous.buffer_stats().key_values_storage, "contiguous");
    assert_eq!(
        paged.estimate_used_bytes(),
        contiguous.estimate_used_bytes()
            + paged.buffer_stats().key_values_pages * std::mem::size_of::<Vec<InternedKeyValue>>()
    );
}

#[test]
fn naive_dedup_interns_same_series() {
    let mut store: NaiveLabelSetStore = NaiveLabelSetStore::default();
    let labels = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-123")),
    ];

    let s1 = store.intern(&labels).unwrap();
    let s2 = store.intern(&labels).unwrap();

    assert_eq!(s1, s2);
    assert_eq!(store.len(), 1);
    assert_eq!(
        decode(&store, s1),
        labels
            .iter()
            .map(|l| (l.key.to_string(), l.value.to_string()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn flat_interned_is_more_memory_efficient_than_naive() {
    let series_count = 1000usize;

    let mut naive: NaiveLabelSetStore = NaiveLabelSetStore::default();
    let mut flat: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();

    for i in 0..series_count {
        let labels = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("container", if i % 2 == 0 { "web" } else { "sidecar" })),
            KeyValueRef::from(("namespace", if i % 3 == 0 { "payments" } else { "search" })),
            KeyValueRef::from(("pod", "backend")),
        ];
        naive.intern(&labels).unwrap();
        flat.intern(&labels).unwrap();
    }

    assert_eq!(naive.len(), flat.len());
    assert!(flat.estimate_used_bytes() < naive.estimate_used_bytes());
}

#[test]
fn keyset_dedup_interns_same_series() {
    let mut store: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
    let labels = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-123")),
    ];

    let s1 = store.intern(&labels).unwrap();
    let s2 = store.intern(&labels).unwrap();

    assert_eq!(s1, s2);
    assert_eq!(store.len(), 1);
    assert_eq!(
        decode(&store, s1),
        labels
            .iter()
            .map(|l| (l.key.to_string(), l.value.to_string()))
            .collect::<Vec<_>>()
    );
}

#[test]
fn keyset_internals_are_layered_correctly() {
    let mut store: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();

    let labels = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-123")),
    ];
    let labels2 = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-1231")),
    ];

    let labels3 = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments2")),
        KeyValueRef::from(("pod", "backend-1231")),
    ];

    let labels4 = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web2")),
        KeyValueRef::from(("namespace", "payments2")),
        KeyValueRef::from(("pod", "backend-1231")),
    ];

    let len1 = labels
        .iter()
        .map(|l| l.key.len() + l.value.len())
        .sum::<usize>();
    let len2 = labels2
        .iter()
        .map(|l| l.key.len() + l.value.len())
        .sum::<usize>();
    let len = len1.saturating_add(len2);
    println!("len = {}", len);

    let s1 = store.intern(&labels).unwrap();
    let s2 = store.intern(&labels2).unwrap();
    let s3 = store.intern(&labels3).unwrap();
    let s4 = store.intern(&labels4).unwrap();

    println!("{}", store.dump());

    assert_eq!(s1, SeriesRef(0));
    assert_eq!(s2, SeriesRef(1));
    assert_eq!(s3, SeriesRef(2));
    assert_eq!(s4, SeriesRef(3));
    assert_ne!(s1, s2);
    assert_eq!(store.len(), 4);

    assert_eq!(
        decode(&store, s1),
        labels
            .iter()
            .map(|l| (l.key.to_string(), l.value.to_string()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        decode(&store, s2),
        labels2
            .iter()
            .map(|l| (l.key.to_string(), l.value.to_string()))
            .collect::<Vec<_>>()
    );

    assert_eq!(
        decode(&store, s3),
        labels3
            .iter()
            .map(|l| (l.key.to_string(), l.value.to_string()))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        decode(&store, s4),
        labels4
            .iter()
            .map(|l| (l.key.to_string(), l.value.to_string()))
            .collect::<Vec<_>>()
    );

    assert_eq!(store.symbols.len(), 13);

    assert_eq!(store.keysets.id_to_keyset.len(), 1);
    assert_eq!(store.keysets.keyset_to_id.len(), 1);

    let key_name = store
        .symbols
        .lookup("__name__")
        .expect("missing __name__ symbol");
    let key_cluster = store
        .symbols
        .lookup("cluster")
        .expect("missing cluster symbol");
    let key_container = store
        .symbols
        .lookup("container")
        .expect("missing container symbol");
    let key_namespace = store
        .symbols
        .lookup("namespace")
        .expect("missing namespace symbol");
    let key_pod = store.symbols.lookup("pod").expect("missing pod symbol");

    let keys = store.keysets.resolve(KeySetId(0));
    assert_eq!(
        keys,
        &[key_name, key_cluster, key_container, key_namespace, key_pod]
    );

    assert_eq!(store.value_dicts.len(), 5);
    assert_eq!(
        store
            .value_dicts
            .get(&key_name)
            .expect("missing value dict for __name__")
            .cardinality(),
        1
    );
    assert_eq!(
        store
            .value_dicts
            .get(&key_cluster)
            .expect("missing value dict for cluster")
            .cardinality(),
        1
    );
    assert_eq!(
        store
            .value_dicts
            .get(&key_container)
            .expect("missing value dict for container")
            .cardinality(),
        2
    );
    assert_eq!(
        store
            .value_dicts
            .get(&key_namespace)
            .expect("missing value dict for namespace")
            .cardinality(),
        2
    );
    assert_eq!(
        store
            .value_dicts
            .get(&key_pod)
            .expect("missing value dict for pod")
            .cardinality(),
        2
    );

    let pod_dict = store.value_dicts.get(&key_pod).expect("missing pod dict");
    assert_eq!(
        store.symbols.resolve(pod_dict.resolve(ValueCode(0))),
        "backend-123"
    );
    assert_eq!(
        store.symbols.resolve(pod_dict.resolve(ValueCode(1))),
        "backend-1231"
    );

    let rows = &store.per_keyset_rows[0];
    assert_eq!(rows.key_count, 5);
    assert_eq!(rows.values.len(), 20);

    assert_eq!(
        rows.row_slice(0),
        &[
            ValueCode(0),
            ValueCode(0),
            ValueCode(0),
            ValueCode(0),
            ValueCode(0)
        ]
    );
    assert_eq!(
        rows.row_slice(1),
        &[
            ValueCode(0),
            ValueCode(0),
            ValueCode(0),
            ValueCode(0),
            ValueCode(1)
        ]
    );
    assert_eq!(
        rows.row_slice(2),
        &[
            ValueCode(0),
            ValueCode(0),
            ValueCode(0),
            ValueCode(1),
            ValueCode(1)
        ]
    );
    assert_eq!(
        rows.row_slice(3),
        &[
            ValueCode(0),
            ValueCode(0),
            ValueCode(1),
            ValueCode(1),
            ValueCode(1)
        ]
    );

    assert_eq!(store.series.len(), 4);
    assert_eq!(
        store.series[0],
        SeriesEntry {
            keyset_id: KeySetId(0),
            row: 0
        }
    );
    assert_eq!(
        store.series[1],
        SeriesEntry {
            keyset_id: KeySetId(0),
            row: 1
        }
    );
    assert_eq!(
        store.series[2],
        SeriesEntry {
            keyset_id: KeySetId(0),
            row: 2
        }
    );
    assert_eq!(
        store.series[3],
        SeriesEntry {
            keyset_id: KeySetId(0),
            row: 3
        }
    );

    assert_eq!(store.by_hash_collisions.len(), 0);
    assert_eq!(store.by_hash.len(), 4);
    let h1 = hash_labelset(&labels);
    let h2 = hash_labelset(&labels2);
    let h3 = hash_labelset(&labels3);
    let h4 = hash_labelset(&labels4);

    let mut hashes = std::collections::HashSet::new();
    assert!(hashes.insert(h1));
    assert!(hashes.insert(h2));
    assert!(hashes.insert(h3));
    assert!(hashes.insert(h4));
    assert_eq!(store.by_hash.get(&h1).copied(), Some(s1));
    assert_eq!(store.by_hash.get(&h2).copied(), Some(s2));
    assert_eq!(store.by_hash.get(&h3).copied(), Some(s3));
    assert_eq!(store.by_hash.get(&h4).copied(), Some(s4));
}

#[test]
fn keyset_fixed_width_seal_roundtrips() {
    let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
    let labels_a = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-123")),
    ];
    let labels_b = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "sidecar")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-456")),
    ];

    let s1 = builder.intern(&labels_a).unwrap();
    let s2 = builder.intern(&labels_b).unwrap();
    let decoded_builder_s1 = decode(&builder, s1);
    let decoded_builder_s2 = decode(&builder, s2);

    let sealed = builder.seal_fixed_width();
    let decoded_sealed_s1 = decode(&sealed, s1);
    let decoded_sealed_s2 = decode(&sealed, s2);

    assert_eq!(decoded_builder_s1, decoded_sealed_s1);
    assert_eq!(decoded_builder_s2, decoded_sealed_s2);
}

#[test]
fn keyset_bit_packed_seal_roundtrips() {
    let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
    let labels_a = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "web")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-123")),
    ];
    let labels_b = [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", "sidecar")),
        KeyValueRef::from(("namespace", "payments")),
        KeyValueRef::from(("pod", "backend-456")),
    ];

    let s1 = builder.intern(&labels_a).unwrap();
    let s2 = builder.intern(&labels_b).unwrap();
    let decoded_builder_s1 = decode(&builder, s1);
    let decoded_builder_s2 = decode(&builder, s2);

    let sealed = builder.seal_bit_packed();
    let decoded_sealed_s1 = decode(&sealed, s1);
    let decoded_sealed_s2 = decode(&sealed, s2);

    assert_eq!(decoded_builder_s1, decoded_sealed_s1);
    assert_eq!(decoded_builder_s2, decoded_sealed_s2);
}

#[test]
fn keyset_bit_packed_handles_large_cardinality() {
    let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();
    let pods = (0..300)
        .map(|i| format!("backend-{i:03}"))
        .collect::<Vec<_>>();
    let mut series = Vec::with_capacity(pods.len());

    for pod in &pods {
        let labels = [
            KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
            KeyValueRef::from(("cluster", "prod")),
            KeyValueRef::from(("pod", pod.as_str())),
        ];
        series.push(builder.intern(&labels).unwrap());
    }

    let first = series[0];
    let last = series[series.len() - 1];
    let decoded_first = decode(&builder, first);
    let decoded_last = decode(&builder, last);

    let sealed = builder.seal_bit_packed();
    let decoded_sealed_first = decode(&sealed, first);
    let decoded_sealed_last = decode(&sealed, last);

    assert_eq!(decoded_first, decoded_sealed_first);
    assert_eq!(decoded_last, decoded_sealed_last);
}

#[test]
fn store_intern_applies_normalization() {
    let mut store: FlatInternedLabelSetStore = FlatInternedLabelSetStore::default();

    let long_value = "a".repeat(crate::labels::MAX_LABEL_VALUE_BYTES + 123);
    let labels = [
        KeyValueRef::from(("__name__", "metric")),
        KeyValueRef::from(("foo", long_value.as_str())),
    ];

    let series = store.intern(&labels).unwrap();
    let decoded = decode(&store, series);
    let foo_value = decoded
        .iter()
        .find(|(k, _)| k == "foo")
        .map(|(_, v)| v.as_str())
        .expect("missing foo label");

    let expected = normalize_label_value(long_value.as_str());
    assert_eq!(foo_value, expected.as_ref());
    assert_eq!(foo_value.len(), crate::labels::MAX_LABEL_VALUE_BYTES);
    assert_eq!(store.len(), 1);
}

struct FailAfterSymbolTable {
    inner: DefaultSymbolTable,
    intern_calls: usize,
    fail_at_call: Option<usize>,
}

impl FailAfterSymbolTable {
    fn new(fail_at_call: usize) -> Self {
        Self {
            inner: DefaultSymbolTable::default(),
            intern_calls: 0,
            fail_at_call: Some(fail_at_call),
        }
    }
}

impl Default for FailAfterSymbolTable {
    fn default() -> Self {
        Self::new(usize::MAX)
    }
}

impl SymbolTable for FailAfterSymbolTable {
    fn len(&self) -> usize {
        self.inner.len()
    }

    fn lookup(&self, symbol: &str) -> Option<SymbolId> {
        self.inner.lookup(symbol)
    }

    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError> {
        self.intern_calls += 1;
        if self.fail_at_call == Some(self.intern_calls) {
            return Err(SymbolTableError::Arena(ArenaSymbolTableError::ArenaFull {
                offset: 0,
                len: 1,
                end: 1,
                max: 0,
            }));
        }
        self.inner.intern(symbol)
    }

    fn resolve(&self, id: SymbolId) -> &str {
        self.inner.resolve(id)
    }

    fn estimate_allocated_bytes(&self) -> usize {
        self.inner.estimate_allocated_bytes()
    }

    fn estimate_used_bytes(&self) -> usize {
        self.inner.estimate_used_bytes()
    }

    fn stats(&self) -> SymbolTableStats {
        self.inner.stats()
    }
}

struct PreparedStoreInterner<'a> {
    store: &'a mut FlatInternedLabelSetStore<FailAfterSymbolTable>,
}

impl OtlpLabelSetInterner for PreparedStoreInterner<'_> {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {}

    fn on_intern_error(&mut self, error: Self::Error) {
        panic!("unexpected prepared interning error: {error}");
    }

    fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
        self.store.intern_prepared_otlp(labels)
    }
}

struct RecoveringPreparedStoreInterner<'a> {
    store: &'a mut FlatInternedLabelSetStore<FailAfterSymbolTable>,
    errors: &'a mut Vec<LabelSetStoreError>,
}

impl OtlpLabelSetInterner for RecoveringPreparedStoreInterner<'_> {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {}

    fn on_intern_error(&mut self, error: Self::Error) {
        self.errors.push(error);
    }

    fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
        self.store.intern_prepared_otlp(labels)
    }
}

struct DefaultPreparedStoreInterner<'a> {
    store: &'a mut FlatInternedLabelSetStore,
}

impl OtlpLabelSetInterner for DefaultPreparedStoreInterner<'_> {
    type Error = LabelSetStoreError;

    fn on_skipped_non_scalar(&mut self) {}

    fn on_intern_error(&mut self, error: Self::Error) {
        panic!("unexpected prepared interning error: {error}");
    }

    fn intern(&mut self, labels: CanonicalLabelSet<'_, '_>) -> Result<SeriesRef, Self::Error> {
        self.store.intern_prepared_otlp(labels)
    }
}

fn otlp_string_attribute(key: &str, value: &str) -> KeyValue {
    KeyValue {
        key: key.to_string(),
        value: Some(OtlpAnyValue {
            value: Some(AnyValue::StringValue(value.to_string())),
        }),
        key_strindex: 0,
    }
}

#[test]
fn prepared_otlp_prefix_reuses_interned_resource_and_metric_symbols() {
    let resource_attributes = [
        otlp_string_attribute("cluster", "prod"),
        otlp_string_attribute("service", "checkout"),
    ];
    let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
    let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
    let metric = resource.metric("request.duration");
    let mut scratch = PreparedOtlpLabelSetScratch::default();
    let symbols = FailAfterSymbolTable::default();
    let mut store = FlatInternedLabelSetStore {
        symbols,
        ..FlatInternedLabelSetStore::default()
    };

    let first = intern_prepared_labelset(
        &mut PreparedStoreInterner { store: &mut store },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    );
    assert_eq!(first, Some(SeriesRef::new(0)));
    let first_calls = store.symbols.intern_calls;
    assert_eq!(first_calls, 8);

    let second = intern_prepared_labelset(
        &mut PreparedStoreInterner { store: &mut store },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    );
    assert_eq!(second, first);
    assert_eq!(store.symbols.intern_calls - first_calls, 2);
    assert_eq!(store.len(), 1);

    let mut second_store = FlatInternedLabelSetStore {
        symbols: FailAfterSymbolTable::default(),
        ..FlatInternedLabelSetStore::default()
    };
    let cross_store = intern_prepared_labelset(
        &mut PreparedStoreInterner {
            store: &mut second_store,
        },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    );
    assert_eq!(cross_store, Some(SeriesRef::new(0)));
    assert_eq!(second_store.symbols.intern_calls, 8);
    assert_eq!(
        decode(&second_store, SeriesRef::new(0)),
        decode(&store, SeriesRef::new(0))
    );
}

#[test]
fn interned_id_hash_deduplicates_legacy_and_prepared_paths_with_store_scoped_caches() {
    let resource_attributes = [
        otlp_string_attribute("cluster", "prod"),
        otlp_string_attribute("service", "checkout"),
    ];
    let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
    let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
    let metric = resource.metric("request.duration");
    let canonical = [
        KeyValueRef::from(("__name__", "request.duration")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("pod", "checkout-0")),
        KeyValueRef::from(("service", "checkout")),
    ];

    let mut legacy_first: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    let legacy_series = legacy_first.intern(&canonical).unwrap();
    let mut scratch = PreparedOtlpLabelSetScratch::default();
    let prepared_series = intern_prepared_labelset(
        &mut DefaultPreparedStoreInterner {
            store: &mut legacy_first,
        },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    );
    assert_eq!(prepared_series, Some(legacy_series));
    assert_eq!(legacy_first.len(), 1);

    let mut prepared_first: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    let prepared_series = intern_prepared_labelset(
        &mut DefaultPreparedStoreInterner {
            store: &mut prepared_first,
        },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    )
    .unwrap();
    assert_eq!(prepared_first.intern(&canonical).unwrap(), prepared_series);
    assert_eq!(prepared_first.len(), 1);
    assert_eq!(
        decode(&prepared_first, prepared_series),
        owned_labels(&canonical)
    );

    let mut preseeded: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    let unrelated = [
        KeyValueRef::from(("__name__", "unrelated")),
        KeyValueRef::from(("aaa", "bbb")),
    ];
    assert_eq!(preseeded.intern(&unrelated).unwrap(), SeriesRef::new(0));
    let preseeded_series = intern_prepared_labelset(
        &mut DefaultPreparedStoreInterner {
            store: &mut preseeded,
        },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    )
    .unwrap();
    assert_eq!(preseeded_series, SeriesRef::new(1));
    assert_eq!(
        decode(&preseeded, preseeded_series),
        owned_labels(&canonical)
    );
    assert_ne!(
        preseeded.series_slice(preseeded_series),
        prepared_first.series_slice(prepared_series),
        "preseeding must make the store-local SymbolIds differ"
    );
}

#[test]
fn interned_id_prepared_partial_cache_recovers_after_symbol_failure() {
    let resource_attributes = [
        otlp_string_attribute("cluster", "prod"),
        otlp_string_attribute("service", "checkout"),
    ];
    let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
    let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
    let metric = resource.metric("request.duration");
    let mut scratch = PreparedOtlpLabelSetScratch::default();
    let mut store: FlatInternedLabelSetStore<FailAfterSymbolTable> =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    store.symbols = FailAfterSymbolTable::new(4);
    let mut errors = Vec::new();

    let first = intern_prepared_labelset(
        &mut RecoveringPreparedStoreInterner {
            store: &mut store,
            errors: &mut errors,
        },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    );
    assert_eq!(first, None);
    assert_eq!(errors.len(), 1);
    assert!(matches!(
        errors[0],
        LabelSetStoreError::SymbolTable(SymbolTableError::Arena(
            ArenaSymbolTableError::ArenaFull { .. }
        ))
    ));
    assert_eq!(store.len(), 0);
    assert_eq!(store.buffer_stats().encoded_scratch_len, 0);
    assert_eq!(store.buffer_stats().fingerprint_calls, 0);
    let calls_after_failure = store.symbols.intern_calls;

    store.symbols.fail_at_call = None;
    let retry = intern_prepared_labelset(
        &mut RecoveringPreparedStoreInterner {
            store: &mut store,
            errors: &mut errors,
        },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    );
    assert_eq!(retry, Some(SeriesRef::new(0)));
    assert_eq!(errors.len(), 1);
    assert_eq!(store.symbols.intern_calls - calls_after_failure, 6);
    assert_eq!(store.buffer_stats().fingerprint_calls, 1);
    assert_eq!(
        decode(&store, SeriesRef::new(0)),
        [
            ("__name__".into(), "request.duration".into()),
            ("cluster".into(), "prod".into()),
            ("pod".into(), "checkout-0".into()),
            ("service".into(), "checkout".into()),
        ]
    );

    let repeat = intern_prepared_labelset(
        &mut RecoveringPreparedStoreInterner {
            store: &mut store,
            errors: &mut errors,
        },
        &metric,
        &datapoint_attributes,
        &mut scratch,
    );
    assert_eq!(repeat, retry);
    let stats = store.buffer_stats();
    assert_eq!(stats.fingerprint_calls, 2);
    assert_eq!(stats.equality_checks, 1);
    assert_eq!(stats.equality_matches, 1);
    assert_eq!(stats.equality_mismatches, 0);
}

#[test]
fn prepared_interned_id_paths_match_across_siphash_and_ahash() {
    let resource_attributes = [otlp_string_attribute("cluster", "prod")];
    let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
    let resource = PreparedOtlpResourceLabels::new(&resource_attributes);
    let metric = resource.metric("request.duration");
    let mut siphash_scratch = PreparedOtlpLabelSetScratch::default();
    let mut ahash_scratch = PreparedOtlpLabelSetScratch::default();
    let mut siphash: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_siphash_labelset_hash();
    let mut ahash: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();

    for expected_series in [SeriesRef::new(0), SeriesRef::new(0)] {
        let siphash_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner {
                store: &mut siphash,
            },
            &metric,
            &datapoint_attributes,
            &mut siphash_scratch,
        )
        .unwrap();
        let ahash_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner { store: &mut ahash },
            &metric,
            &datapoint_attributes,
            &mut ahash_scratch,
        )
        .unwrap();

        assert_eq!(siphash_series, expected_series);
        assert_eq!(ahash_series, expected_series);
        assert_eq!(
            decode(&siphash, siphash_series),
            decode(&ahash, ahash_series)
        );
    }

    assert_eq!(siphash.buffer_stats().labelset_hash, "interned_ids_siphash");
    assert_eq!(ahash.buffer_stats().labelset_hash, "interned_ids_ahash");
    for stats in [siphash.buffer_stats(), ahash.buffer_stats()] {
        assert_eq!(stats.fingerprint_calls, 2);
        assert_eq!(stats.equality_checks, 1);
        assert_eq!(stats.equality_matches, 1);
        assert_eq!(stats.equality_mismatches, 0);
        assert_eq!(stats.collision_inserts, 0);
    }
}

#[test]
fn interned_id_prepared_path_deduplicates_raw_and_normalized_overlength_labels() {
    let raw_key = format!("{}tail", "é".repeat(MAX_LABEL_NAME_BYTES));
    let raw_value = format!("{}tail", "界".repeat(MAX_LABEL_VALUE_BYTES));
    let normalized_key = normalize_label_key(&raw_key).into_owned();
    let normalized_value = normalize_label_value(&raw_value).into_owned();
    let raw_attributes = [otlp_string_attribute(&raw_key, &raw_value)];
    let normalized_attributes = [otlp_string_attribute(&normalized_key, &normalized_value)];
    let raw_resource = PreparedOtlpResourceLabels::new(&raw_attributes);
    let normalized_resource = PreparedOtlpResourceLabels::new(&normalized_attributes);
    let raw_metric = raw_resource.metric("overlength.metric");
    let normalized_metric = normalized_resource.metric("overlength.metric");
    let mut scratch = PreparedOtlpLabelSetScratch::default();
    let mut store: FlatInternedLabelSetStore =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();

    let raw_series = intern_prepared_labelset(
        &mut DefaultPreparedStoreInterner { store: &mut store },
        &raw_metric,
        &[],
        &mut scratch,
    )
    .unwrap();
    let normalized_series = intern_prepared_labelset(
        &mut DefaultPreparedStoreInterner { store: &mut store },
        &normalized_metric,
        &[],
        &mut scratch,
    )
    .unwrap();

    assert_eq!(normalized_series, raw_series);
    assert_eq!(store.len(), 1);
    assert_eq!(
        decode(&store, raw_series),
        [
            ("__name__".into(), "overlength.metric".into()),
            (normalized_key, normalized_value),
        ]
    );
}

#[test]
fn prepared_otlp_interning_is_equivalent_across_key_value_layouts() {
    let resource_attributes = [
        otlp_string_attribute("cluster", "prod"),
        otlp_string_attribute("service", "checkout"),
    ];
    let datapoint_attributes = [otlp_string_attribute("pod", "checkout-0")];
    let paged_resource = PreparedOtlpResourceLabels::new(&resource_attributes);
    let contiguous_resource = PreparedOtlpResourceLabels::new(&resource_attributes);
    let paged_metric = paged_resource.metric("request.duration");
    let contiguous_metric = contiguous_resource.metric("request.duration");
    let mut paged_scratch = PreparedOtlpLabelSetScratch::default();
    let mut contiguous_scratch = PreparedOtlpLabelSetScratch::default();
    let mut paged = FlatInternedLabelSetStore::with_key_value_page_capacity(3);
    let mut contiguous = FlatInternedLabelSetStore::with_contiguous_key_values();

    for _ in 0..2 {
        let paged_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner { store: &mut paged },
            &paged_metric,
            &datapoint_attributes,
            &mut paged_scratch,
        );
        let contiguous_series = intern_prepared_labelset(
            &mut DefaultPreparedStoreInterner {
                store: &mut contiguous,
            },
            &contiguous_metric,
            &datapoint_attributes,
            &mut contiguous_scratch,
        );

        assert_eq!(paged_series, Some(SeriesRef::new(0)));
        assert_eq!(contiguous_series, paged_series);
    }

    assert_eq!(
        decode(&paged, SeriesRef::new(0)),
        decode(&contiguous, SeriesRef::new(0))
    );
    assert_eq!(paged.len(), contiguous.len());
    assert_eq!(paged.symbols().len(), contiguous.symbols().len());
}

#[test]
fn interned_encode_error_clears_scratch_and_allows_retry() {
    let symbols = FailAfterSymbolTable::new(4);
    let mut store: FlatInternedLabelSetStore<FailAfterSymbolTable> =
        FlatInternedLabelSetStore::with_interned_id_labelset_hash();
    store.symbols = symbols;
    let labels = [
        KeyValueRef::from(("__name__", "metric")),
        KeyValueRef::from(("foo", "bar")),
    ];

    let error = store.intern(&labels).unwrap_err();
    assert!(matches!(
        error,
        LabelSetStoreError::SymbolTable(SymbolTableError::Arena(
            ArenaSymbolTableError::ArenaFull { .. }
        ))
    ));
    assert_eq!(store.len(), 0);
    assert_eq!(store.buffer_stats().encoded_scratch_len, 0);
    assert!(store.buffer_stats().encoded_scratch_cap >= labels.len());

    store.symbols.fail_at_call = None;
    let series = store.intern(&labels).unwrap();
    assert_eq!(series, SeriesRef::new(0));
    assert_eq!(
        decode(&store, series),
        [
            ("__name__".into(), "metric".into()),
            ("foo".into(), "bar".into())
        ]
    );
    assert_eq!(store.buffer_stats().encoded_scratch_len, 0);
}

#[test]
fn labelset_store_propagates_symbol_table_errors() {
    #[derive(Default)]
    struct FailingSymbolTable;

    impl SymbolTable for FailingSymbolTable {
        fn len(&self) -> usize {
            0
        }

        fn lookup(&self, _symbol: &str) -> Option<SymbolId> {
            None
        }

        fn intern(&mut self, _symbol: &str) -> Result<SymbolId, SymbolTableError> {
            Err(SymbolTableError::Arena(ArenaSymbolTableError::ArenaFull {
                offset: 0,
                len: 1,
                end: 1,
                max: 0,
            }))
        }

        fn resolve(&self, _id: SymbolId) -> &str {
            ""
        }

        fn estimate_allocated_bytes(&self) -> usize {
            0
        }

        fn estimate_used_bytes(&self) -> usize {
            0
        }

        fn stats(&self) -> SymbolTableStats {
            SymbolTableStats::Arc {
                symbols: 0,
                symbol_to_id_len: 0,
                symbol_to_id_cap: 0,
                id_to_symbol_len: 0,
                id_to_symbol_cap: 0,
            }
        }
    }

    let mut store: FlatInternedLabelSetStore<FailingSymbolTable> =
        FlatInternedLabelSetStore::default();

    let labels = [
        KeyValueRef::from(("__name__", "metric")),
        KeyValueRef::from(("foo", "bar")),
    ];

    let err = store.intern(&labels).unwrap_err();
    assert!(matches!(
        err,
        LabelSetStoreError::SymbolTable(SymbolTableError::Arena(
            ArenaSymbolTableError::ArenaFull { .. }
        ))
    ));
}

#[test]
fn keyset_bit_packed_comprehensive_widths() {
    let mut builder: KeySetDictEncodedLabelSetStore = KeySetDictEncodedLabelSetStore::default();

    // Define widths to test covering various bit boundaries:
    // 0 bits (1 value)
    // 1 bit (2 values)
    // 2 bits (4 values)
    // 3 bits (8 values)
    // 4 bits (nibble)
    // 7 bits (almost byte)
    // 8 bits (byte)
    // 9 bits (byte + 1)
    // 10 bits (crossing)
    let widths = [0, 1, 2, 3, 4, 7, 8, 9, 10];

    let mut keys_info = Vec::new();
    for (i, &w) in widths.iter().enumerate() {
        // Prefix with "k_" and index to ensure uniqueness and order
        let key = format!("k_{:02}_{}", i, w);
        // Needed count to force at least one value to require `w` bits:
        // Max code must be >= 2^(w-1). So we need 2^(w-1) + 1 values (0..2^(w-1)).
        // Exception: width 0 -> 1 value.
        let needed_unique_count = if w == 0 { 1 } else { (1 << (w - 1)) + 1 };
        keys_info.push((key, needed_unique_count));
    }

    let max_count = keys_info.iter().map(|(_, c)| *c).max().unwrap();

    let mut series_refs = Vec::new();

    for j in 0..max_count {
        let mut label_pairs = Vec::new();

        // Always have a name
        label_pairs.push(("__name__".to_string(), "test".to_string()));

        for (key, count) in &keys_info {
            // Use modulo to keep cycling through values, ensuring we use high codes
            // in later series as well, mixing them up.
            // However, `intern` creates code based on insertion order.
            // Value "v_0" gets code 0, "v_1" gets code 1.
            // So as long as we eventually see "v_N", we create code N.
            let val_idx = if j < *count { j } else { j % count };
            let val = format!("v_{}", val_idx);
            label_pairs.push((key.clone(), val));
        }

        // Sort by key to satisfy labelset canonical requirement
        label_pairs.sort_by(|a, b| a.0.cmp(&b.0));

        let label_refs: Vec<KeyValueRef> = label_pairs
            .iter()
            .map(|(k, v)| KeyValueRef::from((k.as_str(), v.as_str())))
            .collect();

        let s = builder.intern(&label_refs).unwrap();
        series_refs.push(s);
    }

    let sealed = builder.seal_bit_packed();
    assert_eq!(sealed.len(), series_refs.len());

    for (i, &s) in series_refs.iter().enumerate() {
        let decoded_orig = decode(&builder, s);
        let decoded_sealed = decode(&sealed, s);
        assert_eq!(
            decoded_orig, decoded_sealed,
            "Mismatch at series index {} (series ref {:?})",
            i, s
        );
    }
}
