use chronoxide_core::labels::{
    BitPackedKeySetLabelSetStore, DefaultSymbolTable, FixedWidthPackedKeySetLabelSetStore,
    FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef, LabelSetStore,
    NaiveLabelSetStore,
};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};

struct Pools {
    namespaces: Vec<String>,
    pods: Vec<String>,
}

fn build_pools(series_count: usize, namespace_count: usize) -> Pools {
    let namespaces = (0..namespace_count)
        .map(|i| format!("ns{i:04}"))
        .collect::<Vec<_>>();
    let pods = (0..series_count)
        .map(|i| format!("backend-{i:06}"))
        .collect::<Vec<_>>();
    Pools { namespaces, pods }
}

fn labelset_for(pools: &Pools, series_index: usize) -> [KeyValueRef<'_>; 5] {
    let namespace = pools.namespaces[series_index % pools.namespaces.len()].as_str();
    let pod = pools.pods[series_index].as_str();
    let container = if series_index.is_multiple_of(2) {
        "web"
    } else {
        "sidecar"
    };
    [
        KeyValueRef::from(("__name__", "pod_cpu_usage_seconds_total")),
        KeyValueRef::from(("cluster", "prod")),
        KeyValueRef::from(("container", container)),
        KeyValueRef::from(("namespace", namespace)),
        KeyValueRef::from(("pod", pod)),
    ]
}

fn bench_intern_unique<S: LabelSetStore + Default>(
    b: &mut criterion::Bencher<'_>,
    pools: &Pools,
    series_count: usize,
) {
    b.iter_batched(
        S::default,
        |mut store| {
            for series_index in 0..series_count {
                let labels = labelset_for(pools, series_index);
                store.intern(&labels).unwrap();
            }
            std::hint::black_box(store.len());
        },
        BatchSize::LargeInput,
    );
}

fn build_store<S: LabelSetStore + Default>(pools: &Pools, series_count: usize) -> S {
    let mut store = S::default();
    for series_index in 0..series_count {
        let labels = labelset_for(pools, series_index);
        store.intern(&labels).unwrap();
    }
    store
}

fn bench_visit<S: LabelSetStore>(b: &mut criterion::Bencher<'_>, store: &S, series_count: usize) {
    b.iter(|| {
        for series_index in 0..series_count {
            let series_ref = chronoxide_core::labels::SeriesRef::new(series_index as u32);
            store.visit_labelset(series_ref, |key, value| {
                std::hint::black_box((key, value));
            });
        }
    });
}

fn labelset_store_benches(c: &mut Criterion) {
    let series_count = 50_000usize;
    let pools = build_pools(series_count, 100);

    let mut group = c.benchmark_group("labelset_intern_unique");
    group.bench_function("NaiveLabelSetStore", |b| {
        bench_intern_unique::<NaiveLabelSetStore>(b, &pools, series_count)
    });
    group.bench_function("FlatInternedLabelSetStore", |b| {
        bench_intern_unique::<FlatInternedLabelSetStore>(b, &pools, series_count)
    });
    group.bench_function("KeySetDictEncodedLabelSetStore", |b| {
        bench_intern_unique::<KeySetDictEncodedLabelSetStore>(b, &pools, series_count)
    });
    group.finish();

    let repeated_keys = (0..23)
        .map(|index| format!("label_{index:02}"))
        .collect::<Vec<_>>();
    let repeated_values = (0..23)
        .map(|index| format!("value_{index:02}"))
        .collect::<Vec<_>>();
    let mut repeated_labels = vec![KeyValueRef::from(("__name__", "metric"))];
    repeated_labels.extend(
        repeated_keys
            .iter()
            .zip(&repeated_values)
            .map(|(key, value)| KeyValueRef::from((key.as_str(), value.as_str()))),
    );
    let mut repeated_store = FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    let repeated_series = repeated_store.intern(&repeated_labels).unwrap();
    assert_eq!(
        repeated_store.intern(&repeated_labels).unwrap(),
        repeated_series
    );

    c.bench_function(
        "labelset_intern_repeated_hit_24_labels/ahash_symbol_ids_default",
        |b| {
            b.iter(|| {
                let series = repeated_store
                    .intern(std::hint::black_box(repeated_labels.as_slice()))
                    .unwrap();
                std::hint::black_box(series);
            });
        },
    );

    let mut repeated_siphash_store =
        FlatInternedLabelSetStore::<DefaultSymbolTable>::with_interned_id_siphash_labelset_hash();
    let repeated_siphash_series = repeated_siphash_store.intern(&repeated_labels).unwrap();
    assert_eq!(
        repeated_siphash_store.intern(&repeated_labels).unwrap(),
        repeated_siphash_series
    );
    c.bench_function(
        "labelset_intern_repeated_hit_24_labels/siphash_symbol_ids_control",
        |b| {
            b.iter(|| {
                let series = repeated_siphash_store
                    .intern(std::hint::black_box(repeated_labels.as_slice()))
                    .unwrap();
                std::hint::black_box(series);
            });
        },
    );

    let mut repeated_canonical_hash_store =
        FlatInternedLabelSetStore::<DefaultSymbolTable>::with_canonical_string_labelset_hash();
    let repeated_canonical_hash_series = repeated_canonical_hash_store
        .intern(&repeated_labels)
        .unwrap();
    assert_eq!(
        repeated_canonical_hash_store
            .intern(&repeated_labels)
            .unwrap(),
        repeated_canonical_hash_series
    );
    c.bench_function(
        "labelset_intern_repeated_hit_24_labels/canonical_string_hash_control",
        |b| {
            b.iter(|| {
                let series = repeated_canonical_hash_store
                    .intern(std::hint::black_box(repeated_labels.as_slice()))
                    .unwrap();
                std::hint::black_box(series);
            });
        },
    );

    let naive_store: NaiveLabelSetStore = build_store(&pools, series_count);

    let flat_store: FlatInternedLabelSetStore<DefaultSymbolTable> =
        build_store(&pools, series_count);

    let key_set_dict_store: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
        build_store(&pools, series_count);

    let packed_key_set_store: FixedWidthPackedKeySetLabelSetStore<DefaultSymbolTable> = {
        let builder: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
            build_store(&pools, series_count);
        builder.seal_fixed_width()
    };

    let bit_packed_key_set_store: BitPackedKeySetLabelSetStore<DefaultSymbolTable> = {
        let builder: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
            build_store(&pools, series_count);
        builder.seal_bit_packed()
    };

    let mut group = c.benchmark_group("labelset_visit_50k");
    group.bench_function("NaiveLabelSetStore", |b| {
        bench_visit(b, &naive_store, series_count);
    });

    group.bench_function("FlatInternedLabelSetStore", |b| {
        bench_visit(b, &flat_store, series_count);
    });

    group.bench_function("KeySetDictEncodedLabelSetStore", |b| {
        bench_visit(b, &key_set_dict_store, series_count);
    });

    group.bench_function("FixedWidthPackedKeySetLabelSetStore", |b| {
        bench_visit(b, &packed_key_set_store, series_count);
    });

    group.bench_function("BitPackedKeySetLabelSetStore", |b| {
        bench_visit(b, &bit_packed_key_set_store, series_count);
    });
    group.finish();
}

criterion_group!(benches, labelset_store_benches);
criterion_main!(benches);
