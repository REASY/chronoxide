use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, NaiveLabelSetStore,
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
    let container = if series_index % 2 == 0 {
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

    let mut naive_store: NaiveLabelSetStore = NaiveLabelSetStore::default();
    let mut key_set_store: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
        KeySetDictEncodedLabelSetStore::<DefaultSymbolTable>::default();
    let mut interned_store: FlatInternedLabelSetStore<DefaultSymbolTable> =
        FlatInternedLabelSetStore::<DefaultSymbolTable>::default();
    for series_index in 0..series_count {
        let labels = labelset_for(&pools, series_index);
        naive_store.intern(&labels).unwrap();
        key_set_store.intern(&labels).unwrap();

        interned_store.intern(&labels).unwrap();
    }

    let sealed = {
        let mut builder_for_seal: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
            KeySetDictEncodedLabelSetStore::<DefaultSymbolTable>::default();
        for series_index in 0..series_count {
            let labels = labelset_for(&pools, series_index);
            builder_for_seal.intern(&labels).unwrap();
        }
        builder_for_seal.seal_fixed_width()
    };

    let mut group = c.benchmark_group("labelset_visit_50k");
    group.bench_function("NaiveLabelSetStore", |b| {
        bench_visit(b, &naive_store, series_count)
    });
    group.bench_function("FlatInternedLabelSetStore", |b| {
        bench_visit(b, &interned_store, series_count)
    });

    group.bench_function("KeySetDictEncodedLabelSetStore", |b| {
        bench_visit(b, &key_set_store, series_count)
    });
    group.bench_function("PackedKeySetLabelSetStore", |b| {
        bench_visit(b, &sealed, series_count)
    });
    group.finish();
}

criterion_group!(benches, labelset_store_benches);
criterion_main!(benches);
