use chronoxide_core::labels::{
    DefaultSymbolTable, FlatInternedLabelSetStore, KeySetDictEncodedLabelSetStore, KeyValueRef,
    LabelSetStore, NaiveLabelSetStore,
};
use std::time::Instant;

use chronoxide_core::alloc_tracking::{
    TrackingAllocator, allocation_stats, reset_allocation_counters,
};

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

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

fn run_intern_benchmark(
    name: &'static str,
    pools: &Pools,
    series_count: usize,
    mut store: impl LabelSetStore,
) {
    reset_allocation_counters();
    let start = Instant::now();
    for series_index in 0..series_count {
        let labels = labelset_for(pools, series_index);
        store.intern(&labels).unwrap();
    }
    let elapsed = start.elapsed();
    let stats = allocation_stats();

    println!(
        "{name}: series={series_count} time={:?} req_total={}B req_current={}B usable_total={}B usable_current={}B internal_frag={}B ({:.2}%), alloc_calls={} dealloc_calls={} realloc_calls={}, estimate_alloc_bytes={}B estimate_used_bytes={}B",
        elapsed,
        stats.requested_total,
        stats.requested_current,
        stats.usable_total,
        stats.usable_current,
        stats.internal_frag_bytes,
        stats.internal_frag_percent,
        stats.alloc_calls,
        stats.dealloc_calls,
        stats.realloc_calls,
        store.estimate_size_bytes(),
        store.estimate_used_bytes(),
    );

    std::hint::black_box(store.len());
}

fn main() {
    let series_count = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(100_000);
    let pools = build_pools(series_count, 100);

    run_intern_benchmark(
        "NaiveLabelSetStore",
        &pools,
        series_count,
        NaiveLabelSetStore::<DefaultSymbolTable>::default(),
    );

    run_intern_benchmark(
        "FlatInternedLabelSetStore",
        &pools,
        series_count,
        FlatInternedLabelSetStore::<DefaultSymbolTable>::default(),
    );
    run_intern_benchmark(
        "KeySetDictEncodedLabelSetStore",
        &pools,
        series_count,
        KeySetDictEncodedLabelSetStore::<DefaultSymbolTable>::default(),
    );

    let mut builder: KeySetDictEncodedLabelSetStore<DefaultSymbolTable> =
        KeySetDictEncodedLabelSetStore::<DefaultSymbolTable>::default();
    for series_index in 0..series_count {
        let labels = labelset_for(&pools, series_index);
        builder.intern(&labels).unwrap();
    }
    let sealed = builder.seal_fixed_width();
    reset_allocation_counters();
    let start = Instant::now();
    for series_index in 0..series_count {
        let series_ref = chronoxide_core::labels::SeriesRef::new(series_index as u32);
        sealed.visit_labelset(series_ref, |_key, _value| {});
    }
    let elapsed = start.elapsed();
    let stats = allocation_stats();
    println!(
        "PackedKeySetLabelSetStore: visit series={series_count} time={:?} req_total={}B req_current={}B usable_total={}B usable_current={}B internal_frag={}B ({:.2}%), alloc_calls={} dealloc_calls={} realloc_calls={} estimate_alloc_bytes={} estimate_used_bytes={}",
        elapsed,
        stats.requested_total,
        stats.requested_current,
        stats.usable_total,
        stats.usable_current,
        stats.internal_frag_bytes,
        stats.internal_frag_percent,
        stats.alloc_calls,
        stats.dealloc_calls,
        stats.realloc_calls,
        sealed.estimate_size_bytes(),
        sealed.estimate_used_bytes(),
    );
}
