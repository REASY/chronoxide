use chronoxide_core::statistics::{
    DEFAULT_TDIGEST_BUFFER_CAPACITY, DEFAULT_TDIGEST_MAX_CENTROIDS, Stats,
};
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;

fn build_values(count: usize) -> Vec<u32> {
    let mut rng = SmallRng::seed_from_u64(0x243f_6a88_85a3_08d3);
    (0..count).map(|_| rng.random()).collect()
}

fn stats_u32_benches(c: &mut Criterion) {
    let insert_count = 300_000usize;
    let values = build_values(insert_count);

    let mut group = c.benchmark_group("stats_u32");
    // Large input size + multiple error configs; keep this benchmark reasonably fast by default.
    group.sample_size(10);
    for max_centroids in [DEFAULT_TDIGEST_MAX_CENTROIDS, 500, 1_000] {
        let buffer_capacity = DEFAULT_TDIGEST_BUFFER_CAPACITY;
        let label = format!("centroids={max_centroids}_buf={buffer_capacity}");
        group.bench_with_input(
            BenchmarkId::new("insert", &label),
            &(max_centroids, buffer_capacity),
            |b, &(max_centroids, buffer_capacity)| {
                b.iter_batched(
                    || Stats::<u32>::new_tdigest(max_centroids, buffer_capacity),
                    |mut stats| {
                        for &value in &values {
                            stats.insert(value);
                        }
                        std::hint::black_box(stats.count());
                    },
                    BatchSize::LargeInput,
                );
            },
        );

        group.bench_with_input(
            BenchmarkId::new("insert_summarize", &label),
            &(max_centroids, buffer_capacity),
            |b, &(max_centroids, buffer_capacity)| {
                b.iter_batched(
                    || Stats::<u32>::new_tdigest(max_centroids, buffer_capacity),
                    |mut stats| {
                        for &value in &values {
                            stats.insert(value);
                        }
                        std::hint::black_box(stats.summarize());
                    },
                    BatchSize::LargeInput,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, stats_u32_benches);
criterion_main!(benches);
