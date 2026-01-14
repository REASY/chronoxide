use chronoxide_core::labels::{
    ArcSymbolTable, ArenaSymbolTablePacked, ArenaSymbolTableUnpacked, GermanSymbolTable,
    PackedSymbolLoc, SmolStrSymbolTable, SymbolId, SymbolTable, UnpackedSymbolLoc,
};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use std::hash::{DefaultHasher, Hasher};

const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_-";

fn fill_random_ascii(out: &mut String, target_len: usize, rng: &mut impl Rng) {
    while out.len() < target_len {
        let b = ALPHABET[rng.random_range(0..ALPHABET.len())];
        out.push(b as char);
    }
    out.truncate(target_len);
}

fn choose_key_len(rng: &mut impl Rng) -> usize {
    // Based on observed stats:
    // - avg key len ~14
    // - key max up to ~67
    let p: u32 = rng.random_range(0..1000);
    if p < 750 {
        rng.random_range(10usize..=18usize)
    } else if p < 970 {
        rng.random_range(19usize..=30usize)
    } else {
        rng.random_range(31usize..=67usize)
    }
}

fn choose_value_len(rng: &mut impl Rng) -> usize {
    // Based on observed stats:
    // - avg value len ~15
    // - long tail with max ~680
    let p: u32 = rng.random_range(0..1000);
    if p < 820 {
        rng.random_range(5usize..=25usize)
    } else if p < 960 {
        rng.random_range(26usize..=45usize)
    } else if p < 990 {
        rng.random_range(46usize..=150usize)
    } else {
        rng.random_range(151usize..=680usize)
    }
}

fn build_keys(count: usize, rng: &mut impl Rng) -> Vec<String> {
    let mut out = Vec::with_capacity(count + 1);
    out.push("__name__".to_string());
    for i in 0..count {
        let mut s = format!("k{:x}_", i);
        let len = choose_key_len(rng).max(s.len());
        fill_random_ascii(&mut s, len, rng);
        out.push(s);
    }
    out
}

fn build_values(prefix: &str, count: usize, rng: &mut impl Rng) -> Vec<String> {
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        let mut s = format!("{prefix}{:x}-", i);
        let len = choose_value_len(rng).max(s.len());
        fill_random_ascii(&mut s, len, rng);
        out.push(s);
    }
    out
}

fn build_intern_ops<'a>(
    pairs: usize,
    rng: &mut impl Rng,
    keys: &'a [String],
    common_values: &'a [String],
    rare_values: &'a [String],
) -> Vec<&'a str> {
    // Model "intern label pairs": roughly 50/50 keys/values.
    // Values are a mix of common (repeat) and rare (high-cardinality/mostly-new).
    let mut ops = Vec::with_capacity(pairs * 2);
    let mut rare_index = 0usize;
    for _ in 0..pairs {
        let key = &keys[rng.random_range(0..keys.len())];
        ops.push(key.as_str());

        let p: u32 = rng.random_range(0..100);
        let value = if p < 75 {
            &common_values[rng.random_range(0..common_values.len())]
        } else {
            let v = &rare_values[rare_index % rare_values.len()];
            rare_index = rare_index.wrapping_add(1);
            v
        };
        ops.push(value.as_str());
    }
    ops
}

fn bench_intern<S: SymbolTable + Default>(b: &mut criterion::Bencher<'_>, ops: &[&str]) {
    b.iter_batched(
        S::default,
        |mut table| {
            for &s in ops {
                std::hint::black_box(table.intern(s).unwrap());
            }
            std::hint::black_box(table.len());
            // Return the table to exclude the drop cost!
            table
        },
        BatchSize::LargeInput,
    );
}

fn bench_lookup<S: SymbolTable>(b: &mut criterion::Bencher<'_>, table: &S, queries: &[&str]) {
    b.iter(|| {
        for &q in queries {
            std::hint::black_box(table.lookup(q));
        }
    });
}

fn bench_resolve<S: SymbolTable>(b: &mut criterion::Bencher<'_>, table: &S, ids: &[SymbolId]) {
    b.iter(|| {
        for &id in ids {
            let str = table.resolve(id);
            let hash = {
                // Touch the string as it would be in real workloads
                let mut hasher = DefaultHasher::new();
                hasher.write(str.as_bytes());
                hasher.finish()
            };
            std::hint::black_box(hash);
        }
    });
}

fn symbol_table_benches(c: &mut Criterion) {
    // Keep counts moderate; intern is expensive and Criterion will run multiple samples.
    const UNIQUE_KEYS: usize = 512;
    const COMMON_VALUES: usize = 25_000;
    const RARE_VALUES: usize = 75_000;
    const INTERN_PAIRS: usize = 150_000; // 300k intern ops (key+value)
    const LOOKUP_QUERIES: usize = 200_000;

    let mut rng = SmallRng::seed_from_u64(0x243f_6a88_85a3_08d3);
    let keys = build_keys(UNIQUE_KEYS, &mut rng);
    let common_values = build_values("c", COMMON_VALUES, &mut rng);
    let rare_values = build_values("r", RARE_VALUES, &mut rng);
    let miss_values = build_values("m", LOOKUP_QUERIES, &mut rng);

    let mut unique = Vec::with_capacity(keys.len() + common_values.len() + rare_values.len());
    unique.extend(keys.iter().map(|s| s.as_str()));
    unique.extend(common_values.iter().map(|s| s.as_str()));
    unique.extend(rare_values.iter().map(|s| s.as_str()));

    let ops = build_intern_ops(INTERN_PAIRS, &mut rng, &keys, &common_values, &rare_values);

    let hit_queries: Vec<&str> = ops.iter().copied().take(LOOKUP_QUERIES).collect();
    let miss_queries: Vec<&str> = miss_values.iter().map(|s| s.as_str()).collect();

    let mut group = c.benchmark_group("symbol_table_intern");
    group.bench_function("ArenaSymbolTablePacked/mixed", |b| {
        bench_intern::<ArenaSymbolTablePacked>(b, &ops)
    });
    group.bench_function("ArenaSymbolTableUnpacked/mixed", |b| {
        bench_intern::<ArenaSymbolTableUnpacked>(b, &ops)
    });
    group.bench_function("GermanSymbolTable/mixed", |b| {
        bench_intern::<GermanSymbolTable>(b, &ops)
    });
    group.bench_function("SmolStrSymbolTable/mixed", |b| {
        bench_intern::<SmolStrSymbolTable>(b, &ops)
    });
    group.bench_function("ArcSymbolTable/mixed", |b| {
        bench_intern::<ArcSymbolTable>(b, &ops)
    });
    group.bench_function("ArenaSymbolTablePacked/unique", |b| {
        bench_intern::<ArenaSymbolTablePacked>(b, &unique)
    });
    group.bench_function("ArenaSymbolTableUnpacked/unique", |b| {
        bench_intern::<ArenaSymbolTableUnpacked>(b, &unique)
    });
    group.bench_function("GermanSymbolTable/unique", |b| {
        bench_intern::<GermanSymbolTable>(b, &unique)
    });
    group.bench_function("SmolStrSymbolTable/unique", |b| {
        bench_intern::<SmolStrSymbolTable>(b, &unique)
    });
    group.bench_function("ArcSymbolTable/unique", |b| {
        bench_intern::<ArcSymbolTable>(b, &unique)
    });
    group.finish();

    // Pre-build tables once for lookup/resolve benches (not included in timings).
    // Do this after the `intern` benches so those runs are less affected by prior heap state.
    let mut arena_packed = ArenaSymbolTablePacked::default();
    let mut arena_packed_ids = Vec::with_capacity(unique.len());
    for &s in &unique {
        arena_packed_ids.push(arena_packed.intern(s).unwrap());
    }

    let mut arena_unpacked = ArenaSymbolTableUnpacked::default();
    let mut arena_unpacked_ids = Vec::with_capacity(unique.len());
    for &s in &unique {
        arena_unpacked_ids.push(arena_unpacked.intern(s).unwrap());
    }

    let mut arc = ArcSymbolTable::default();
    let mut arc_ids = Vec::with_capacity(unique.len());
    for &s in &unique {
        arc_ids.push(arc.intern(s).unwrap());
    }

    let mut german = GermanSymbolTable::default();
    let mut german_ids = Vec::with_capacity(unique.len());
    for &s in &unique {
        german_ids.push(german.intern(s).unwrap());
    }

    let mut smol = SmolStrSymbolTable::default();
    let mut smol_ids = Vec::with_capacity(unique.len());
    for &s in &unique {
        smol_ids.push(smol.intern(s).unwrap());
    }

    let mut group = c.benchmark_group("symbol_table_lookup_hit");
    group.bench_function("ArenaSymbolTablePacked", |b| {
        bench_lookup(b, &arena_packed, &hit_queries)
    });
    group.bench_function("ArenaSymbolTableUnpacked", |b| {
        bench_lookup(b, &arena_unpacked, &hit_queries)
    });
    group.bench_function("GermanSymbolTable", |b| {
        bench_lookup(b, &german, &hit_queries)
    });
    group.bench_function("SmolStrSymbolTable", |b| {
        bench_lookup(b, &smol, &hit_queries)
    });
    group.bench_function("ArcSymbolTable", |b| bench_lookup(b, &arc, &hit_queries));
    group.finish();

    let mut group = c.benchmark_group("symbol_table_lookup_miss");
    group.bench_function("ArenaSymbolTablePacked", |b| {
        bench_lookup(b, &arena_packed, &miss_queries)
    });
    group.bench_function("ArenaSymbolTableUnpacked", |b| {
        bench_lookup(b, &arena_unpacked, &miss_queries)
    });
    group.bench_function("GermanSymbolTable", |b| {
        bench_lookup(b, &german, &miss_queries)
    });
    group.bench_function("SmolStrSymbolTable", |b| {
        bench_lookup(b, &smol, &miss_queries)
    });
    group.bench_function("ArcSymbolTable", |b| bench_lookup(b, &arc, &miss_queries));
    group.finish();

    let mut group = c.benchmark_group("symbol_table_resolve");
    group.bench_function("ArenaSymbolTablePacked", |b| {
        bench_resolve(b, &arena_packed, &arena_packed_ids)
    });
    group.bench_function("ArenaSymbolTableUnpacked", |b| {
        bench_resolve(b, &arena_unpacked, &arena_unpacked_ids)
    });
    group.bench_function("GermanSymbolTable", |b| {
        bench_resolve(b, &german, &german_ids)
    });
    group.bench_function("SmolStrSymbolTable", |b| bench_resolve(b, &smol, &smol_ids));
    group.bench_function("ArcSymbolTable", |b| bench_resolve(b, &arc, &arc_ids));
    group.finish();

    // Print best-effort size estimates after interning the full unique set.
    // This is outside the timed regions and helps compare memory footprints.
    println!(
        "symbol_table_symbol_loc packed(size={}, align={}) unpacked(size={}, align={})",
        std::mem::size_of::<PackedSymbolLoc>(),
        std::mem::align_of::<PackedSymbolLoc>(),
        std::mem::size_of::<UnpackedSymbolLoc>(),
        std::mem::align_of::<UnpackedSymbolLoc>(),
    );
    println!(
        "symbol_table_dataset unique_keys={} common_values={} rare_values={} unique_total={} intern_ops={} hit_queries={} miss_queries={}",
        keys.len(),
        common_values.len(),
        rare_values.len(),
        unique.len(),
        ops.len(),
        hit_queries.len(),
        miss_queries.len()
    );
    println!(
        "symbol_table_estimate kind=ArenaSymbolTablePacked symbols={} alloc_bytes={} used_bytes={}",
        arena_packed.len(),
        arena_packed.estimate_allocated_bytes(),
        arena_packed.estimate_used_bytes()
    );
    println!(
        "symbol_table_estimate kind=ArenaSymbolTableUnpacked symbols={} alloc_bytes={} used_bytes={}",
        arena_unpacked.len(),
        arena_unpacked.estimate_allocated_bytes(),
        arena_unpacked.estimate_used_bytes()
    );
    println!(
        "symbol_table_estimate kind=GermanSymbolTable symbols={} alloc_bytes={} used_bytes={}",
        german.len(),
        german.estimate_allocated_bytes(),
        german.estimate_used_bytes()
    );
    println!(
        "symbol_table_estimate kind=SmolStrSymbolTable symbols={} alloc_bytes={} used_bytes={}",
        smol.len(),
        smol.estimate_allocated_bytes(),
        smol.estimate_used_bytes()
    );
    println!(
        "symbol_table_estimate kind=ArcSymbolTable symbols={} alloc_bytes={} used_bytes={}",
        arc.len(),
        arc.estimate_allocated_bytes(),
        arc.estimate_used_bytes()
    );
}

criterion_group!(benches, symbol_table_benches);
criterion_main!(benches);
