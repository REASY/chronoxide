use chronoxide_core::alloc_tracking::{
    TrackingAllocator, allocation_stats, reset_allocation_counters,
};
use chronoxide_core::labels::{
    ArcSymbolTable, ArenaSymbolTablePacked, ArenaSymbolTableUnpacked, GermanSymbolTable,
    LassoSymbolTable, SmolStrSymbolTable, SymbolTable,
};
use rand::rngs::SmallRng;
use rand::{Rng, RngExt, SeedableRng};
use std::time::Instant;

#[global_allocator]
static GLOBAL: TrackingAllocator = TrackingAllocator;

const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyz0123456789_-";

fn fill_random_ascii(out: &mut String, target_len: usize, rng: &mut impl Rng) {
    while out.len() < target_len {
        let b = ALPHABET[rng.random_range(0..ALPHABET.len())];
        out.push(b as char);
    }
    out.truncate(target_len);
}

fn choose_key_len(rng: &mut impl Rng) -> usize {
    // From observed PROD stats (approx):
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
    // From observed PROD stats (approx):
    // - avg value len ~15-16
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

fn run_intern_unique(name: &str, symbols: &[String], mut table: impl SymbolTable) {
    reset_allocation_counters();
    let start = Instant::now();
    for s in symbols {
        table.intern(s).unwrap();
    }
    let elapsed = start.elapsed();
    let stats = allocation_stats();

    let estimate_alloc = table.estimate_allocated_bytes();
    let estimate_used = table.estimate_used_bytes();
    let estimate_slack = estimate_alloc.saturating_sub(estimate_used);

    println!(
        "{name}: symbols={} time={:?} req_current={}B usable_current={}B internal_frag={}B ({:.2}%) alloc_calls={} realloc_calls={} estimate_alloc_bytes={}B estimate_used_bytes={}B estimate_slack_bytes={}B",
        table.len(),
        elapsed,
        stats.requested_current,
        stats.usable_current,
        stats.internal_frag_bytes,
        stats.internal_frag_percent,
        stats.alloc_calls,
        stats.realloc_calls,
        estimate_alloc,
        estimate_used,
        estimate_slack,
    );

    std::hint::black_box(table.len());
}

fn main() {
    // Defaults mirror the `symbol_table` bench dataset.
    let unique_keys = std::env::args()
        .nth(1)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(512);
    let common_values = std::env::args()
        .nth(2)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(25_000);
    let rare_values = std::env::args()
        .nth(3)
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(75_000);

    let mut rng = SmallRng::seed_from_u64(0x243f_6a88_85a3_08d3);

    // Build the input strings first (not counted in allocator stats).
    let keys = build_keys(unique_keys, &mut rng);
    let common = build_values("c", common_values, &mut rng);
    let rare = build_values("r", rare_values, &mut rng);

    let mut symbols = Vec::with_capacity(keys.len() + common.len() + rare.len());
    symbols.extend(keys);
    symbols.extend(common);
    symbols.extend(rare);

    println!(
        "dataset: unique_total_symbols={} (keys={}, common_values={}, rare_values={})",
        symbols.len(),
        unique_keys + 1,
        common_values,
        rare_values
    );

    run_intern_unique(
        "ArenaSymbolTablePacked",
        &symbols,
        ArenaSymbolTablePacked::default(),
    );
    run_intern_unique(
        "ArenaSymbolTableUnpacked",
        &symbols,
        ArenaSymbolTableUnpacked::default(),
    );
    run_intern_unique("GermanSymbolTable", &symbols, GermanSymbolTable::default());
    run_intern_unique(
        "SmolStrSymbolTable",
        &symbols,
        SmolStrSymbolTable::default(),
    );
    run_intern_unique("LassoSymbolTable", &symbols, LassoSymbolTable::default());
    run_intern_unique("ArcSymbolTable", &symbols, ArcSymbolTable::default());
}
