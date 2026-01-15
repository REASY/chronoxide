# Stop Allocating Per Label: A Data‑Driven Rust SymbolTable for OTLP/TSDB

When you ingest OTLP metrics at scale, you quickly learn that you're not "storing time series data" — you're mostly storing **strings**: metric names and label keys/values.

In Chronoxide (OTLP-native TSDB prototype), every incoming datapoint touches multiple label pairs, and every label pair touches two strings (key + value). That makes the **SymbolTable** (string interner) part of the ingestion hot path and a major contributor to memory footprint.

This post analyzes why my initial [ArcSymbolTable](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/labels/symbol_table.rs#L219) implementation failed under a production workload of 11 million OTLP messages, and how replacing it with a custom [ArenaSymbolTable](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/labels/symbol_table.rs#L703) reduced memory overhead and allocation counts by orders of magnitude. I also evaluated two Small-String Optimization (SSO) crates: [german-str](https://crates.io/crates/german-str) and [smol_str](https://crates.io/crates/smol_str).

## TL;DR

- [ArcSymbolTable](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/labels/symbol_table.rs#L219) is simple but does ~1 heap allocation per unique symbol; on 100,513 unique strings: **100,530** `alloc_calls` and `intern/unique` **8.89ms**.
- [ArenaSymbolTablePacked](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/labels/symbol_table.rs#L919) stores bytes in a single grow-only `Vec<u8>` and keeps `(offset,len)` per symbol; on the same dataset: **18** `alloc_calls`, `intern/unique` **3.85ms**, and **0.14%** internal fragmentation.
- [SmolStrSymbolTable](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/labels/symbol_table.rs#L533) is the best off-the-shelf SSO option I tried, but it still does **25,873** allocations and uses more memory than arena.
- [GermanSymbolTable](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/labels/symbol_table.rs#L354) spills often for this workload shape (12B inline cap vs ~13B typical strings), leading to **69,069** allocations and **6.73%** internal fragmentation.
- [LassoSymbolTable](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/labels/symbol_table.rs#L297) (via [lasso](https://crates.io/crates/lasso)) keeps allocations low (**20** `alloc_calls`), but `intern/unique` is slower at **6.43ms**; for memory, rely on TrackingAllocator output (estimates are disabled).

## What is a SymbolTable?

A SymbolTable maps strings to small integer IDs and back:

- `intern("service.name") -> SymbolId(123)`
- `resolve(SymbolId(123)) -> "service.name"`
- `lookup("service.name") -> Option<SymbolId>`

Everything else (label sets, postings, dictionaries, etc.) can store `SymbolId` instead of storing/copying strings repeatedly.

In Chronoxide, `SymbolId` is a dense `u32`, and `SymbolTable` is a trait:

```rust
pub trait SymbolTable {
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
    fn lookup(&self, symbol: &str) -> Option<SymbolId>;
    fn intern(&mut self, symbol: &str) -> Result<SymbolId, SymbolTableError>;
    fn resolve(&self, id: SymbolId) -> &str;
}
```

## The baseline: `ArcSymbolTable`

The simplest correct interner in Rust is: allocate each unique string once and share it via `Arc`.

Chronoxide's first implementation looked like this (simplified):

```rust
pub struct ArcSymbolTable {
    symbol_to_id: HashMap<Arc<str>, SymbolId>,
    id_to_symbol: Vec<Arc<str>>,
}
```

How it behaves:
- On a miss, `intern()` allocates a new `Arc<str>` (heap allocation + copy), pushes it into `id_to_symbol`, and inserts it into `symbol_to_id`.
- On a hit, it returns the previously assigned ID.

Why it's attractive:
- Basic, safe, and straightforward to reason about.
- `resolve()` is effectively `&self.id_to_symbol[id]` (returns a pointer/len), which is extremely fast; the cost comes later when you actually process the string bytes.

### The first red flag: per-symbol allocation overhead

On x86_64, both `&str` and `Arc<str>` are **fat pointers** (pointer + length). The handle itself is small — the real cost is that **each unique `Arc<str>` also requires a heap allocation**.

```rust
use std::{mem::size_of, sync::Arc};

assert_eq!(size_of::<&str>(), 16);
assert_eq!(size_of::<Arc<str>>(), 16);
```

Every unique `Arc<str>` allocation includes:
- refcount header (strong/weak, typically `2 * usize`),
- the string bytes,
- allocator metadata and size-class rounding.

With a workload dominated by short strings (~10–20 bytes), this fixed per-symbol overhead becomes painful.

## What 11 million OTLP messages told me

I processed **11,376,766** real OTLP messages (time window is 3.5 hours) and collected label statistics:
- Data points observed: **413,593,326**
- Series observed: **79,005,309**
- Unique symbols interned: **2,621,843**

And the most important part for the SymbolTable design: typical string lengths are small:

| Metric                                | Mean  | StdDev | Min | Max  | P50 | P75 | P95 | P99 |
|---------------------------------------|-------|--------|-----|------|-----|-----|-----|-----|
| Labels per Series                     | 23.41 | 8.52   | 3   | 64   | 22  | 30  | 39  | 47  |
| Avg Key Length/Series (Bytes/label)   | 13.33 | 2.66   | 6   | 32   | 13  | 14  | 19  | 23  |
| Avg Value Length/Series (Bytes/label) | 13.22 | 3.84   | 5   | 76   | 12  | 16  | 20  | 22  |
| Key Len Max/Series (Bytes)            | 28.98 | 7.31   | 12  | 71   | 29  | 29  | 49  | 63  |
| Value Len Max/Series (Bytes)          | 64.29 | 51.77  | 11  | 2048 | 44  | 64  | 191 | 193 |

This is the "`Arc<str>` pain zone": when many strings are ~10–20 bytes, the per-string allocation/header/rounding dominates.

## The real problem: allocation count and fragmentation

`ArcSymbolTable` does **one allocation per unique string**.

With ~2.62M unique symbols, that's ~2.62M heap allocations just for symbol storage, plus additional allocations for growing the `HashMap` and `Vec`.

That hurts in two ways:

1) **CPU cost in the hot path**: allocating and copying strings for misses.
2) **Allocator overhead**: lots of small allocations scatter across allocator size classes; size-class rounding and bookkeeping add overhead beyond "useful string bytes".

To quantify this, I measured allocation behavior (custom [TrackingAllocator](https://github.com/REASY/chronoxide/blob/2dc78e9a5fbac0c23da98b0142c53984ed18d82a/chronoxide-core/src/alloc_tracking.rs#L129)) and CPU (Criterion). The numbers are in the Results section below.

## The fix: `ArenaSymbolTable`

The production data suggested the right optimization: **stop allocating per string**.

`ArenaSymbolTable` stores all string bytes in a single growing `Vec<u8>` ("arena"), and stores a compact location per symbol:

```rust
#[repr(C, packed)] // size=6, align=1
struct PackedSymbolLoc {
    offset: u32,
    len: u16,
}
```

I also tested an unpacked (aligned) variant:

```rust
#[repr(C)] // size=8, align=4 (padding for alignment)
struct UnpackedSymbolLoc {
    offset: u32,
    len: u16,
}
```

So each symbol is:
- bytes appended to `arena`,
- one `(offset,len)` entry appended to `id_to_loc`,
- and an entry in `hash_to_id`.

The main structure (simplified):

```rust
pub struct ArenaSymbolTable {
    hash_to_id: U64HashMap<SymbolId>,
    hash_collisions: U64HashMap<Vec<SymbolId>>,
    arena: Vec<u8>,
    id_to_loc: Vec<PackedSymbolLoc>, // or UnpackedSymbolLoc
}
```

### How `intern()` works (high-level)

1) Hash the input string.
2) Look up the hash in `hash_to_id`.
3) If the candidate resolves to the same bytes, return it.
4) If there's a hash collision, scan the collision list and compare strings.
5) Otherwise, append bytes to `arena`, append `SymbolLoc`, assign next `SymbolId`.

In practice, hash collisions are rare enough that the collision path is usually cold (in one production report: `hash_collisions_len=0` for millions of symbols).

### Why arena + (offset,len) helps so much

- **Almost no heap allocations**: the arena grows occasionally; `id_to_loc` and hash tables grow occasionally.
- **Less allocator waste**: fewer small allocations → less size-class rounding / internal fragmentation.
- **Better CPU cache behavior**: `SymbolLoc` is tiny; string bytes are stored contiguously.

## Results: allocations and fragmentation

Raw output: `memory_results.log`.

I can't re-run multi-million-unique-symbol tests for every iteration, so I use a repeatable dataset shaped by production length distributions:
- `unique_total_symbols=100_513` (all unique inserts)
- built from `unique_keys=512`, `common_values=25_000`, `rare_values=75_000` (the example adds `__name__`, so keys=513)
- command: `cargo run --release -p chronoxide-core --example symbol_table_memory -- 512 25000 75000`

The `TrackingAllocator` tracks requested bytes (`req_current`) vs allocator-reserved bytes (`usable_current`). `internal_frag` is `usable_current - req_current` (size-class rounding); it is not process RSS.

The `time` column includes TrackingAllocator's own atomic accounting overhead; use it for rough relative comparisons, not fine-grained CPU benchmarking.

| SymbolTable      | Time, ms | Alloc Calls | Realloc Calls |          Req Current |        Usable Current | Internal Fragmentation |
|------------------|---------:|------------:|--------------:|---------------------:|----------------------:|-----------------------:|
| Arena (packed)   |    4.434 |          18 |            33 | 6,029,328B (5.75MiB) |  6,037,480B (5.76MiB) |         8,152B (0.14%) |
| Arena (unpacked) |    3.971 |          18 |            33 | 6,291,472B (6.00MiB) |  6,291,496B (6.00MiB) |            24B (0.00%) |
| GermanStr        |    4.563 |      69,069 |            15 | 6,502,335B (6.20MiB) |  6,971,152B (6.65MiB) |       468,817B (6.73%) |
| SmolStr          |    4.386 |      25,873 |            15 | 7,281,640B (6.94MiB) |  7,401,800B (7.06MiB) |       120,160B (1.62%) |
| Lasso            |    6.055 |          20 |            14 | 6,479,112B (6.18MiB) |  6,479,192B (6.18MiB) |            80B (0.00%) |
| Arc              |    9.371 |     100,530 |            15 | 9,763,864B (9.31MiB) | 10,136,160B (9.67MiB) |       372,296B (3.67%) |

The headline is allocation count: arena stays at **18** and Lasso is nearly as low at **20**, while small-string types still do **tens of thousands** of allocations (`SmolStr`: 25,873; `GermanStr`: 69,069), vs **100,530** for `Arc<str>`.

## Results: speed + size

Raw output: `bench_results.log`.

Criterion benchmark: `cargo bench -p chronoxide-core --bench symbol_table -- --warm-up-time 5 --sample-size 400`
Dataset: `unique_total=100_513` (keys=513, common_values=25_000, rare_values=75_000).

Notes:
- `intern/*` benches return the table to exclude drop/teardown cost from the timed region.
- `resolve+hash` hashes the resolved bytes to ensure the string is actually touched.

| Benchmark       | Arena (packed), ms | Arena (unpacked), ms | GermanStr, ms | SmolStr, ms | Lasso, ms | Arc, ms |
|-----------------|-------------------:|---------------------:|--------------:|------------:|----------:|--------:|
| `intern/mixed`  |            10.3140 |              10.2250 |       10.7150 |     10.1800 |   12.4800 | 13.4860 |
| `intern/unique` |             3.8544 |               3.9410 |        4.5199 |      4.4196 |    6.4287 |  8.8852 |
| `lookup_hit`    |             5.7041 |               5.7748 |        6.0212 |      5.4822 |    5.4763 |  5.0876 |
| `lookup_miss`   |             4.9085 |               4.8256 |        4.9160 |      4.8680 |    4.8613 |  4.8479 |
| `resolve+hash`  |             1.4285 |               1.4262 |        1.6496 |      1.6880 |    1.5539 |  1.5745 |

Best-effort size estimates after interning all unique symbols (these do **not** include allocator rounding / usable size). Lasso estimates are intentionally `0` because the current accounting is too far off; use TrackingAllocator results for its memory:

| SymbolTable      | symbols | estimate_alloc_bytes | estimate_used_bytes |
|------------------|--------:|---------------------:|--------------------:|
| Arena (packed)   | 100,513 |            6,029,312 |           4,660,155 |
| Arena (unpacked) | 100,513 |            6,291,456 |           4,861,181 |
| GermanStr        | 100,513 |            6,502,319 |           5,393,359 |
| SmolStr          | 100,513 |            6,774,342 |           5,420,910 |
| Lasso            | 100,513 |                    0 |                   0 |
| Arc              | 100,513 |            9,431,029 |           8,077,597 |

### Evaluating Small-String Optimization (SSO)

> For a deeper explanation of the "German string" layout (and why it shows up in systems code), CedarDB’s post is excellent: [Why German Strings are Everywhere](https://cedardb.com/blog/german_strings/)

Because the production stats are dominated by short strings (~10–20 bytes), I tried two "off-the-shelf" small-string-optimized crates:

- [german-str](https://github.com/ostnam/german-str): `GermanStr` is 16 bytes and stores up to 12 bytes inline.
- [smol_str](https://github.com/rust-lang/rust-analyzer/tree/master/lib/smol_str): `SmolStr` is 24 bytes and stores up to 23 bytes inline (spills to heap for longer strings).

On x86_64: `size_of::<german_str::GermanStr>() == 16` and `size_of::<smol_str::SmolStr>() == 24`.

I implemented `GermanSymbolTable` and `SmolStrSymbolTable` in the most memory-friendly way I could: store each unique string once (in a `Vec<GermanStr>` / `Vec<SmolStr>`) and index by `u64 hash -> SymbolId` (same collision-checked approach as the arena table) to avoid duplicating keys in a `HashMap`.

What I found:

- `SmolStr` is the better fit for this workload shape: `intern/mixed` is close to arena, and the TrackingAllocator run shows far fewer allocations than `Arc<str>` (25,873 vs 100,530). But it's still much higher than arena's ~18 allocations, and it needs more memory than arena for the same symbol set.
- `GermanStr`'s 12-byte inline cap sits right below the typical key/value length (~13 bytes), so a large fraction spills to the heap. That shows up as more allocations (69,069) and worse allocator fragmentation in the TrackingAllocator run.

For Chronoxide's SymbolTable goals (high cardinality, lots of unique inserts, and strict memory focus), SSO helps but doesn't beat a purpose-built arena.

### Lasso (`lasso::Rodeo`)

`lasso` is a popular interner crate built around `Rodeo`, which returns compact `Spur` keys (`u32`) and keeps string storage in its own arena. I added `LassoSymbolTable` as a drop-in comparison.

On this dataset, Lasso keeps allocations low (**20** `alloc_calls`) and has similar requested memory to the arena tables (~6.18MiB), but its `intern/mixed` and `intern/unique` timings are slower (12.48ms and 6.43ms). Lookup/resolve are in the same ballpark as the other tables.

Memory estimates for Lasso are intentionally set to `0` because the current accounting is too far off; use the TrackingAllocator results above for realistic memory comparisons.

### Packed vs Unpacked `SymbolLoc`

The packed vs unpacked choice is a classic "memory vs alignment" trade:

- **Memory**: packed `SymbolLoc` is 6 bytes, unpacked is 8 bytes. On this dataset the packed variant reserves ~256KiB less (because `id_to_loc` grows to a power-of-two capacity). With millions of symbols this becomes multiple MiB.
- **CPU**: if you only need the returned `&str`, unpacked avoids unaligned loads; if you touch/hash the bytes, the `SymbolLoc` alignment cost is usually drowned out by the string work (and is effectively a tie here: `1.4285ms` vs `1.4262ms`).
- **Intern hot path**: in this run, packed wins on `intern/unique` while unpacked edges out on `intern/mixed`, but the deltas are small enough that hashing/`HashMap` effects likely dominate; don't pick a layout solely for `intern()` speed.

Practical default:
- If ingestion/head memory is the priority (and you rarely `resolve()`), packed is a good default.
- If `resolve()` is hot and you mostly just need the `&str` handle, unpacked can be safer/faster; if you process the string bytes, packed vs unpacked is usually a wash and memory tends to dominate.

This trade-off is expected:
- Arc is excellent at "read an already-interned pointer".
- Arena wins where it matters for ingestion: **interning new symbols**, which is heavy in high-cardinality environments.

## Downsides / trade-offs of `ArenaSymbolTable`

No free lunch:

- `lookup()` can be slower than `ArcSymbolTable` because it does an extra `resolve()` + string equality check to protect against hash collisions (even when collisions are rare).
- Packed `(offset,len)` metadata reduces per-symbol overhead, but it also means unaligned loads; the unpacked variant trades ~2 bytes/symbol for better alignment.
- Memory is **monotonic** (like most interners): you can't delete individual symbols without rebuilding.
- Using `u16` for length means the table rejects "too long" symbols. In real TSDB systems, you typically enforce label length limits and/or truncate+hash (Grafana Cloud does this); Chronoxide follows the same direction.

## Conclusion

`ArcSymbolTable` is a great "first correct implementation". But after running on real OTLP workloads (11 million messages) and seeing:
- ~2.62M unique symbols,
- most symbols in the ~10–20 byte range,
- and the allocator cost of per-symbol allocations,

I moved to `ArenaSymbolTable`:
- fewer allocations (18 vs ~100k on a 100k-symbol dataset),
- lower internal fragmentation (~0.00–0.14% vs ~3.67% in the TrackingAllocator run),
- and materially faster intern performance under unique-heavy workloads (≈3.85ms vs ≈8.89ms in the Criterion run).

Small-string types (`SmolStr` / `GermanStr`) were a useful sanity check: they reduce allocations and improve `intern/unique` vs `Arc<str>`, but they still can't match an arena on allocation count or memory density for high-cardinality workloads.

The key lesson is simple: **measure with real data**. The workload shape (string lengths + cardinality) strongly determines whether "simple and safe" is also "fast and cheap".

## Appendix: Bench Environment

- Ubuntu 25.10
- Kernel `6.17.0-8-generic`
- CPU: AMD Ryzen 9 9950X (16-core), x86_64
- Build flags: `-C target-cpu=native` (via `.cargo/config.toml`)
- Note: CPU frequency scaling/turbo can shift small deltas; keep clocks stable when comparing close results.
