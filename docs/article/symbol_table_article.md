# SymbolTable: from `Arc<str>` Interning to an Arena (Data-Driven)

When you ingest OTLP metrics at scale, you quickly learn that you're not "storing time series data" — you're mostly storing **strings**: metric names and label keys/values.

In Chronoxide (OTLP-native TSDB prototype), every incoming datapoint touches multiple label pairs, and every label pair touches two strings (key + value). That makes the **SymbolTable** (string interner) part of the ingestion hot path and a major contributor to memory footprint.

This post describes:
- the first implementation, [ArcSymbolTable](https://github.com/REASY/chronoxide/blob/f57210ab091b0d29091f3c78448aa2b7095578cd/chronoxide-core/src/labels/symbol_table.rs#L117),
- what went wrong (performance + memory fragmentation),
- what production data (11 million OTLP messages) told us,
- the resulting replacement [ArenaSymbolTable](https://github.com/REASY/chronoxide/blob/f57210ab091b0d29091f3c78448aa2b7095578cd/chronoxide-core/src/labels/symbol_table.rs#L194), including evidence from a custom [TrackingAllocator](https://github.com/REASY/chronoxide/blob/f57210ab091b0d29091f3c78448aa2b7095578cd/chronoxide-core/src/alloc_tracking.rs#L129),
- and a short experiment with small-string types: `GermanSymbolTable` ([german-str crate](https://crates.io/crates/german-str)) and `SmolStrSymbolTable` ([smol_str](https://crates.io/crates/smol_str)).

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

### The first red flag: `Arc<str>` is a fat pointer on x86_64

On x86_64, both `&str` and `Arc<str>` are **fat pointers** (pointer + length):

```rust
use std::{mem::size_of, sync::Arc};

assert_eq!(size_of::<&str>(), 16);
assert_eq!(size_of::<Arc<str>>(), 16);

// Small-string optimized types aren't "tiny pointers" either.
assert_eq!(size_of::<german_str::GermanStr>(), 16);
assert_eq!(size_of::<smol_str::SmolStr>(), 24);
```

Those 16 bytes are just the handle. Every unique `Arc<str>` also requires a separate heap allocation containing:
- refcount header (strong/weak, typically `2 * usize`),
- the string bytes,
- allocator metadata and size-class rounding.

With a workload dominated by short strings, this fixed per-symbol overhead becomes painful.

Note: small-string types can still help by avoiding *some* heap allocations (when strings fit inline). We'll quantify that later.

## What 11 million OTLP messages told us

We processed **11,376,766** real OTLP messages (time window is 3.5 hours) and collected label statistics
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

### Measuring it: a `TrackingAllocator`

To avoid hand-waving, we built a small `TrackingAllocator` (cross-platform; uses `malloc_usable_size`/`malloc_size`/`_msize` where available) that tracks:
- requested bytes (`req_current`): what the program asked for,
- usable bytes (`usable_current`): what the allocator actually reserved (includes rounding),
- allocation/reallocation call counts.

Notes:
- `usable_current` is allocator-specific (`malloc_usable_size` on Linux/glibc) and is **not** process RSS; it also doesn't include allocator metadata/arenas.
- The `time` column below includes the TrackingAllocator's own atomic accounting overhead; use it for rough relative comparisons, not for fine-grained CPU benchmarking (Criterion results below are better for that).

We then ran a synthetic dataset shaped by production stats:
- `unique_total_symbols=100_513`
- intern workload: unique inserts (all symbols are distinct)

Result (`cargo run --release -p chronoxide-core --example symbol_table_memory -- 512 25000 75000`):

### Bench environment

- Ubuntu 25.10
- Kernel `6.17.0-8-generic`
- CPU: AMD Ryzen 9 9950X (16-core), x86_64
- Build flags: `-C target-cpu=native` (via `.cargo/config.toml`)
- Note: CPU frequency scaling/turbo can shift small deltas; keep clocks stable when comparing close results.

| SymbolTable              | Unique Symbols |    Time | Alloc Calls | Realloc Calls |          Req Current |        Usable Current |    Internal Frag |
|--------------------------|---------------:|--------:|------------:|--------------:|---------------------:|----------------------:|-----------------:|
| ArenaSymbolTablePacked   |        100,513 | 4.361ms |          18 |            33 | 6,029,328B (5.75MiB) |  6,037,480B (5.76MiB) |   8,152B (0.14%) |
| ArenaSymbolTableUnpacked |        100,513 | 4.252ms |          18 |            33 | 6,291,472B (6.00MiB) |  6,291,496B (6.00MiB) |      24B (0.00%) |
| GermanSymbolTable        |        100,513 | 4.437ms |      69,069 |            15 | 6,502,335B (6.20MiB) |  6,971,152B (6.65MiB) | 468,817B (6.73%) |
| SmolStrSymbolTable       |        100,513 | 4.264ms |      25,873 |            15 | 7,281,640B (6.94MiB) |  7,401,800B (7.06MiB) | 120,160B (1.62%) |
| ArcSymbolTable           |        100,513 | 8.543ms |     100,530 |            15 | 9,763,864B (9.31MiB) | 10,136,160B (9.67MiB) | 372,296B (3.67%) |

The headline is the allocation count: arena stays at **18**, while small-string types still do **tens of thousands** of allocations (`SmolStr`: 25,873; `GermanStr`: 69,069), vs **100,530** for `Arc<str>`.

That's the core reason `ArcSymbolTable` gets slower and more fragmented as cardinality rises.

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

We also tested an unpacked (aligned) variant:

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

## Benchmarks: speed + size

Criterion benchmark (`cargo bench -p chronoxide-core --bench symbol_table -- --warm-up-time 10 --sample-size 200`, same machine as above):

Notes:
- `intern/*` benches return the table to exclude drop/teardown cost from the timed region.
- `resolve+hash` hashes the resolved bytes to ensure the string is actually touched.

| Benchmark       | Arena (packed) | Arena (unpacked) | GermanStr |    SmolStr |       Arc |
|-----------------|---------------:|-----------------:|---------:|-----------:|----------:|
| `intern/mixed`  |      9.9544 ms |        10.054 ms | 10.712 ms |  9.9840 ms | 13.066 ms |
| `intern/unique` |      3.7835 ms |        3.8917 ms | 4.4849 ms |  4.3177 ms | 8.7886 ms |
| `lookup_hit`    |      5.6822 ms |        5.7099 ms | 5.9586 ms |  5.5003 ms | 5.0460 ms |
| `lookup_miss`   |      4.8432 ms |        4.8614 ms | 4.9052 ms |  4.8912 ms | 4.7206 ms |
| `resolve+hash`  |      1.4265 ms |        1.4202 ms | 1.5516 ms |  1.7207 ms | 1.6196 ms |

Best-effort size estimates after interning all unique symbols (these do **not** include allocator rounding / usable size):

| SymbolTable      | symbols | estimate_alloc_bytes | estimate_used_bytes |
|------------------|--------:|---------------------:|--------------------:|
| Arena (packed)   | 100,513 |            6,029,312 |           4,660,155 |
| Arena (unpacked) | 100,513 |            6,291,456 |           4,861,181 |
| GermanStr        | 100,513 |            6,502,319 |           5,393,359 |
| SmolStr          | 100,513 |            6,774,342 |           5,420,910 |
| Arc              | 100,513 |            9,431,029 |           8,077,597 |

### Side quest: `german-str` and `smol_str`

Because the production stats are dominated by short strings (~10–20 bytes), we tried two "off-the-shelf" small-string-optimized crates:

- [`german-str`](https://github.com/ostnam/german-str): `GermanStr` is 16 bytes and stores up to 12 bytes inline.
- [`smol_str`](https://github.com/rust-lang/rust-analyzer/tree/master/lib/smol_str): `SmolStr` is 24 bytes and stores up to 23 bytes inline (spills to heap for longer strings).

We implemented `GermanSymbolTable` and `SmolStrSymbolTable` in the most memory-friendly way we could: store each unique string once (in a `Vec<GermanStr>` / `Vec<SmolStr>`) and index by `u64 hash -> SymbolId` (same collision-checked approach as the arena table) to avoid duplicating keys in a `HashMap`.

What we found:

- `SmolStr` is the better fit for this workload shape: `intern/mixed` is close to arena, and the TrackingAllocator run shows far fewer allocations than `Arc<str>` (25,873 vs 100,530). But it's still much higher than arena's ~18 allocations, and it needs more memory than arena for the same symbol set.
- `GermanStr`'s 12-byte inline cap sits just below our typical ~13-byte keys/values, so many strings spill to the heap. That shows up as more allocations (69,069) and worse allocator fragmentation in the TrackingAllocator run.

For Chronoxide's SymbolTable goals (high cardinality, lots of unique inserts, and strict memory focus), SSO helps but doesn't beat a purpose-built arena.

### Packed vs Unpacked `SymbolLoc`

The packed vs unpacked choice is a classic "memory vs alignment" trade:

- **Memory**: packed `SymbolLoc` is 6 bytes, unpacked is 8 bytes. On this dataset the packed variant reserves ~256KiB less (because `id_to_loc` grows to a power-of-two capacity). With millions of symbols this becomes multiple MiB.
- **CPU**: if you only need the returned `&str`, unpacked avoids unaligned loads; if you touch/hash the bytes, the `SymbolLoc` alignment cost is usually drowned out by the string work (and is effectively a tie here: `1.4265ms` vs `1.4202ms`).
- **Intern hot path**: in this run, packed is slightly faster for both `intern/mixed` and `intern/unique`, but the deltas are small enough that hashing/`HashMap` effects likely dominate; don't pick a layout solely for `intern()` speed.

Practical default:
- If ingestion/head memory is the priority (and you rarely `resolve()`), packed is a good default.
- If `resolve()` is hot and you mostly just need the `&str` handle, unpacked can be safer/faster; if you process the string bytes, packed vs unpacked is usually a wash and memory tends to dominate.

This trade-off is expected:
- Arc is excellent at "read an already-interned pointer".
- Arena wins where it matters for ingestion: **interning new symbols**, which is heavy in high-cardinality environments.

## Downsides / trade-offs of `ArenaSymbolTable`

No free lunch:

- `lookup()` can be slower than `ArcSymbolTable` because we do an extra `resolve()` + string equality check to protect against hash collisions (even when collisions are rare).
- Packed `(offset,len)` metadata reduces per-symbol overhead, but it also means unaligned loads; the unpacked variant trades ~2 bytes/symbol for better alignment.
- Memory is **monotonic** (like most interners): you can't delete individual symbols without rebuilding.
- Using `u16` for length means the table rejects "too long" symbols. In real TSDB systems, you typically enforce label length limits and/or truncate+hash (Grafana Cloud does this); Chronoxide follows the same direction.

## Conclusion

`ArcSymbolTable` is a great "first correct implementation". But after running on real OTLP workloads (11 million messages) and seeing:
- ~2.62M unique symbols,
- most symbols in the ~10–20 byte range,
- and the allocator cost of per-symbol allocations,

we moved to `ArenaSymbolTable`:
- fewer allocations (18 vs ~100k on a 100k-symbol dataset),
- lower internal fragmentation (~0.00–0.14% vs ~3.67% in the TrackingAllocator run),
- and materially faster intern performance under unique-heavy workloads (≈3.78ms vs ≈8.79ms in the Criterion run).

Small-string types (`SmolStr` / `GermanStr`) were a useful sanity check: they reduce allocations and improve `intern/unique` vs `Arc<str>`, but they still can't match an arena on allocation count or memory density for high-cardinality workloads.

The key lesson is simple: **measure with real data**. The workload shape (string lengths + cardinality) strongly determines whether "simple and safe" is also "fast and cheap".
