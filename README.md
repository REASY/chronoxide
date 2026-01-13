# Chronoxide

Public, minimal extract of the Chronoxide (OTLP-native TSDB) workbench, focused on **SymbolTable / string interning** performance and memory behavior under high-cardinality label workloads.

This repo exists to accompany the "`Arc<str>` vs arena interning" write-up and to provide a small, reproducible codebase with:
- [ArcSymbolTable](chronoxide-core/src/labels/symbol_table.rs#L114): baseline `HashMap<Arc<str>, SymbolId>` interner.
- [ArenaSymbolTable](chronoxide-core/src/labels/symbol_table.rs#L191): arena-backed interner (single `Vec<u8>` + `(offset,len)` per symbol).
- [TrackingAllocator](chronoxide-core/src/alloc_tracking.rs): a cross-platform global allocator wrapper that tracks requested vs usable allocation sizes to approximate allocator **internal fragmentation**.

## Layout

- `chronoxide-core/`
  - `src/labels/symbol_table.rs`: `ArcSymbolTable` + `ArenaSymbolTable`
  - `src/alloc_tracking.rs`: `TrackingAllocator`
  - `benches/symbol_table.rs`: Criterion benchmark comparing intern/lookup/resolve
  - `examples/symbol_table_memory.rs`: memory + fragmentation experiment using `TrackingAllocator`

## Requirements

- Rust `1.92.0+` (workspace `rust-version`)
- No external services required (synthetic dataset generation)

## Run the benchmark

```bash
cargo bench -p chronoxide-core --bench symbol_table -- --warm-up-time 10 --sample-size 200
```

The benchmark prints:
- wall-clock timings for `intern`/`lookup`/`resolve`
- best-effort size estimates (`estimate_allocated_bytes`, `estimate_used_bytes`)

## Run the memory/fragmentation experiment

```bash
  cargo run --release -p chronoxide-core --example symbol_table_memory -- 512 25000 75000
```

Arguments:
- `512`: number of unique “keys” generated (plus `__name__`)
- `25000`: number of “common” values (reused frequently)
- `75000`: number of “rare” values (high-cardinality / mostly-new)

The output includes:
- `req_current`: total bytes requested by allocations still live
- `usable_current`: total bytes actually reserved by the allocator (includes rounding)
- `internal_frag`: `usable_current - req_current` and percentage
- allocation call counts (`alloc_calls`, `realloc_calls`)

## Notes on dataset shape

The synthetic string generator is tuned to resemble real OTLP label workloads we observed during a 10M-message ingestion run:
- short keys (≈10–20 bytes typical, max ≈67)
- short-to-medium values with a long tail (up to ≈680)


