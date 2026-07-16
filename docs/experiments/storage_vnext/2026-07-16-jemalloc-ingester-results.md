# Jemalloc Ingester Results

- **Date:** 2026-07-16
- **Status:** Linked jemalloc is available as an opt-in Linux GNU feature. The
  system allocator remains the default pending a clean long-run tuning gate.

## Scope

This experiment replaces the allocator used by the `chronoxide-ingester`
binary. It applies to both live Kafka ingestion and capture replay because
both use the same protobuf decode, label interning, head buffering, and segment
writing path.

It is independent of the persistent Zstd decoder context. That decoder is
specific to capture-file replay. Kafka batch decompression is performed by
`librdkafka`, while `capture_to` uses a separate Zstd encoder.

## Motivation

The post-adaptive-head profile replayed one million real capture messages and
sampled the complete process lifetime. `HeadBuffer::push_sample_to_window`
fell from 5.07% to 0.76% self CPU after the adaptive table, while explicit
allocator routines remained the largest named family at roughly 18%.

Raw profile:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717`

The profile captured 5,521 samples at 49 Hz with zero lost samples. It used the
real `kafka-capture-001` prefix, Schema 8, compact numeric series, the adaptive
head table, and one million messages.

## Process-wide preload screen

The first screen used one identical adaptive-head binary and selected the
allocator only with `LD_PRELOAD`. Its order was glibc, jemalloc, jemalloc,
glibc. Every run began after `POSIX_FADV_DONTNEED` left zero capture pages
resident.

Raw result:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/allocator-ab-20260716-224642`

The table contains means of two 250,000-message runs per mode.

| Metric | glibc | preloaded jemalloc | Change |
| --- | ---: | ---: | ---: |
| Wall time | 49.625 s | 45.095 s | -9.13% |
| Task clock | 49,549.835 ms | 45,129.605 ms | -8.92% |
| Cycles | 275.051 B | 250.216 B | -9.03% |
| Instructions | 768.039 B | 761.587 B | -0.84% |
| Branches | 140.523 B | 134.538 B | -4.26% |
| Branch misses | 497.303 M | 479.036 M | -3.67% |
| Minor faults | 1.632 M | 2.768 M | +69.56% |
| Peak RSS | 5,239,718 KiB | 5,263,848 KiB | +0.46% |
| Instructions/cycle | 2.792 | 3.044 | +9.00% |

All four runs emitted the same 34 files. The checksum-list digest was
`3094a23de602ff94e7e5898556c4935600b349b2933b31209d111fb2c1581423`.

This proves the allocator direction, but preload also intercepts allocations
inside linked C libraries. It does not by itself prove the result of Rust's
linked global allocator.

## Linked allocator screen

The linked comparison built two release binaries from the same source: the
system allocator with `--no-default-features`, and `tikv-jemallocator` 0.7.0
as Rust's global allocator. The 250,000-message order was system, jemalloc,
jemalloc, system. The final code keeps the system allocator as the package
default; `--features jemalloc` selects the measured linked allocator on Linux
GNU targets.

Raw result:

`/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/linked-jemalloc-ab-20260716-225734`

| Binary | SHA-256 | Build ID |
| --- | --- | --- |
| System | `6023387efce3e0fbfd1dbc5ccab3edf6e0c735c44b3ccccff45caeb609bd6c8a` | `1d6a4323da75c7871884205e3e420aa58ffb527d` |
| Jemalloc | `bf8e005baf3568f22fe71d051affee43bc5a6ff6bedd1771f18cf2d9155b357b` | `bb7f4b2930e23d3df4e7c6a5e6c8a2e351c2c937` |

The table contains means of two runs per binary.

| Metric | System | Linked jemalloc | Change |
| --- | ---: | ---: | ---: |
| Wall time | 49.240 s | 45.050 s | -8.51% |
| Task clock | 49,252.675 ms | 45,117.270 ms | -8.40% |
| Cycles | 273.693 B | 250.169 B | -8.59% |
| Instructions | 768.887 B | 759.339 B | -1.24% |
| Branches | 140.555 B | 133.852 B | -4.77% |
| Branch misses | 498.442 M | 450.283 M | -9.66% |
| Minor faults | 1.633 M | 2.705 M | +65.66% |
| Peak RSS | 5,240,518 KiB | 5,312,988 KiB | +1.38% |
| Instructions/cycle | 2.809 | 3.035 | +8.04% |

All four output manifests and file hashes were identical to the preload
screen. Logs independently reported `allocator="system"` or
`allocator="jemalloc"` for every intended binary.

## One-million-message scale gate

The adjacent one-million-message pair retained the CPU direction but exposed
a larger memory trade-off.

| Metric | System | Linked jemalloc | Change |
| --- | ---: | ---: | ---: |
| Wall time | 114.52 s | 98.00 s | -14.43% |
| Task clock | 114,653.52 ms | 98,279.99 ms | -14.28% |
| Cycles | 637.305 B | 545.931 B | -14.34% |
| Instructions | 1,623.445 B | 1,579.056 B | -2.73% |
| Branches | 298.612 B | 275.507 B | -7.74% |
| Branch misses | 1,321.140 M | 1,327.042 M | +0.45% |
| Minor faults | 2.247 M | 3.696 M | +64.52% |
| Peak RSS | 8,202,772 KiB | 9,030,492 KiB | +10.09% |
| Instructions/cycle | 2.547 | 2.892 | +13.55% |

Both runs emitted the same 34 files. Their checksum-list digest was
`c57bd2970b615958820edced252694180bede6d57ab898d4e864cefff5b70bfd`.

The replicated short screen is the stronger host-noise control. The long pair
is a scale check and demonstrates that default jemalloc arena/decay policy can
retain substantially more memory on this workload.

## Current decision

The linked allocator is a real ingest CPU improvement, not a replay-only
optimization. However, the untuned one-million-message RSS increase is too
large to ignore. Jemalloc therefore remains opt-in and the system allocator
remains the default. Before promoting jemalloc, compare bounded arena counts
and decay/background-purge policy with the same linked binary. The build-time
comparator is:

```sh
# System allocator (default)
cargo build --release -p chronoxide-ingester --bin chronoxide-ingester

# Linked jemalloc on Linux GNU
cargo build --release -p chronoxide-ingester --bin chronoxide-ingester \
  --features jemalloc
```

The next algorithmic experiment after allocator policy is a compact label-set
fingerprint index. It remains lower confidence: the estimated saving is only
about 32 MiB at one million messages, and separating fingerprints from series
locations adds another dependent random load on successful lookups.
