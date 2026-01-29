# Head Buffer Overhead Findings

This document summarizes head-buffer memory findings from replaying
`chronoxide-otlp_all_11M.capture` with `headbuffer_replay`. Runs were done with
`labelset-store=flat_interned`, one window, and the same dataset across all
versions. The numbers below reflect `HeadWindow::estimated_bytes()` and
`HeadWindow::payload_bytes()` (payload excludes struct overhead but includes
timestamp/value encoding). These figures do not include HashMap bucket overhead
or label store memory.

## Dataset Summary

- Messages: 953,887
- Samples recorded: 27,690,759
- Series: 2,699,680
- Avg samples per series: ~10.26

## Results (MiB)

| Version                          | Raw total | Raw payload | Raw overhead | Gorilla total | Gorilla payload | Gorilla overhead |
|----------------------------------|----------:|------------:|-------------:|--------------:|----------------:|-----------------:|
| Original (per-series Vecs)       |    612.59 |      303.61 |       308.98 |        521.41 |          141.08 |           380.33 |
| Arena + boxed builder            |    550.79 |      303.61 |       247.18 |        388.27 |          141.08 |           247.18 |
| Arena + boxed builder + SmallVec |    530.20 |      303.61 |       226.60 |        367.68 |          141.08 |           226.60 |

## Observations

- Payload compression is unchanged by structural changes; only overhead moved.
- Gorilla/zigzag roughly halves the payload vs raw (141.08 MiB vs 303.61 MiB).
- Overhead stabilized across codecs after arena + boxed builder.
- SmallVec shaved another ~20 MiB of overhead by avoiding heap allocations for
  single-block series.

## Arena Slack (Final Version)

Raw encoding:

- Arena capacity: 304.00 MiB
- Arena used: 303.61 MiB
- Arena slack: 0.39 MiB (~0.01 B/sample)

Gorilla/zigzag encoding:

- Arena capacity: 144.00 MiB
- Arena used: 141.08 MiB
- Arena slack: 2.92 MiB (~0.11 B/sample)

Arena slack is not a major contributor.

## Why Raw Still ~42% Overhead

Raw total overhead remains high because per-series metadata is large relative to
payload per series:

- Overhead per series: 226.60 MiB / 2,699,680 ≈ 88 B/series
- Payload per series (raw): 11.50 B/sample * 10.26 ≈ 118 B/series
- Overhead fraction ≈ 88 / (118 + 88) ≈ 42%

This is a cardinality effect: many series with few samples.

## Largest Overhead Contributor

The largest contributor is **per-series metadata**, primarily the inline `Block`
stored in `SmallVec<[Block<C>; 1]>` (per-series, even for a single block). The
block metadata is roughly:

- `base_ms`, `min_ts`, `max_ts`: 24 B
- two `BufferRef`s: 24 B
- `samples` + padding: ~8-16 B

The rest of the per-series overhead is from the `Series` struct fields
(SmallVec header, sample counter, and the `Option<Box<BlockBuilder>>` pointer).

## What These Numbers Do Not Include

- HashMap bucket capacity and allocator metadata.
- Label store memory (symbol table and label sets).
- Transient allocations during decode/ingest.

## Summary

The arena refactor removed per-block heap allocations and eliminated codec-
dependent overhead. SmallVec further reduced per-series overhead by avoiding heap
allocations for the common single-block case. The remaining overhead is mostly
fixed per series, so total overhead is dominated by series cardinality.
