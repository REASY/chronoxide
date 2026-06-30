# PromQL Storage Milestones

**Goal:** Turn the current chunk-writing storage prototype into a shard-local TSDB segment store that can answer PromQL selectors and return sample streams.

**Current baseline:** The repo can ingest OTLP number datapoints into a windowed `HeadBuffer` and publish queryable sealed segment directories with `chunks.bin`, `chunk_index.bin`, `meta.json`, segment-local symbols, series metadata, and equality postings. `SegmentStoreReader` can query sealed segments and the active head from PromQL vector selector strings, merge samples by stable `series_id`, apply `=` / `!=` matchers, and prefer head samples for duplicate timestamps. WAL/recovery, regex selectors, full PromQL expression evaluation, cached head indexes, discovery APIs, and guardrails remain open.

## Compression Defaults

- Persist PromQL number samples as `FLOAT/GORILLA` chunks.
- Convert OTLP integer number datapoints to `f64` for the PromQL storage path.
- Encode timestamps as `t0_ms` plus varint deltas inside each chunk.
- Keep chunk frames uncompressed for v1 so `chunk_index.bin` can address exact chunk byte ranges.
- Keep `chunk_index.bin` fixed-width and mmap-friendly.
- Store segment label metadata with segment-local symbols and keyset/value-code encoding once v1 is stable.
- Store postings as delta-varint lists for small sets and roaring-style bitmaps for large sets.
- Use an FST over sorted label values for regex/prefix enumeration after exact selectors work.
- Keep WAL uncompressed and checksummed at first; consider zstd only after WAL throughput is measured.

## Milestone 1: Queryable Sealed In-Order Segments

Make each sealed segment self-describing and queryable for exact PromQL matchers over in-order float samples.

Deliverables:
- [x] PromQL-compatible metric and label name normalization.
- [x] Stable `series_id` derived from the canonical normalized labelset.
- [x] Segment-local `symbols.bin` containing label names and values.
- [x] `series.bin` v1 mapping segment-local `series_ref` to `series_id` and sorted label pairs.
- [x] A basic postings index for equality matchers.
- [x] A segment reader API that resolves exact selectors to `series_ref`s, filters `chunk_index.bin` by time, reads chunks, decodes samples, and returns labels plus samples.

Non-goals:
- Regex matchers.
- Negative-only selectors.
- Queryable head.
- OOO chunks.
- WAL/recovery.
- Full PromQL expression evaluation.

## Milestone 2: Full Selector Semantics and Discovery

Extend Milestone 1 from exact positive matchers to practical PromQL selector behavior.

Deliverables:
- [x] Negative equality matchers.
- [x] Regex and negative regex matchers.
- [x] Segment-local in-memory value enumeration from `series.bin` for regex expansion.
- [x] Per-label value FSTs in `indexes.puffin`.
- [x] All-series range support for negative-only selectors.
- [x] PromQL vector-selector adapter for metric selectors plus `=` / `!=` matchers.
- [x] Metadata discovery for metric names, label names, and label values from segment indexes.
- [x] Query guardrails for matched series, chunk reads, bytes read, samples decoded, and regex expansion.

## Milestone 3: Head Querying and Merge

Make recent, unsealed data visible to PromQL queries.

Deliverables:
- [x] Active head query overlay using label-store `SeriesRef`s and normalized PromQL labelsets.
- [x] Head postings/bitmaps over normalized labels for cached selector evaluation.
- [x] Head range scan over encoded head blocks.
- [x] Merge of head and sealed segment results by stable `series_id`.
- [x] Deterministic duplicate timestamp handling.

## Milestone 4: Durability and Recovery

Make ingestion restart-safe and segment discovery authoritative.

Deliverables:
- [x] WAL record format with crc32c.
- [x] Checkpoint records and `checkpoint.meta`.
- [x] WAL replay into head on startup.
- [x] Manifest `CURRENT` and `MANIFEST-*`.
- [x] Manifest-published segment inventory.
- [ ] Segment footer checksums and validation.
- [ ] WAL truncation once manifest-published segments cover the data.

## Milestone 5: Out-of-Order Lane

Handle bounded late samples without breaking sealed segment immutability.

Deliverables:
- Per-series last timestamp tracking.
- `out_of_order_time_window` acceptance policy.
- OOO-only chunks or overlapping OOO segments.
- Query merge of in-order and OOO lanes.
- Last-write-wins dedupe using deterministic precedence.

## Milestone 6: Retention and Maintenance

Keep long-running shards bounded and operationally safe.

Deliverables:
- SSD retention tombstones in the manifest.
- Crash-safe move to `.trash`.
- Manifest compaction.
- Bounded open-segment metadata cache.
- Optional segment packing for sparse/high-cardinality workloads.

## Implementation Order

1. Implement Milestone 1 before adding WAL or OOO. A queryable segment is the smallest useful TSDB unit.
2. Keep chunk compression simple until selector/index correctness is in place.
3. Add WAL and manifest authority before treating the system as durable.
4. Add regex and FST after exact postings are proven with tests.
