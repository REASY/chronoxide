# Phase 2 governed compact query-label IDs

- **Measured:** 2026-07-21, final run started at 17:55 `+08:00`
- **Source provenance:** base commit
  `d63642ddde16f07873e0e665ec0f9c8f3f2b8486` plus sealed tracked-source
  patch SHA-256
  `300c273d7ac5b4b0c62dabb448cd371f22cd196c5ac60b2511e7bcdc94182c94`
- **Query binary SHA-256:**
  `50cbb53205a3267dbaa7b83ebc396a9820ff14475d3bda5e052ae8f8af313a0e`
- **Corpus:** 66 files, 5,569,314,896 bytes, inventory SHA-256
  `28547c0fc2b738eb58948400602640c017844cd57bd49917bffdf100a6e14a0b`
- **Query corpus fingerprint:**
  `7e5cf252e5df9bdb786e1b9deb9248f09667962ac559f339ba47312c5c0e3ca3`
- **Fixed query-manifest SHA-256:**
  `3420740cc3e5eb38e82ca53b58d6d1a075b9007380b8745e0193ec18236a07e7`
- **Runner/gate SHA-256:**
  `15995d4a7474361b6ccddf4215241836c71fd477951ff6ee50df406154827788`
  / `c4d01a4799c9eb9059087fb5fe3bd1591c87cea49f5295d1960bb8a52aa32eb2`
- **Raw result:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase2-compact-ids-final-20260721-175509`
- **Status:** correctness and performance gates passed; promoted for Schema 7/8

Compact query-local IDs are a material improvement for the workload that
motivated Phase 2. Against the same-binary `OwnedStrings` comparator, the
broad selector improved 14.82% cold and 10.20% warm and reduced process peak
RSS by 71.48%. The final gate covered 176 fresh processes and 528 query
evaluations. Exact and portable result fingerprints, result cardinality,
every public `QueryStats` field, payload I/O counters, and label-integrity
counters matched. All control rows passed their latency and RSS gates.

The accepted decision is to use `CompactIds` for normal Schema 7/8 query
sessions and retain `OwnedStrings` as an explicit one-binary comparator. This
is an in-memory execution change, not a storage-format change.

## Runtime design

Each query session owns one aggregate, byte-governed label arena. Distinct
UTF-8 strings admitted to that arena receive dense query-local `u32` IDs, and
a label pair is exactly `(u32 name_id, u32 value_id)`, eight bytes. Query
results carry an `Arc` to the arena, so returned IDs remain valid after the
session and its generation-translation tables have been dropped.

The Schema 7/8 metadata facade supplies verified encoded label pairs instead
of first allocating complete `(String, String)` pairs. The path preserves the
existing authority boundary:

- it decodes the complete canonical source row, resolves every canonical name
  and value, hashes every pair in order, and preserves full source identity
  separately from any demand-driven visible subset;
- it validates touched metadata and symbol state and verifies the authoritative
  stored series identity before matcher success, merge, cache publication, or
  result exposure;
- every source-symbol translation table is bound to immutable segment
  generation provenance and its authoritative symbol count; a foreign
  generation or changed count is an error, not a cache miss;
- the first translation of a source symbol resolves its UTF-8 bytes and interns
  them with hash-bucket plus exact-byte collision checking; later translations
  in that generation are direct paged-table lookups after the mandatory source
  row verification described above;
- identical strings from different segment dictionaries converge on the same
  query-local atom, while missing and explicit-empty label values remain
  distinct; and
- corruption, limit, allocation, and arena-budget errors propagate. There is
  no automatic retry or fallback to `OwnedStrings`.

Compact labels remain compact through sealed-source merge, deduplication,
terminal grouping, range evaluation, and result construction. Same-arena
equality has an ID-pair fast path; ordering and cross-arena comparison retain
canonical string-byte semantics. Demand-driven terminal aggregations allocate
only a new governed pair block when narrowing the visible names. Typed
Histogram and ExponentialHistogram scalar projections rewrite `_count` and
`_sum` metric names through a cached derived atom instead of rebuilding an
owned label set.

Prometheus API vector/matrix encoding and exact/portable fingerprinting walk
borrowed compact pairs directly. They do not first construct a complete owned
compatibility copy. This matters because serialization and fingerprinting are
observable boundaries where a hidden materialization would otherwise erase a
large part of the RSS win.

## `QueryLabels` source API change

`QueryLabels` no longer exposes `as_slice`, `AsRef<[(String, String)]>`, or
`Deref` to an owned-string slice. Those interfaces would require retaining a
second complete representation for compact results. Consumers now use:

- `pairs()` or `iter()` for a borrowed exact-size iterator of `(&str, &str)`;
- `visit_pairs()` for callback-style borrowed traversal; or
- `to_vec()` when an explicit caller-owned `(String, String)` copy is actually
  required.

This is an intentional Rust source-level API change. It does not change HTTP
response JSON, PromQL label semantics, fingerprints, or on-disk bytes. The
stable `compact_compatibility_materializations` counter remains as a tripwire
and was zero in every accepted observation.

## Governor and accounting model

The default compact-arena limit is 512 MiB per query session. It is a portable
modeled retained-allocation admission budget, not allocator `usable_size` and
not a promise that whole-process RSS remains below 512 MiB. The model charges
four reconciling categories:

| Category | Modeled retained allocation |
| --- | --- |
| Atoms | Arena/root `Arc`, fixed atom directory, lazily allocated atom-slot chunks, and aligned `Arc<str>` allocations including tail padding |
| Pairs | Each boxed eight-byte pair array plus its owning `Arc<CompactQueryLabels>` object |
| Hash directory | A fixed initial reserve plus conservative portable envelopes for hash admissions and derived metric-name entries |
| Translations | Per-generation page directories, lazy 4,096-entry `u32` pages, and a conservative outer-list capacity envelope |

Admission reserves the modeled bytes before allocating or publishing the
object. RAII charge guards release each category after its payload fields are
dropped, and checked release arithmetic fails closed rather than wrapping.
Labels retained by a result intentionally keep the arena, atoms, and their
pair blocks alive; session-local translation pages release when the session is
dropped.

The HashMap and outer translation-list envelopes are deliberately
implementation-independent and conservative. Allocator metadata, size-class
rounding beyond the modeled object alignment, fragmentation, stacks, decoded
samples, metadata caches, and all other process state are outside this arena
counter; process RSS is the authority for those effects. There is also a tiny
drop-order interval in which a guard has logically released its charge while
the containing `Arc` allocation is completing destruction. Relaxed concurrent
counter snapshots can transiently disagree for the same reason. The benchmark
takes quiescent snapshots and requires exact category reconciliation there.

For the broad compact cold run, the retained snapshot was:

| Accounting field | Bytes | MiB |
| --- | ---: | ---: |
| Atom storage | 17,652,616 | 16.835 |
| Pair blocks | 109,741,280 | 104.657 |
| Hash directory | 10,458,080 | 9.974 |
| Generation translations | 1,509,632 | 1.440 |
| **Current/retained total** | **139,361,608** | **132.906** |
| Peak | 139,361,928 | 132.906 |
| Configured budget | 536,870,912 | 512.000 |

That run represented 11,134,014 compact pairs in 334,665 label sets. It
performed 16,253,586 source-symbol translations: 16,144,619 hits and 108,967
misses. The arena retained 108,918 unique strings containing 12,849,301 UTF-8
bytes. In contrast, the unchanged logical materialization counter for the
broad query was 415,320,441 label-content bytes. The distinction is
intentional: one describes the query's semantic label work, while the other
describes unique retained compact storage.

Across the complete matrix, current charge never exceeded 139,361,608 bytes,
peak charge never exceeded 139,361,928 bytes, retained charge always equaled
the four current categories, translation and atom counters reconciled, and
there were zero arena admission refusals and zero compatibility
materializations.

## Fixed measurement matrix

The sealed manifest contains eleven expressions:

| Query name | Shape | Exact expression |
| --- | --- | --- |
| `broad_raw_count_selector` | Instant, full labels | `{__name__=~"^http_.*_count$"}` |
| `equality_last` | Instant, full demand | `last_over_time({service_name_x55e50a58f9befba7="chatgpt-in-slack"}[5m])` |
| `sparse_regex_last` | Instant, full demand | `last_over_time({__name__=~"^ag_consul_(request\|watch)_x[0-9a-f]+_count$"}[5m])` |
| `negative_matcher_last` | Instant, full demand | `last_over_time({__name__="http_client_duration_xf5f33b0f6bbd8257_count",service_name_x55e50a58f9befba7!="chatgpt-in-slack"}[5m])` |
| `no_result` | Instant, empty control | `{service_name_x55e50a58f9befba7="__chronoxide_phase2_missing__"}` |
| `scalar_rate_sum_instant` | Instant, selective | `sum by (service_name_x55e50a58f9befba7)(rate(container_cpu_usage_seconds_total[15m]))` |
| `scalar_rate_sum_range` | Range, selective | `sum by (service_name_x55e50a58f9befba7)(rate(container_cpu_usage_seconds_total[15m]))` |
| `native_hist_count_range` | Range, selective | `count by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257[15m]))` |
| `native_hist_p95_range` | Range, full control | `histogram_quantile(0.95,sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257[15m])))` |
| `native_exp_count_range` | Range, selective | `count by (service_name_x55e50a58f9befba7)(rate(ag_consul_request_x0f4a28dca7d2d184[15m]))` |
| `native_exp_p95_range` | Range, full control | `histogram_quantile(0.95,sum by (service_name_x55e50a58f9befba7)(rate(ag_consul_request_x0f4a28dca7d2d184[15m])))` |

Instant queries evaluated at `1782980413585`; the broad row used the explicit
requested interval `1782980113585..1782980413585`, and the other instant rows
used `0..1782980413585`. Range queries used
`1782978613585..1782980413585` with a 300,000 ms step, seven evaluations per
range execution, and a disabled range-scalar cache.

Every query used four counterbalanced blocks:

```text
odd blocks:  owned-strings, compact-ids, compact-ids, owned-strings
even blocks: compact-ids, owned-strings, owned-strings, compact-ids
```

Each fresh process executed one CLI-cold and two warm evaluations. This gives
eight cold and sixteen warm observations per arm and query. All processes used
one identical release binary, Schema 8, demand-driven label ownership, pread
with queue depth 128, no prewarm, no prefetch, Off instrumentation, a zero-byte
range cache, and the 512 MiB compact-arena budget.

The raw files use `chronoxide.query-benchmark.raw/v11`; the aggregate gate uses
`chronoxide/storage-vnext-phase2-compact-ids-ab/v2`. Measurement ran on one AMD
Ryzen 9 9950X host with Linux 7.0.0, Rust 1.97.0, and the corpus/result tree on
ext4. The observation ranges retained in the artifact are descriptive, not
confidence intervals or a cross-host claim.

Before every process, all 66 corpus files received `POSIX_FADV_DONTNEED`, and
`fincore` reported zero resident corpus bytes in all 176 cases. As in Phase 1,
CLI-cold means the first expression in a fresh process and query session.
Store startup and corpus-fingerprint work may repopulate pages before the
timed expression; this does not claim a flushed NVMe/controller cache or zero
residency at the exact query boundary. Process peak RSS covers the complete
three-evaluation process and is not split into cold and warm values.

## End-to-end results

The table reports medians. Delta is `(CompactIds / OwnedStrings - 1)`; negative
is faster or smaller.

| Query | Owned cold | Compact cold | Delta | Owned warm | Compact warm | Delta | Owned RSS | Compact RSS | Delta |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Broad raw `_count` regex | 4,531.35 ms | 3,859.86 ms | **-14.82%** | 4,042.79 ms | 3,630.22 ms | **-10.20%** | 1,868.65 MiB | 532.88 MiB | **-71.48%** |
| Equality `last_over_time` | 32.98 ms | 33.74 ms | +2.31% | 7.56 ms | 8.14 ms | +7.65% | 19.39 MiB | 18.40 MiB | -5.11% |
| Sparse regex `last_over_time` | 102.36 ms | 96.08 ms | -6.14% | 60.85 ms | 57.59 ms | -5.36% | 38.47 MiB | 28.93 MiB | -24.78% |
| Negative matcher `last_over_time` | 48.21 ms | 44.81 ms | -7.04% | 18.90 ms | 17.47 ms | -7.55% | 32.33 MiB | 25.18 MiB | -22.11% |
| No result | 1.805 ms | 1.806 ms | +0.03% | 0.0342 ms | 0.0333 ms | -2.55% | 11.78 MiB | 11.83 MiB | +0.43% |
| Scalar rate/sum instant | 912.58 ms | 923.96 ms | +1.25% | 745.79 ms | 766.11 ms | +2.72% | 94.03 MiB | 105.12 MiB | +11.79% |
| Scalar rate/sum range | 2,803.86 ms | 2,798.80 ms | -0.18% | 2,607.18 ms | 2,603.70 ms | -0.13% | 101.25 MiB | 101.16 MiB | -0.09% |
| Native Histogram count range | 442.84 ms | 418.57 ms | -5.48% | 381.74 ms | 357.18 ms | -6.43% | 38.96 MiB | 37.43 MiB | -3.93% |
| Native Histogram p95 range | 455.54 ms | 426.17 ms | -6.45% | 392.34 ms | 365.66 ms | -6.80% | 39.12 MiB | 37.27 MiB | -4.73% |
| Native ExponentialHistogram count range | 329.48 ms | 307.18 ms | -6.77% | 256.56 ms | 233.72 ms | -8.90% | 33.08 MiB | 31.99 MiB | -3.29% |
| Native ExponentialHistogram p95 range | 341.67 ms | 315.23 ms | -7.74% | 269.98 ms | 241.05 ms | -10.72% | 35.16 MiB | 34.07 MiB | -3.09% |

The broad gate required at least a 5% improvement in both cold and warm
latency and in process RSS. Controls allowed at most a 3% regression when the
absolute latency increase was at least 1 ms, or when the RSS increase was at
least 16 MiB. The equality warm percentage looks large only because its
absolute increase was 0.578 ms; the cold increase was 0.761 ms. The scalar
instant row remained below the 3% latency ceiling. Its 11,352 KiB RSS increase
was below the predeclared 16 MiB materiality floor. These exceptions passed
the gate but remain useful small-query watch points.

The result is not merely an RSS trade. Broad, sparse-regex, negative-matcher,
and all four native range controls improved end-to-end latency. The selective
scalar range was neutral, consistent with Phase 1 evidence that its repeated
storage planning dominates label ownership. The compact arena has fixed and
hashing overhead on small result sets, which explains why promotion retains a
same-binary owned comparator and reports controls rather than hiding them.

## Final-binary profiler evidence

A separate supporting profile reran the broad selector with the exact accepted
binary after the promotion gate. The artifact is:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase2-compact-ids-profile-final-20260721-181419
```

The `perf stat` arm used two counterbalanced four-process blocks, one query
evaluation per fresh process, pread, and zero `fincore` corpus residency after
eviction. Exact/portable fingerprints, result cardinality, `QueryStats`, and
payload counters matched in all eight processes. Medians moved as follows:

| Counter | OwnedStrings | CompactIds | Delta |
| --- | ---: | ---: | ---: |
| Task clock | 5,257.02 ms | 4,438.14 ms | -15.58% |
| Cycles | 29.332 billion | 24.806 billion | -15.43% |
| Instructions | 67.765 billion | 52.891 billion | -21.95% |

Heaptrack then ran one isolated process per policy as mechanism evidence, not
as another latency gate. The principal Rust allocation path recorded
38,870,810 allocation calls and 561.21M peak consumption for `OwnedStrings`,
versus 3,316,260 calls and 243.13M for `CompactIds`: reductions of 91.47% and
56.68%. Both profiled queries produced the same fingerprints, result shape,
`QueryStats`, and payload accounting. This supports the expected mechanism;
the larger counterbalanced query matrix remains the promotion authority.

## Correctness and artifact integrity

The v2 result gate passed with no failures. For every query and run kind it
required exact equivalence of:

- exact and portable semantic fingerprints;
- result series and sample counts;
- every public `QueryStats` field;
- logical and physical payload counters;
- label materialization, omission, and integrity counters;
- range-scalar-cache counters; and
- all non-exempt metadata and symbol counters.

Four policy-sensitive diagnostic fields were named before measurement. Only
metadata-cache hits actually differed: generation-bound source-symbol
translations change repeated metadata-cache use. Logical symbol returns,
symbol-page cache hits, and symbol validation time happened to match. No
semantic or payload-I/O difference was waived.

Footer validation was a separate untimed pass over all eight segments. It
passed over 154,902,724 datapoints, 17,286,077 series/chunks, and 3,336,298,511
chunk bytes. The intentionally independent readback oracle then executed all
38 expected cases with zero mismatches, zero skips, and zero isolation skips.
Neither full-file footer reads nor oracle work is included in query timings.

The complete before/after inventory was byte-identical: the same 66 regular
files, 5,569,314,896 bytes, file list, per-file contents, and corpus inventory
hash. The artifact checksum manifest contains 2,137 entries, and every entry
verifies. It covers the comparison, validation, inventory, schedule, summary,
and all timed run products, but intentionally excludes `metadata/`, the
manifest itself, and `COMPLETE`. Separate binary, harness, and eviction-helper
checksum manifests verify; other captured metadata is not covered by one
aggregate tamper-evident manifest. The final gate JSON SHA-256 is
`b381035de749012bf6beb02b7ecf020bdcdccc1a6a0cc07eb287284ac4c3f9b5`.

Focused implementation coverage additionally exercises Schema 7 and Schema 8
owned/compact equivalence, generation binding, selective omitted-label
corruption, touched-page corruption precedence, hash collisions, explicit
empty values, typed metric-name projection, budget refusal without fallback,
policy freezing, result lifetime after session drop, charge reconciliation,
and API/fingerprint traversal without compatibility materialization.

## Promotion and compatibility decision

`CompactIds` is promoted with these defaults and boundaries:

- a non-empty native Schema 7 or Schema 8 `SegmentStoreQuerySession` opens in
  `CompactIds` mode, including sessions used by the HTTP API;
- `chronoxide-query` defaults to `--query-label-storage compact-ids` and keeps
  `--query-label-storage owned-strings` as the authoritative comparator;
- `SharedAtoms` remains an explicit historical comparator and is not a
  fallback;
- core Schema 6 sessions stay on `OwnedStrings`, because their validated
  adapter does not expose generation-bound encoded labels, and an explicit
  compact request is rejected. The benchmark CLI's flag default is
  `compact-ids` independent of its explicitly selected layout, so a legacy
  `--storage-layout schema6-ab` comparator invocation must also pass
  `--query-label-storage owned-strings`;
- an empty store and the low-level standalone policy/interner default remain
  owned, because there is no native store generation to bind; and
- the storage policy and arena budget freeze on the first query, prewarm, or
  prefetch attempt so one session cannot mix representations.

The 512 MiB budget leaves substantial modeled headroom for the measured broad
query, but exceeding it is intentionally a visible query error. Operators
should not interpret it as a transparent spill threshold. A larger corpus or
different label-cardinality distribution still needs its own bounded-RSS and
latency evidence.

## Storage-format disposition

No segment, component, footer, symbol, series, postings, chunk-index, or
payload bytes changed. No writer path, version number, checksum rule,
corruption rule, replay requirement, or migration policy changed. Schema 7
and Schema 8 retain their existing segment-local on-disk symbol IDs; the new
IDs exist only inside one query arena and are deliberately unrelated to the
stored ordinals.

Consequently this phase does not open a new storage-version boundary and does
not require a normative `storage.md` layout update. The byte-identical corpus
inventory is direct evidence for that scope. Any future attempt to persist or
share these query-local IDs across sessions, processes, or segments would be a
different design and would require a new authority and versioning review.

The CLI benchmark excludes HTTP JSON serialization latency, although direct
compact API serialization is correctness-tested. The accepted claim is
therefore the measured query-execution/RSS improvement plus preservation of
the wire result, not an unmeasured HTTP end-to-end latency percentage.
