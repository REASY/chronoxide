# Delayed and Selective Label Materialization Design

- **Date:** 2026-07-15
- **Status:** Scalar phase 1 and pure native Histogram/ExponentialHistogram
  terminal `count`/`group` demand are enabled by default with an explicit
  full-label A/B policy; broader propagation remains ongoing
- **Normative storage format:** [storage.md](storage.md)
- **Related review:**
  [storage read-layout review](2026-07-13-storage-read-layout-review.md)

## Decision

Chronoxide will add an internal label-demand path for terminal PromQL
aggregations. A query that can prove it needs only a fixed set of grouping
labels may carry those labels, plus a private verified source identity, through
selection and range evaluation. It need not allocate every source label as an
owned `String` merely to discard most of them at the aggregation boundary.

This is a query-execution optimization, not an on-disk format change. Normal
query sessions use demand-driven ownership for schema-7/schema-8 terminal
scalar aggregations and the proved pure native Histogram/ExponentialHistogram
`count`/`group` shapes, and retain an explicit `Full` policy for one-binary
A/B. Schema 6 remains a full-materialization comparator. Direct store queries,
head-inclusive queries, and public raw-selection APIs continue to request
complete labels.

Selective materialization never means selective integrity checking. Every
touched series row and every referenced symbol remains fully decoded, resolved,
validated, and integrity-checked before the row can affect matching, merging,
or query output.

## Goals

- Avoid owned label-name and label-value allocation for labels that a proven
  terminal aggregation cannot observe.
- Keep the complete, integrity-checked source-series identity through
  selection, cross-segment merging, and range evaluation.
- Preserve all PromQL label, matching, grouping, stale-marker, reset, typed
  histogram, and result-identity semantics.
- Prevent a selective result from escaping through a public storage API,
  fingerprint, HTTP response, or non-terminal execution path.
- Keep full-label and selective-label caches isolated.
- Measure integrity-check work separately from materialized output-label work.
- Provide one-binary, runtime-selectable `Full`-versus-`DemandDriven` A/B
  coverage.

## Non-goals

- Changing `series.bin`, symbols, postings, chunk indexes, payloads, or segment
  footer versions.
- Skipping validation of labels that are absent from the final aggregation
  group.
- Making the public `SegmentSelector` contract partial or demand-aware.
- General lazy labels for arbitrary PromQL expressions in phase 1.
- Optimizing label-mutating functions, vector matching, rank aggregations, or
  virtual classic-histogram/summary projections in phase 1.
- Redefining `SeriesId` or treating a hash alone as proof that two complete
  label sets are equal.

## Required invariants

The implementation must preserve all of the following:

1. A touched malformed keyset, series block, encoded label pair, symbol page,
   symbol ID, ordering constraint, checksum, or stored identity is corruption.
   It is never a matcher miss, pruning decision, cache miss, or empty result.
2. Missing-label and explicit-empty-label matching retain PromQL's `""`
   matcher semantics while preserving label presence for grouping and output.
3. `__name__` is an ordinary grouping label only when the child operator
   semantically retains it. Ordinary range functions such as `rate()` and
   `increase()` drop it; `last_over_time()` retains it.
4. Cross-segment merge identity remains the stored `SeriesId` after complete
   integrity checking, following the existing store-wide identity contract;
   it is never recomputed from the selectively visible group labels.
5. Typed OTLP temporality, start times, reset hints, flags, stale markers, and
   native Histogram/ExponentialHistogram values are unchanged.
6. A selective result is incomplete by construction. Only the terminal
   aggregation may convert it to a complete public result.
7. Unknown or unsupported expression shapes receive `Full` label demand during
   planning. The planner must never retry after partial execution or silently
   degrade semantics.

## Internal demand contract

The initial demand model is deliberately small:

```rust
#[derive(Clone, Debug, Default, Eq, PartialEq)]
enum QueryLabelDemand {
    Full,
    Include {
        names: Arc<[String]>,
        derive_metric_name_dropped_identity: bool,
    },
}
```

`Include` names are normalized once: sort by raw UTF-8 bytes, remove exact
duplicates, and retain label presence rather than substituting missing labels
with allocated empty values. Label-less `All` grouping is enabled; its output
demand is empty before matcher names and `__name__` are added.
The derived-identity flag is set only for the whitelisted `rate()` and
`increase()` children; a direct terminal aggregation does not pay for an
unused second hash.

This type is private to query execution. `SegmentSelector` may carry it in a
crate-private field, but public selector constructors and raw-selector APIs
always produce `Full`. The demand is produced only after the lowering layer
has recognized a supported terminal aggregation and proved that no downstream
consumer can observe other labels.

A demand has two distinct parts during storage selection:

- **matching demand:** every label needed by equality, inequality, regex,
  negative-regex, metric-name, and projection-name matchers;
- **output demand:** labels that must remain visible to the terminal
  aggregation.

Phase 1 materializes the normalized union of matching demand, output demand,
and `__name__`; matcher evaluation still runs after the integrity-checked
borrowed row pass. Moving matcher evaluation into that pass so matcher-only
strings need not be retained is a follow-up optimization. The reader still
inspects and integrity-checks every other canonical pair.

## Demand derivation

Phase 1 derives `Include` only for an explicitly whitelisted terminal
aggregation with `by(...)` or label-less `All` grouping. The initial candidate
operators are:

- `sum`, `count`, `avg`, `min`, `max`, `stddev`, `stdvar`, `group`, and
  `quantile`.

The lowering pass must propagate demand through the child operator's label
semantics. It cannot blindly pass the aggregation's group-name list to the
storage selector. In particular, a requested `__name__` disappears through
ordinary range functions and remains through `last_over_time()`.

An implementation may initially enable a narrower subset of the whitelist.
Every combination not enabled explicitly must return `Full`.

The implemented scalar child shapes are:

- a direct scalar instant selector;
- scalar `rate()` and `increase()` over one range selector.

The implemented native child shapes are root `count` and `group`, with `All`
or `by(...)` grouping, over either a direct native selector or native
`rate()`/`increase()` over one range selector. Schema-7/schema-8 rows are
selective only when their integrity-checked kind mask is wholly contained in
the exact allowed family for that reader: Float/Int64 for scalar execution,
Histogram for the Histogram reader, or ExponentialHistogram for the
ExponentialHistogram reader. Schema 6, Summary, every mixed-kind row, and all
other native expressions remain fully materialized.

Nested aggregations and arbitrary function composition remain full until a
separate proof covers their intermediate-label behavior.

## Full integrity-checking contract

For a cold schema-7/schema-8 series row, selective execution must perform the
same integrity-checking work as full execution:

1. Decode and validate the keyset reference and complete encoded canonical
   `(key_sym, value_sym)` row.
2. Validate all counts, bounds, block references, key order, dictionary/value
   codes, and every referenced symbol ID.
3. Resolve every canonical key and value to its full byte string.
4. Hash the complete canonical byte stream in canonical order and compare it
   with the stored `SeriesId`.
5. Complete all matcher evaluation, including matchers on labels outside the
   output demand and PromQL missing-label behavior.
6. Only after complete integrity checking may the row be exposed for chunk
   routing, source merging, or cache reuse.

There is no matcher short circuit before step 4. Corruption in an omitted
label must still fail a query even when another matcher would reject the row.
The existing sticky corruption behavior applies to page and metadata failures.

CRC granularity is a physical page. Loading any byte from a hot, cold, or
symbol page therefore validates the checksum over that complete touched page;
the reader does not decode unrelated rows and does not read untouched pages.
A page already pinned in an integrity-checked generation cache may be reused
without issuing another physical read.

The implementation may combine symbol resolution, full-identity hashing,
matcher evaluation, and selective copying in one borrowed-row pass. It may not
avoid resolving a symbol merely because that label is neither matched nor
retained.

Projection-name matchers must evaluate the effective PromQL series name, not
only the stored OTLP metric name. Missing versus explicit-empty presence must
remain distinguishable after the borrowed pass.

## Transient source identity

`SegmentQueryResult.series_id` initially serves as the established
cross-segment source identity. Hashing only selected labels would collapse
distinct source series before aggregation, so phase 1 retains the complete,
integrity-checked stored `SeriesId` through selection and the pre-range
cross-segment merge. A crate-private `labels_complete` marker makes that
transient state explicit.

`rate()` and `increase()` then remove `__name__` on the established full-label
path and recompute the result identity from every remaining source label. That
identity controls the following merge order, which can affect the exact low
bits of floating-point aggregation. For those two selective shapes, the
borrowed-row pass therefore computes a second, private
`metric_name_dropped_series_id` from every integrity-checked canonical pair
except `__name__`. It does not compute this identity from the selected subset,
and it does not replace full-row integrity checking: the complete stored
`SeriesId` must still match first. Direct terminal aggregations do not compute
the otherwise unused second hash.

After the range function, `series_id` becomes that derived identity exactly as
it does on the full-label path. The labels remain incomplete until the terminal
aggregation emits a new complete public label set and canonical result ID.

This follows the current store-wide identity contract, which already uses
`SeriesId` for cross-segment merging. Phase 1 carries no separate complete
label evidence and claims no stronger collision resistance than the existing
full-label path. A future collision-resolving canonical-label interner can
strengthen both paths independently.

The transient state must:

- merge samples across segments and head storage by verified complete source
  identity;
- preserve the full source identity through pre-range merging, then use the
  exact full-path metric-name-dropped identity for whitelisted scalar range
  evaluation;
- keep demanded labels separate from the complete identity evidence;
- never merge on the selectively visible group labels; and
- be consumable only by the terminal aggregation node that requested it.

The terminal aggregation constructs a complete group-label result and computes
its normal canonical public `SeriesId`. No incomplete marker or source identity
appears in the public result.

Virtual `_count`, `_sum`, classic bucket, and summary quantile projections can
change the complete source identity. They remain `Full` in phase 1. A later
slice may support `_count`/`_sum` by computing exact projected identity during
the full borrowed-row pass. Bucket `le` and summary `quantile` labels depend on
decoded payload values and require their own design.

## Query flow

The selective path is internal and end-to-end:

1. `PromqlAggregation` recognizes a supported terminal expression and derives
   normalized child demand.
2. Session execution selects a specialized deferred-aggregation path for the
   supported terminal shape. Both `Full` and `DemandDriven` use this path; the
   policy changes only the selectors' ownership demand. Ordinary vector/range
   selection remains unchanged.
3. Lowering forwards demand through only whitelisted child operators.
4. Session-owned sealed-segment readers perform full integrity checking on each
   candidate, evaluate all matchers, retain the demanded union, and attach the
   verified stored source identity. Pure Histogram and ExponentialHistogram
   rows use the same verifier only for the root native `count`/`group`
   whitelist. Direct-store and head-inclusive execution remain full.
5. The pre-range cross-segment merge keys by verified full source identity,
   not demanded labels. `rate()`/`increase()` then use the separately derived
   full-path identity with `__name__` removed.
6. The terminal aggregation groups by demanded labels and emits ordinary,
   complete query results.

No public query path may expose a result whose private completeness marker is
false. The phase-1 executor checks that terminal aggregation has restored
complete results, and focused tests run a raw query after a selective query to
prove that session caches were not poisoned. A separate type-state wrapper is
still preferable if the selective whitelist expands beyond this tightly
bounded terminal path.

Query sessions use demand-driven ownership by default. An explicit `Full`
policy forces the same specialized terminal-aggregation execution flow to own
every label for one-binary semantic and performance A/B. Head-inclusive and
direct raw-selection paths continue to request `Full` in phase 1.

## Cache isolation and governance

The existing session `SeriesLabelCache` stores complete `QueryLabels`. It must
remain a full-label cache:

- never insert partial labels into it;
- never satisfy `Full` from a selective cache entry;
- never reinterpret a full-cache miss as permission to skip integrity checking;
  and
- when it hits, it may safely filter the complete labels into a demanded
  result after the current touched row has still been integrity-checked.

Phase 1 does not retain selective labels in any cache: each touched segment
owns only the requested subset for the current plan. This prevents poisoning
the session-wide full cache, at the cost of repeated selected-label allocation
when the same source series spans segments. A later execution-local selective
cache must key by both verified source identity and normalized demand;
different `by(...)` lists cannot alias.

Every touched segment row is fully integrity-checked before a selective-cache
hit may be used. A cache hit saves repeated owned-string materialization; it
does not suppress metadata validation or corruption propagation.

A later selective cache must scope entries to one query/query-range execution
and charge them to that execution's aggregate byte governor. It must not create
an unbounded per-segment cache or retain one cache budget per touched segment.
That follow-up must record current and peak bytes for complete-label identity
evidence and demanded-label cache separately.

Head storage already owns complete labels. It may filter those into the same
transient representation, which avoids result-label clones but does not reduce
the head index's resident memory.

## Query-session shared label atoms experiment

Schema-7/schema-8 query sessions may retain materialized source label names
and values as session-local `Arc<str>` atoms. Equal UTF-8 content maps to one
atom for the lifetime of the query session; each `QueryLabels` value owns its
atom references, so returned results remain valid after the session is
dropped. Canonical order, content equality, ordering, public serialization,
and fingerprints continue to operate on string bytes rather than pointer
identity.

This is a code-only runtime experiment. `OwnedStrings` remains the default;
the hardened real-corpus A/B found that `SharedAtoms` saves substantial RSS but
regresses the high-cardinality full-label selector. `SharedAtoms` therefore
remains an explicit candidate only. Both are selected using one release
binary. The representation is frozen before the first query, prewarm, or
prefetch attempt, including an empty result, parser failure, bounds failure,
or storage/integrity error. The policy is never changed or retried after that
boundary. Full and selective label-demand caches remain isolated exactly as
described above; atom sharing changes neither label demand nor the complete-row
integrity-checking contract.

Internal query paths iterate shared atoms as borrowed `&str` pairs. The
existing public owned-slice view remains available as a lazy compatibility
view, but benchmark-internal fingerprint, grouping, matching, and raw scalar
decode paths must not initialize it. Label mutation and final synthetic output
may construct owned strings and then enter the same session interner at the
public result boundary. Operators that preserve a label set exactly must move
the existing representation instead: `last_over_time()` does so and retains
the established series identity without a final atom lookup pass.

The first experiment deliberately does not change the governed schema-7/8
verifier's output type. That verifier still creates short-lived `String`
values before the facade moves them into the session interner. Therefore this
slice tests retained duplication and downstream ownership, not fully
allocation-free symbol resolution. A direct governed borrowed-symbol-to-atom
visitor is a separate follow-up and must preserve the same accounting and
integrity-error precedence.

## Schema behavior

### Schema 7 and schema 8

Paged-symbol readers use the full integrity-checking borrowed-row pass above.
Adaptive postings in schema 8 change candidate discovery only; they do not
weaken series-row or symbol integrity checking.

### Schema 6 A/B baseline

Schema 6 must remain semantically equivalent in the A/B harness. Its reader
may materialize complete labels and then filter to the demand, so no selective
allocation win is claimed for that path. It must still attach verified
complete source identity and obey the same transient/public boundary.

Schema-specific behavior must not change semantic fingerprints, ordinary
`QueryStats`, or corruption results.

## Queries requiring full label ownership

Phase 1 uses `Full` for:

- every public/raw selector and every query whose result exposes source labels;
- `without(...)`;
- `topk` and `bottomk`, which return original labels and use labels for stable
  tie behavior;
- `sort` and `sort_desc`;
- `label_replace` and `label_join`;
- binary and set operators, including default matching, `on`, `ignoring`,
  `group_left`, `group_right`, included labels, and duplicate-match errors;
- arbitrary instant-vector or unary functions unless that exact function has
  a proved demand-forwarding rule;
- `absent` and `absent_over_time` until their derived-label rules are covered;
- virtual HistogramBucket, SummaryQuantile, `_count`, and `_sum` projection
  modes. `AllPromql` may select labels only for rows whose integrity-checked
  kind mask is entirely Float/Int64; typed or mixed-kind rows remain full;
- nested or multi-consumer subexpressions;
- native `sum`/`avg`, native scalar functions, `changes`/`resets`, Summary,
  and any typed scalar/native combination not explicitly enabled;
- every row with a mixed scalar/typed or mixed typed kind mask; and
- any planning uncertainty, unsupported modifier, or internal type mismatch.

`Full` demand is selected during planning, before storage execution. It is a
semantic ownership requirement, not error recovery. A query must never retry
as `Full` after observing corruption or partial results.

## Diagnostics and counters

Add dedicated diagnostic/profile fields rather than changing the semantics of
existing `QueryStats`. Phase 1 records integrity-checked rows/pairs, full versus
selective rows, materialized pairs, omitted pairs, and materialized
string-content bytes in `SegmentStoreQueryProfile`. Benchmark raw schema v8 and
the Markdown report expose these counters without changing ordinary
`QueryStats`.

The following are hardening follow-ups and are not yet implemented:

- matcher-only label pairs and bytes inspected;
- integrity-checked and omitted string-content bytes;
- full-cache filter hits;
- selective-cache hits and misses;
- complete-identity interner hits, misses, current bytes, and peak bytes;
- selective-label cache current bytes and peak bytes; and
- demand-decision counts by stable reason code.

The shared-atom experiment reports label sets, atom lookups, hits, misses, and
unique UTF-8 content bytes in the human-readable benchmark report and raw-v9
machine output. Raw-v8 remains immutable; raw-v9 adds the selected
`query_label_storage` policy and the same per-run counters. These are
experimental diagnostics and do not change ordinary `QueryStats`.

Profile elapsed time separately for full row/identity integrity checking,
matcher evaluation, complete-identity interning, demanded-label allocation,
source merge, grouping/hash construction, and final result construction.
Timers are diagnostics, not semantic query counters.

Existing matched-series, projected-series, chunk-read, payload-byte, sample,
and logical-work counters must match between full and selective modes unless a
separately reviewed change names the difference. A cache hit must not make
integrity-check counters lie about touched rows.

## Required tests

### Demand and lowering unit tests

- Normalize empty, duplicate, differently ordered, non-ASCII, empty-string,
  and `__name__` include lists deterministically.
- Prove the exact operator/child whitelist and `Full` demand for every other
  expression shape.
- Cover `count_values` as an explicit full-label demand.
- Cover metric-name dropping through `rate()`/`increase()` and retention
  through `last_over_time()`.

### Reader integrity-checking and corruption tests

For schema 7 and schema 8, repeat focused selective queries after corrupting:

- an omitted key symbol;
- an omitted value symbol;
- an omitted dictionary/value code;
- a keyset or series-block bound/count/order field;
- the stored full-series identity;
- a cold symbol-page checksum; and
- data reached after cache eviction/retry.

Every case must return the same corruption class as full materialization and
retain sticky-error behavior. Add a case where a different matcher would reject
the row to prove there is no pre-integrity-check matcher short circuit.

### Semantic integration tests

- Each supported aggregation over direct scalar selection.
- `sum by (service_name) (rate(...))` and `increase(...)` across segment
  boundaries.
- Missing versus explicitly empty grouping labels.
- `by(__name__)` over direct selection, `rate()`, and `last_over_time()`.
- Equality, inequality, regex, negative-regex, and empty-string matchers on a
  label omitted from output.
- The same logical series with different segment-local symbol IDs.
- Distinct complete label sets with identical demanded labels, proving they
  merge only at the terminal group.
- Preserve the established full-path `SeriesId` merge behavior without
  claiming a stronger collision guarantee for the selective path.
- Sealed plus head data and out-of-order precedence.
- Query-range execution across repeated steps.
- Native Histogram and ExponentialHistogram direct/rate terminal `count` and
  `group` queries proving Full-versus-demand equivalence, plus direct native
  output and unsupported-operation controls proving their full-label demand.
- Mixed scalar/native `count` and `group`, including mixed-kind rows proving
  complete-label fallback.
- Sequential session queries: selective, raw full, different demand, then full
  again, proving cache isolation.

Add explicit full-demand tests for raw selectors, `without`, `topk`/`bottomk`,
label functions, binary/set operators and modifiers, sort, absent functions,
nested expressions, and all virtual projection modes. Full-demand results must
match the pre-change path byte-for-byte at the public boundary.

Run the independent query-readback oracle for supported isolation-safe cases
and inspect executed/skipped diagnostics. Run the Prometheus golden suite with
`promtool` for affected aggregation, range, name, and matcher semantics when
available.

### Schema 6 tests

Run the same semantic and cache-isolation suite against the schema-6 A/B
corpus. Its full-then-filter implementation must produce the same public
result and ordinary `QueryStats` as schema 7/schema 8.

## Default-policy A/B acceptance criteria

These are continuing hardening gates for the default demand-driven policy.
Demand-decision reason codes, richer timing, and cache/interner accounting
remain pending instrumentation.

The benchmark uses one identical release binary with a runtime policy switch
between `Full` and `DemandDriven`. It must use the same fingerprinted corpus,
host, query schedule, limits, writer/query configuration, and explicit cache
budgets. Do not overlap builds, replay, footer validation, profilers, or other
known measured workloads.

For every query:

- exact and portable semantic fingerprints match;
- result series/sample counts and values match;
- complete ordinary `QueryStats` match;
- demand decision is recorded and agrees with the expression whitelist; and
- selective counters reconcile: integrity checking remains complete while
  owned materialized label pairs/bytes fall only for eligible queries.

The hardened query set must include direct aggregation, scalar range
aggregation, eligible native Histogram and ExponentialHistogram terminal
aggregation, native full-demand controls, omitted label matchers, `__name__`,
empty/missing labels, high-cardinality grouping, no-result and small-result
cases, query-range execution, and all mandatory full-demand families.

Report cold and warm latency, CPU profile, peak RSS, complete-identity and
future selective-cache charges, logical payload-used bytes, coalesced
payload-read bytes, and read/used amplification. A CLI `cold` run is only the first
expression in a fresh query session; operating-system page-cache state must be
reported separately.

Use at least 20 alternating repetitions on a quiet host, or report confidence
intervals and environmental noise when that is not possible. Further whitelist
expansion requires repeatable latency/CPU improvement on eligible real-corpus queries,
no material regression on full-demand queries, and a demonstrated reduction in
owned materialized label bytes. A single noisy run or reduced counters without
latency evidence is not sufficient.

## Implementation sequence

1. Add normalized `QueryLabelDemand`, whitelist derivation, and exhaustive
   full-demand tests without changing reader behavior.
2. Add the private transient result and verified complete-source-identity
   representation; route it only to terminal aggregation.
3. Implement schema-7/schema-8 borrowed full integrity checking plus selective
   output copying without inserting partial labels into persistent caches.
4. Add query-session scalar direct/range support, make `DemandDriven` the
   normal policy, and retain an explicit `Full` same-binary A/B control.
   Direct-store and head-inclusive execution remain full in phase 1.
5. Add pure native Histogram/ExponentialHistogram root `count`/`group` paths
   after typed equivalence tests pass. Completed for direct selectors and
   `rate()`/`increase()` children; mixed-kind rows deliberately remain full.
6. Continue focused corruption, readback, Prometheus, workspace, and hardened
   real-corpus A/B gates before expanding the demand-propagation whitelist.

Any later expansion of the whitelist must document the operator's exact label
transform, source-identity behavior, cache interaction, and new oracle cases.
