# Versioned Immutable Live Query View

**Status:** Accepted after three review iterations; opt-in correctness
implementation verified; incremental roots plus the proof-gated empty-final
and one-active-partition publication fast paths landed. Live serving remains
experimental and disabled by default; the naturally quiet 250k owner-fast-path
gate and formal 4M evaluation remain pending.

**Performance evidence:**
`docs/experiments/storage_vnext/2026-07-25-live-query-ingest-screen-results.md`

**Date:** 2026-07-25

**Scope:** in-process Prometheus reads over manifest-published segments plus
mutable ingester head data

## 1. Purpose

Chronoxide currently has two relevant capabilities that are not connected:

- the ingester owns mutable, partition-local `HeadBuffer`s and seals them into
  immutable segments; and
- `chronoxide-api` opens a fixed `SegmentStoreReader` and serves only that
  sealed inventory.

The core query engine can merge one borrowed head with sealed data, but the
running HTTP API cannot safely borrow the ingester's mutable state. The
standalone API is a different process, so a Rust lock cannot make that heap
visible to it.

This design adds an optional API embedded in the ingester. It publishes
immutable, versioned read views. Each request pins exactly one view containing
both:

1. one manifest-derived sealed-store inventory; and
2. all head fragments that are not represented by that inventory.

The result is a coherent request-level snapshot without holding an ingestion
lock for the duration of a query.

## 2. Authority

This document defines the live-view ownership, synchronization, publication,
and seal-handoff contract. It does not change the Schema 8 byte layout,
event-time routing, typed OTLP semantics, PromQL semantics, or manifest
publication rules in:

- `docs/superpowers/specs/storage.md`;
- `docs/superpowers/specs/clock.md`; and
- `docs/promql-coverage.md`.

If this document conflicts with those normative semantics, those documents
win. The implementation must update `storage.md` with the resulting live-read
contract, but no on-disk format version changes.

## 3. Goals

The implementation must:

- make accepted in-memory samples queryable through the Prometheus instant and
  range endpoints while ingestion continues;
- give one request one immutable `(sealed inventory, head)` generation;
- avoid a gap or duplicate logical sample while a head range moves to a
  manifest-published segment;
- preserve exact stale-NaN, ordinary NaN/Inf, start-time, temporality, flags,
  reset-hint, native histogram, exponential histogram, summary, OOO, and
  last-write-wins behavior;
- keep the request path free of writer locks after it pins a view;
- keep ingestion free of query-duration locks;
- transfer encoded head storage into immutable fragments instead of cloning
  the complete mutable head;
- bound root lookup fanout while structurally sharing immutable leaf runs;
- preserve existing sealed-only API behavior; and
- expose freshness and publication failures rather than silently serving an
  indefinitely stale live view.

## 4. Non-goals

This change does not:

- share an in-process head with a standalone `chronoxide-api` process;
- add cross-process shared memory or a head RPC protocol;
- change Kafka ordering, capture/replay ordering, event-time policy, WAL, or
  recovery;
- promise that a just-arrived message is visible before its publication
  boundary;
- make a query linearizable with an individual datapoint inside an OTLP
  message;
- change segment bytes, segment IDs, footer validation, or manifest authority;
- make OS page-cache state part of a view generation; or
- turn an unvalidated directory discovered on disk into queryable data.

## 5. Terms

**Message boundary**

The point after `process_message` has reinserted its temporarily removed
partition head. If ingestion accepted a prefix and then returned an error, the
boundary is still after reinsertion and includes that accepted prefix.

**Head fragment**

An immutable `HeadWindow` containing samples accepted since the preceding
publication for one `(source partition, aligned event-time range, lane)`.

**Head read view**

A generation cut over compact catalog rows and structurally shared sample-run
descriptors that reference immutable frozen payload pages.

**Live query view**

One immutable root containing a generation number, sealed reader, head read
view, publication metadata, and health metadata.

**Published**

Reachable through the current live-view root. This is distinct from a segment
being manifest-published.

**Lane**

The in-order lane or OOO lane already defined by the storage specification.
Pre-seal OOO is co-sealed into the normal segment. OOO for an already sealed
range is written to the overlapping OOO segment lane.

## 6. External behavior

### 6.1 Deployment modes

The existing `chronoxide-api` binary remains a sealed-only process and retains
its current CLI and behavior.

An optional HTTP server may run inside `chronoxide-ingester`. Only this
embedded mode exposes mutable head data. It uses the same Prometheus response
format, query limits, read configuration, concurrency admission, and error
mapping as the sealed-only router.

Live serving requires a configured segment writer. Configuration that enables
the embedded API without a head or segment writer is rejected at startup.
Publication activation is startup-only. It rejects a processor with a
non-empty head, active message, queued completed coverage, or any prior
completed message sequence, whether the label store is still the ordinary
FlatInterned store or has already been upgraded to its versioned facade. A
pristine coverage-only/versioned processor may still upgrade atomically.

### 6.2 Visibility

A successful live query sees all data in its pinned generation and no data
from a later generation. Publication occurs only at a message boundary.
Therefore a request can see either all accepted datapoints from one OTLP
message or none of them; it must not see a partially processed message.

The default target publication interval is one second. A message boundary
publishes when:

- no usable view exists yet;
- the target interval has elapsed and visible state changed;
- a segment was manifest-published;
- shutdown begins; or
- a test or administrative caller explicitly forces publication.

An interval is a coalescing target, not an event-time rule. No wall-clock value
changes sample placement or sealing.

### 6.3 Request pinning

The HTTP path acquires its concurrency permit first, then loads one
`Arc<LiveQueryView>`. Queue time therefore does not pin an obsolete view or
retain its memory.

The request uses that same `Arc` for parsing, all instant/range evaluation
steps, sealed reads, head reads, and response construction. It never reloads
the current pointer mid-request.

The response includes:

- `x-chronoxide-view-generation`;
- `x-chronoxide-view-age-ms`;
- `x-chronoxide-visible-message-sequence`;
- `x-chronoxide-catalog-revision`;
- `x-chronoxide-view-pin-wait-ns`;
- `x-chronoxide-view-pin-held-ns`; and
- the query timing, complete `QueryStats`, and success-only compact query-I/O
  headers.

Those live-only values come from the same retained pin. Pin wait and hold are
the root read-lock acquisition and critical section only; queueing,
query-retention admission charging, evaluation, and serialization are
excluded. The compact query-I/O header is captured from the same query session
after successful evaluation and reports logical payload-used bytes,
physical/coalesced payload-read bytes and read count, series-entry bytes,
chunk-index-range bytes, and exact-postings bytes without enabling detailed
stage timing.

### 6.4 Readiness and freshness

Health means that the process is running. Readiness for the embedded live API
requires:

- at least one successfully published view;
- no unresolved publication error; and
- view age no greater than the configured maximum stale duration while
  ingestion has unpublished changes.

When live readiness is false, `/-/ready` returns `503`. Query endpoints also
return `503` rather than presenting a stale view as current. A quiescent
ingester with no unpublished change does not become stale merely because no
new message arrived.

`DirtySince(t)` remains queryable from the current coherent root until
`t + max_view_staleness`; the generation/age headers make its cut explicit.
`Failed` and an expired dirty deadline reject new queries immediately. If the
retained-generation admission governor cannot admit any new live query,
queries return `503` with an explicit resource-pressure error and readiness is
false until pressure clears. This does not invalidate already pinned queries.

The default maximum stale duration is ten times the publication interval,
with a minimum of ten seconds.

## 7. Ownership model

### 7.1 Atomic root

The public handle owns one linearizable state:

```text
LiveQueryHandle
  state: RwLock<PublishedLiveState>

PublishedLiveState
  current: Option<Arc<LiveQueryView>>
  readiness: Ready | DirtySince(Instant) | Failed(PublicationError)
  status_epoch: u64
```

The standard-library `RwLock` protects only cloning or replacing one `Arc`.
It also makes the view and its readiness epoch one indivisible snapshot; a
separate set of status atomics is forbidden because it could tear readiness
from the root. No disk access, index construction, head encoding/decoding,
PromQL evaluation, or serialization occurs while that lock is held. Poisoning
is surfaced as an internal/readiness error; it is not treated as an empty
store. A request that pinned a ready generation may finish if a later
generation fails; requests admitted after the failure receive `503`.

The `LiveQueryView` payload and logical cuts are immutable after construction.
Its publication-age anchor is finalized exactly once by the successful commit,
after validation and immediately before the root swap, while the candidate is
still unreachable through the handle:

```text
LiveQueryView
  generation: u64
  published_at: one-shot commit-finalized monotonic age anchor
  sealed: Arc<SegmentStoreReader>
  head: Arc<HeadReadView>
  manifest_cut:
    Absent
    | Present(manifest identity + validated byte offset
              + SHA-256 of the complete validated prefix)
  visible_message_sequence: u64
  catalog_revision: exclusive u64 row revision
```

`ManifestCut::Absent` is valid only for a proven-empty initial state:
`CURRENT`, `CURRENT.tmp`, `MANIFEST-*`, and top-level `seg-*` publication
evidence are all absent. It is the cut used by an initial head-only view.
Unrelated runtime entries are allowed. A malformed, missing-after-publication,
or replaced manifest is an error.

Before the first successful publication, `current` is `None` and readiness is
false. The generation starts at one and increases by exactly one for each
successful root replacement. Overflow is a fatal publication error, not
wraparound.

Read-horizon enforcement remains a known existing API gap, not part of this
first live-view change. The current processor's partition-watermark report uses
the source/Kafka timestamp for diagnostics, which is forbidden as an event-time
fallback. A future completeness change must separately track maximum accepted
datapoint event time, active/idle partitions, and bind that snapshot to this
root before implementing strict/clamped responses. This design must not reuse
the diagnostic source timestamp or claim completeness that it cannot prove.

### 7.2 Writer ownership

Only the ingestion thread mutates:

- `HeadBuffer`;
- the label interner;
- pending frozen fragments and their version descriptors;
- the append-only live series catalog;
- `SegmentWriter`; and
- the next generation under construction.

Query workers receive only immutable persistent catalog/sample roots and
`Arc` payloads. They acquire no writer/catalog/sample lock while planning,
decoding, or evaluating. No `unsafe` implementation of `Send` or `Sync` is
permitted.

### 7.3 Compact frozen payload ownership

At publication, each non-empty mutable window fragment is sealed in memory,
moved out of its `HeadBuffer`, converted to a `FrozenHeadFragment`, wrapped in
`Arc`, and replaced by an empty window with the same range and lane. The
`LastTimestampTable` remains in the writer head.

The current `BlockArena` cannot be placed directly in a frozen fragment: its
first non-empty allocation reserves and zero-fills at least 4 MiB. The
implementation therefore adds:

```text
FrozenBlockArena
  pages: Box<[Box<[u8]>]>

ArenaRead
  slice(BufferRef) -> io::Result<&[u8]>
```

Freezing seals each current builder, copies each arena page's exact used prefix
into a boxed slice, verifies every existing `BufferRef`, and drops the mutable
capacity. Page numbers and offsets do not change. `FrozenBlockArena` has no
allocation or mutation API. Mutable and frozen arenas implement the private
`ArenaRead` decode interface.

Live-mode mutable fragments start with a 16 KiB page. Each subsequent ordinary
page doubles in size (`16 KiB, 32 KiB, ...`) up to the ordinary 4 MiB cap,
which remains the size of later ordinary pages. An individual write larger
than the current geometric target receives one exact-write-sized page without
raising the next geometric target above the 4 MiB cap. This avoids
allocating/zeroing 4 MiB for every small publication. The disabled path
retains the current arena policy exactly: each ordinary page is a fixed 4 MiB,
with the pre-existing exact-write-sized exception for an oversized individual
write.

On the live/adaptive path, sealing reserves page buffers and page-directory
capacity fallibly. The timestamp/value writes for one block form one arena
transaction: failure on either write restores page membership, used offsets,
and the next geometric target while retaining the complete block builder and
window for retry. Freezing also fallibly reserves its page directory and every
exact-used byte copy before the source arena is consumed. The fixed/disabled
path retains its historical infallible sealing hot path. Allocated bytes and
used bytes are separate telemetry and acceptance values.

Publication must not:

- clone the full label store;
- clone the full head series table;
- materialize every typed sample into a second long-lived representation; or
- retain mutable 4 MiB arena pages merely to expose a small used prefix.

Freezing may transiently copy the used encoded bytes once to eliminate arena
slack. The mutable arena is dropped immediately afterward; both copies are
never long-lived. Failure leaves either the original mutable window or a
complete frozen fragment in the publisher's pending queue.

The precise claim is: encoded payload bytes are transferred into compact
immutable pages and shared; mutable tails, selector metadata, and level
descriptors have dedicated frozen forms.

### 7.4 Versioned shared series catalog

The existing `HeadSelectorIndex` is not the live catalog. It owns canonical
labels as `String`s and repeats them in B-trees, postings keys, and label-value
dictionaries. Building it per fragment would multiply the label corpus by the
publication count.

Version 1 live mode requires `FlatInternedLabelSetStore`. Its symbol bytes and
raw identity rows become append-only immutable pages with exclusive revisions;
the writer uses a mutable tail page and old pages are shared by the interner,
publisher, and pinned views. The raw row is byte-for-byte semantic parity with
disabled FlatInterned interning: PromQL name projection must not happen before
`SeriesRef` assignment and must not collapse two raw rows which normalize to
the same PromQL labels. A parallel derived PromQL row, aligned one-for-one by
`SeriesRef` and using the same symbol-ID domain, supplies normalized
metric/label names to the query catalog without owning strings. Segment
metadata and sealing continue to consume the raw row and apply the existing
writer normalization, preserving disabled/live segment bytes. Live mode is
rejected for the Naive and KeySetDictEncoded stores until they provide an
equivalent versioned facade. Disabled FlatInterned mode retains its measured
writer hot path.

`LiveSeriesCatalog` is a read/index facade over those shared rows, not a second
copy. It mirrors every successfully interned label row exactly once and in
dense `SeriesRef` order, including a row whose datapoint is later absent,
rejected by typed encoding, or has a missing number value:

```text
LiveSeriesCatalog
  shared versioned FlatInterned symbols + derived PromQL row pages
  active SeriesRef -> stable series_id
  active (name_id, value_id) -> versioned sorted SeriesRef postings
  active name_id -> versioned distinct value_id dictionary
```

Rows and postings use checked `u32` IDs/refs. Existing rows never change. The
exclusive `catalog_revision` is a `u64` row count: revision zero is empty and
revision N contains exactly refs `0..N`. A catalog transaction rejects a gap,
duplicate, or out-of-order ref. Each row, posting chunk, and first
label-value-dictionary membership also carries its born revision/generation.
`LiveQueryView.catalog_revision` excludes later appends, so a query observes
the catalog as of its generation even though storage is shared. Postings and
value dictionaries filter by that cut. Regex value accounting deduplicates a
compact value ID once per matcher, independent of publication history.

Interning without a stored sample may advance the catalog revision but creates
no sample run, kind guard, or visible query result. A selector only charges a
matched series after the sample store proves time-overlapping presence. This
resolves delayed/lower-ref gaps without exposing a zero-valued sample.

Live postings and stable query IDs are born when a series first has a visible
head run and retire after complete head handoff plus the last historical view
lease. They can be rebuilt if the global `SeriesRef` later becomes active
again. Thus shared identity rows/symbols retain the interner's process-lifetime
semantics, while the additional live-only inverted index is reclaimable.

Label resolution during publication is strict and fallible. A missing row,
out-of-range ref, missing canonical metric identity, allocation failure that
can be represented, or inconsistent existing row fails the candidate. The
current head index's silent skip behavior is not reused. Result-label
materialization remains demand-driven through the query session.

### 7.5 Compact frozen runs and persistent sample roots

`FrozenHeadFragment` does not retain `HeadSeriesTable`, its hash capacity,
boxed builders, or adaptive page directory. Freezing seals builders, consumes
the table, converts entries to compact `FrozenSeriesRun` values referencing the
exact-used frozen arena, sorts once by `(SeriesRef, kind)`, and stores the runs
as a boxed slice. That slice is also the fragment's ordered series directory.
The sample store indexes those runs directly.

`LiveSampleStoreBuilder` constructs a persistent immutable map from
`(PartitionKey { topic, partition }, range, lane, SeriesRef, kind)` to ordered
run roots referencing `Arc<FrozenHeadFragment>` payloads. Catalog and sample
maps use path-copying `Arc` pages/nodes. Active ownership is a
candidate-publication invariant derived from the sample store; it is not
query-visible state and version 1 commits no owner root. A candidate shares
unchanged catalog/sample paths, adds new paths, and simply omits handed-off
paths; it never mutates the root used by generation N. The resulting
`HeadReadView` owns the exact immutable catalog and sample roots for generation
N+1. A numeric partition ID never aliases another topic.

Alongside the sample map, the committed store carries an immutable exact set
of every represented non-empty fragment's full identity and recorded-order
range. Candidate insert and retirement update that certificate only after the
replacement descriptor root is complete. Descriptor/certificate disagreement
or a duplicate fragment identity rejects the candidate. The current
implementation uses an `Arc<BTreeSet<_>>`; its first mutation in a candidate
copy-on-writes the prior set, which must be included in the live performance
and memory-accounting gates.

Before the at-most-one-partition shortcut, the publisher sorts the exact
identities of all non-handed-off pending fragments, rejects duplicates, and
requires exact equality with this certificate. Only that proven-equal identity
set may determine the active partition count.

Pending runs live only in the writer-side candidate builder and are invisible.
Old roots retain removed paths through `Arc`; normal reference counting
reclaims catalog/sample nodes and frozen pages after the final pinned view
drops.

### 7.6 Commit and reclamation mechanics

All rows, postings, descriptor trees, candidate-local owner-validation state,
inventory, and the complete `LiveQueryView` for generation `N+1` are fully
allocated and validated outside the public state lock under a unique
`CandidateToken`. Shared append-only interner pages may be staged earlier, but
only the candidate's immutable page directory/revision can expose them.
Generation N has no pointer to candidate roots.

One immutable `CommitDescriptor` binds the base status epoch, generation,
message/recorded-sample cut, catalog revision, manifest cut, catalog root,
sample root, and sealed inventory. Commit acquires the state write lock,
verifies that the base epoch still matches, and performs one infallible
`Arc<LiveQueryView>` plus readiness/status-epoch replacement. It uses
`mem::replace` to take ownership of the preceding root, releases the state
lock, and only then drops that old `Arc`; final reclamation of a large old view
must never run inside the root-swap critical section. There are no bucket
locks, path inserts, births, retirements, or per-key writes at the linearization
point.

An aborted candidate token is unreachable and reclaimable. If more ingestion
arrives before retry, the publisher builds a replacement candidate by
path-copying from the same committed base and incorporating all pending
batches; it does not expose or duplicate records from the abandoned token.

Every sealed inventory root owns a generation lease used for delayed physical
segment deletion and resource admission. Replacing the current root closes
generation N to new pins but does not expire existing leases. Reclamation
never relies on a timeout or a racy `Arc::strong_count` observation. Tests
expose deterministic lease counts and candidate-drop/reclamation hooks.

## 8. Frozen-window correctness

### 8.1 Per-range kind guard

Moving a fragment out must not forget the sample kind already accepted for a
series. `HeadBuffer` retains a specialized dense/sparse `u8` kind table keyed
locally by `(range, lane, SeriesRef)`, where zero is absent and nonzero values
encode the five `SampleKind`s. The publisher qualifies it with the head's full
`PartitionKey { topic, partition }`. It covers both active and OOO windows.
A subsequent sample with a different kind is rejected/dropped exactly as it
would have been before publication.

On a first sample, the writer checks an existing guard, encodes
transactionally, and installs a new guard only after encoding succeeds.
Mismatch does not update the timestamp table, dirty sequence, or catalog.
Guards survive freezes, descriptor compaction, publication failures, segment
write failures, and manifest-refresh failures. A guard is retired only when
all fragments and tails for that exact key have successfully handed off and
the writer no longer has an accumulation for it.

Failed first-sample encoding remains transactional: it creates neither a
series entry, kind guard, dirty marker, nor published fragment.

### 8.2 Fragment identity and ordering

Each frozen fragment carries:

```text
FragmentKey
  topic
  partition
  start_ms
  end_ms
  lane

SequenceRange
  first_message_sequence
  last_message_sequence

WithinMessageRange
  first_sample_ordinal
  last_sample_ordinal
```

The ingester assigns one monotonically increasing message sequence after
source acquisition and before processing. Every accepted sample in that
message also receives its stable traversal ordinal. Runs for one key have
strictly non-overlapping sequence/ordinal ranges. Fragment ordering is:

1. aligned `(start_ms, end_ms)`;
2. in-order lane before OOO lane, preserving existing OOO precedence;
3. source partition;
4. message sequence; and
5. stable within-message ingest order.

Equal timestamps use the existing stable last-write-wins rule. Ordering is
independent of `HashMap` iteration.

Message sequence advances for every acquired and completed message, including
an empty, rejected-only, missing-value-only, or error-returning message. The
view's `visible_message_sequence` may therefore advance without a sample.
Sequence overflow is detected before processing the next message and stops
ingestion/publication; it never wraps.

### 8.3 Recorded-sample coverage ledger

Visibility coverage is defined over successfully recorded samples, not every
message ordinal and not merely time-policy-accepted datapoints.
`record_head_sample` adds each successful record to a per-message ledger using
its stable recorded ordinal and the same canonical typed semantic bytes used
by replay/readback fingerprinting. The ledger records a checked count and a
256-bit order-independent aggregate fingerprint. Each completed message also
carries its exact successful ordinal membership as canonical, single-message
runs. Each mutable window and pending/frozen fragment carries both that exact
membership and its ledger contribution.

The single writer maintains a bounded inductive `expected_unsealed` set.
Before an attempted datapoint receives an ordinal, the writer reserves one
worst-case run slot in the active-message set and in
`expected_unsealed`'s pending boundary capacity, before that datapoint can
mutate a head. Rejected datapoints merely leave spare capacity. The completed
message therefore appends without allocation. A failed append retains the
complete `CompletedMessageCoverage`, leaves the prior expected set unchanged,
and prevents admission of a later message until retry succeeds or publication
fails closed.

Candidate construction partitions every retained contribution into either a
non-handed candidate-head owner or an exact manifest-handed owner. The
publisher validates every fragment set, pairwise disjointness, and:

```text
non_handed_head_orders ∪ manifest_handed_orders == expected_unsealed
```

It separately validates checked counts and aggregate fingerprints against all
completed per-message ledgers at or below the candidate message cut;
contributions above the cut are absent. Fingerprints are diagnostics
supporting the ownership proof, not permission to ignore a structural
duplicate/missing contribution.

After, and only after, the immutable root commits,
`expected_unsealed` becomes `non_handed_head_orders` and the handed fragments
may retire. Any failure before that commit preserves the old expected set and
both fragment classes for exact retry. Older sealed membership is not retained
forever: the preceding successful exact proof and monotonically validated
manifest cut form the induction hypothesis. The set is consequently bounded
by current uncommitted/unsealed ownership rather than ingestion history.

An accepted-prefix error is finalized after the partition head is reinserted:
its recorded prefix contributes normally, rejected suffix work does not, and
the original processing error remains reportable. A zero-record message has a
zero ledger and can safely advance the message cut.

Schema 8's sealed precedence is manifest order, while an unsealed fragment has
no manifest ordinal. Version 1 therefore requires one active source-partition
owner for a canonical series. If the catalog observes the same series in two
simultaneously unsealed source partitions, live publication fails closed and
readiness becomes false; ingestion and sealing continue. Disjoint series
across arbitrarily many partitions are supported. This restriction avoids
inventing a cross-partition duplicate winner that could change merely because
one partition sealed first. Removing it requires an on-disk or manifest-bound
provenance design and is outside this version.

Ownership is not stored in the append-only catalog or committed view. For each
candidate generation it is derived from the exact sample-root fragment
certificate. With at most one distinct full `PartitionKey`, no
cross-partition conflict is possible. With two or more, validation builds a
candidate-local collision-safe table keyed first by stable `series_id`; every
matching ID requires complete canonical-row comparison, so a hash collision
neither merges owners nor creates a false conflict. Distinct raw `SeriesRef`
rows that normalize to the same canonical row share this owner identity.
Conflict detection happens at publication because two partitions may have
accumulated mutable tails since the preceding boundary.

A successful handoff commit removes the old partition's fragments from the
candidate generation even if a slow view retains their historical
descriptors; those historical descriptors do not participate in the new
candidate's owner proof. A new partition may acquire ownership only when the
candidate generation has no visible tail/pending/current run for the old
partition. This permits a Kafka rebalance after logical handoff while old
queries finish safely.

## 9. Bounded root fanout

For each `(FragmentKey, SeriesRef, kind)`, the sample store keeps binary
descriptor levels. A new run starts at level zero. If that level is occupied,
the publisher creates one constant-size immutable concat node with the older
root as its left child and newer root as its right child. It does not flatten,
decode, copy block arrays, or re-encode payloads. The candidate root replaces
the two level entries with the concat root; old immutable roots remain valid
for old readers.

The following are required:

- at most one visible run per level and series key;
- at most `ceil(log2(publications_for_series_key + 1))` visible run roots per
  series key;
- every node stores checked sequence bounds, leaf/block/sample counts, and
  depth; depth cannot exceed the 64-bit generation bound;
- older sequence ranges traverse before newer ranges;
- concat does not inspect or deduplicate equal timestamps; query/seal traversal
  preserves older-before-newer order so their existing stable last-write-wins
  pass retains the complete newer sample;
- each carry allocates one bounded node and clones two `Arc`s, never payload or
  flattened descriptor arrays;
- sample kind, codec identity, event-time range, lane, typed metadata, and
  semantic query fingerprint remain unchanged;
- descriptor compaction needed for the new level completes before root
  replacement; and
- old input runs remain reachable through old roots until their final pin
  drops.

There is no synchronous payload reblocking. Any future background repack must
be separately specified, governed, and proven byte/semantic neutral.

Traversal is iterative with an explicit stack bounded by validated node depth;
recursive traversal is forbidden. The logarithmic bound is on visible run
roots, not total leaf/block descriptors or sample decoding. A query still
visits every selected leaf/block and decodes every selected logical sample.
Telemetry reports roots, nodes, leaves, blocks, and traversal work separately.
The design must not advertise logarithmic query complexity.

Version 1 deliberately creates micro-runs when a series is active across many
publication boundaries. A one-hour, 1 Hz series can therefore have roughly
3,600 one-sample leaves rather than a handful of natural 1,024-sample head
blocks. Persistent levels bound lookup roots, not leaves, blocks, or traversal
CPU. Live mode remains experimental/opt-in; the performance gate measures this
cost explicitly. If real-corpus or continuous-series gates fail, a compact
delta-run/reblocking design is required before wider enablement.

A configured live-view admission governor accounts for compact payload pages,
catalog rows/symbols/postings, run descriptors, candidate descriptors, and
exclusive bytes retained solely by old generations. Mutable writer tails are
reported separately and included in total live-memory telemetry.

The governor is not called a hard process-memory bound: rejecting new queries
cannot stop mutable ingestion growth, and the synchronous evaluator has no
cooperative cancellation contract yet. When the admission limit is exceeded,
the server rejects new live queries and publication may coalesce; ingestion
must not drop visible samples. If dirty age exceeds its limit, readiness fails.
Existing pinned queries retain their generation until they finish. This
limitation keeps live mode opt-in until a separate backpressure/spill and
cooperative-cancellation design exists.

## 10. Query abstraction

The production `SegmentStoreQuerySession` accepts an optional pinned
`&HeadReadView`. The view provides:

- selector projection with one shared `QueryBudget`;
- native Histogram selection;
- native ExponentialHistogram selection; and
- metadata collection.

The existing direct `*_with_head(&HeadBuffer, &resolver)` evaluator remains as
a compatibility/reference path in this change. Live HTTP must not use it.
Migrating or deleting that legacy evaluator is a later refactor.

All fragments participate in one selector operation before PromQL functions,
aggregations, binary operations, rate/extrapolation, or range stepping. It is
incorrect to evaluate a full PromQL expression independently per fragment and
merge only final results.

Planning first derives the generation- and time-filtered series-presence set
from the immutable sample root. Exact postings, negative/missing-label logic,
regex value dictionaries, result-label materialization, and metric/label
metadata are intersected with that presence set. A catalog-only row from a
missing number or failed encode cannot appear in metadata and its label value
does not consume `regex_values_examined`. Regex charging occurs once for each
distinct value whose postings intersect time-filtered presence, in stable
symbol order.

The session injects the head source at all six storage-source seams: generic
normal and cross-segment flow, native Histogram normal and cross flow, and
native ExponentialHistogram normal and cross flow. Every recursive AST branch
and repeated range step already funnels through those selector/native seams.
The diagnostic one-pass range mode is rejected explicitly while a non-empty
head is attached in version 1; there is no silent fallback to repeated mode.

All fragment and sealed reads share one `QueryBudget`. A stable logical series
is charged once for matched-series accounting even if it occurs in several
fragments or in both head and sealed storage. Samples are charged when decoded
according to the existing policy. Any intentional `QueryStats` delta must be
documented and tested.

The session-based API used by HTTP must support the pinned view and
retain the selected chunk reader, cross-segment-read option, range scalar cache
budget, query-label storage/materialization policy, instrumentation policy,
range execution mode, and query limits. Live mode must not silently fall back
to different query settings. Regex values are planned once against the
versioned series catalog so a query limit cannot change solely because the
same logical head was split by more publications.

The request's `Arc<LiveQueryView>` outlives session construction, every
normal/cross generic and native call, range-cache use, and serialization.
Head results are appended after all manifest-ordered sealed suppliers and
before the one logical merge. An empty sealed inventory must still consult a
non-empty head. No cache may store a result containing head data unless its key
contains the exact view generation/catalog revision; version-agnostic sealed
cache entries remain reusable.

## 11. Seal handoff

### 11.1 Invariant

For every logical sample whose message sequence is no greater than
`view.visible_message_sequence`, a pinned ready view has at least one physical
supplier:

- a head fragment in that generation; or
- a manifest-published segment in that generation.

The evaluator emits exactly one logical winner for a duplicate timestamp.
Samples after the view's cut are absent by definition. An old pinned view may
therefore omit data accepted after it was published.

A newly published root normally contains no head fragment that was handed off
in its sealed manifest cut. A previous pinned root legitimately retains the
head copy. If an implementation temporarily includes both in one candidate,
it must apply the normal deterministic precedence and charge physical decode
work; it may not double-emit or double-charge the stable matched series.

### 11.2 State transition

For one range:

```text
HEAD_VISIBLE
  -> mutable tail freezes into retained pending fragments
  -> writer streams/decodes pending fragments by immutable borrow
  -> segment files and footer complete
  -> segment directory renamed into place
  -> manifest append succeeds
  -> SEALED_MANIFEST_VISIBLE
  -> publisher incrementally validates/extends the shared store inventory
  -> one new root swaps in:
       new sealed reader
       handed-off sample runs retired at the new generation
  -> SEALED_VIEW_VISIBLE
```

There is no root replacement between the manifest append and successful store
open. If store refresh fails, the preceding view remains current, including
its head fragments, and readiness fails. Later boundaries retry refresh.

`SegmentWriter::flush()` returning success is the handoff trigger because it
returns only after the manifest append. Directory discovery alone is never a
trigger in manifest mode. The publisher retains the exact manifest filename,
validated end offset, and SHA-256 of the complete validated prefix that
includes the handoff.

Opening a wholly independent `SegmentStoreReader` per generation is forbidden.
The store inventory therefore owns immutable `Arc<SegmentReader>` entries and
one process-scoped metadata/FD runtime. Incremental refresh applies every
validated suffix record in order and opens every newly referenced segment;
unchanged readers and safe cache state are shared. A removed/tombstoned reader
stays physically available while any old inventory lease can reference it, so
a pinned query cannot lose a lazily opened file. Old views may retain old
inventory vectors, but not independent governors or duplicate unchanged
segment readers.

`ManifestCut` is a complete-prefix fingerprint, not merely a last-record hash.
It binds manifest identity, validated byte offset, SHA-256 of every byte
through that offset, complete ordered record prefix, and `CURRENT` generation.
Refresh handles:

- multiple appended records in one suffix;
- a partial/truncated tail by failing the general reader closed; only the
  retained in-process append attempt may repair an exact prefix of its known
  record at its known pre-append offset;
- manifest rotation and a changed `CURRENT` only when the filename advances
  and the complete previous logical record prefix is retained;
- ordered tombstones/removals;
- a missing/replaced prefix as corruption rather than a fresh empty inventory;
  and
- deletion only after all inventory leases release the removed reader.

### 11.3 Sealing frozen fragments

Sealing never consumes the sole recoverable copy of a tail. Rotation/drain
first freezes the tail into the retained pending queue; sealing then decodes
every fragment by immutable borrow. This permits a long query to retain
generation N while ingestion seals and publishes generation N+1, and permits a
failed destructive `SegmentWriter::flush()` to be rebuilt from the retained
fragments.

The seal path is a two-pass indirect-order streaming merge. Its first pass
walks each fragment's sorted run directory for one `(partition, range)` into a
fallibly reserved flat `(SeriesRef, stored kind)` array, then uses an
allocation-free unstable sort and deduplication. It builds fixed-size ordering
records that borrow canonical rows and symbols from the versioned label store,
then applies the existing metric-query/canonical-label comparator without
decoding samples or cloning labelsets. A k-way directory merge can later
remove the flat run-key scratch without changing this ordering contract. The
second pass follows that indirect order, gathers one series' strictly ordered
in-order runs followed by OOO runs, decodes one logical kind stream,
stable-sorts and deduplicates it once, writes it, and drops the scratch before
visiting the next series. It must not materialize every decoded series from
every fragment at once. Different native kinds remain separate under one
metadata row; Float and Int64 deduplicate after the existing scalar
conversion.

After merge, the existing writer record path is invoked exactly once per
`(range, output lane, SeriesRef, stored kind)` in the identical canonical
series/kind/metadata-batch order and with the identical scalar raw/non-raw
route used when live mode is disabled. It is never invoked once per fragment.
Because `SegmentWriter` re-encodes that one logical ordered slice, publication
fragment boundaries cannot affect chunks, indexes, footer, manifest order, or
deterministic IDs. Whole output trees are compared byte-for-byte across
several publication schedules.

Each frozen fragment's checked, sorted `Box<[FrozenSeriesRun]>` is its series
directory; it does not retain a second ref array or the otherwise unordered
head table. Pending and born runs both participate. Sealing uses a k-way merge
of those directories, so `AHashMap` iteration never determines writer order.
Run/directory bytes are charged to the live-view governor.

Scratch is bounded across series but may still grow to one complete logical
series because current `SegmentWriter` record APIs accept a per-series slice.
Allocation/count overflow fails the seal and retains pending inputs; it cannot
drop or truncate that series. Telemetry reports peak per-series scratch and a
pathological single-series test exercises the failure/retry path. External
spill for one unbounded series is a separately specified future improvement.

For pre-seal OOO, in-order fragments are merged first and OOO fragments
second, then written to `chunks.bin`. For post-seal OOO-only ranges, the merged
fragments are written to `ooo_chunks.bin`. These are the existing storage
semantics.

Pending fragments remain retained through every writer stage. On a failure
before a confirmed manifest record, the publisher discards/reinitializes the
entire writer attempt and may retry from those fragments. This reset applies
to a record-stage accepted prefix as well as a `flush()` error; retry never
appends retained fragments to a partially populated writer. On success, the
fragments remain pending until the incremental reader validates the exact
manifest cut and the root commit retires their sample runs.

The writer exposes a retryable flush attempt whose idempotency key is the
already stable segment ID; no manifest record-format field is added. The
attempt retains its logical input fingerprint, exact encoded manifest record,
pre-append offset, intended manifest identity, and `ManifestCut` outcome. A
retry after directory rename validates/reuses an
identical published directory rather than allocating a second logical segment.
After any append, manifest-sync, CURRENT-rewrite, or CURRENT-sync error, retry
first reconciles the intended manifest directly even if `CURRENT` is stale.
It authenticates every byte before the retained pre-append offset, then accepts
only an empty suffix, the exact complete retained record, or a strict byte
prefix of that record. Empty appends once; exact completes `CURRENT`; a strict
prefix truncates exactly to the retained offset, syncs, and appends once. Any
other partial/corrupt tail is corruption. A mismatched directory, segment
record, logical fingerprint, or prefix is corruption.
Ordinary successful replays retain current deterministic segment IDs, record
order, and bytes.

This in-process tail repair is specified normatively in `storage.md`; manifest
record version 1 bytes do not change. The current `ManifestCoordinator`
exclusively serializes retryable segment-seal append and tail-repair attempts;
repair is forbidden if another writer changed the retained pre-append prefix.
The legacy retention tombstone helper and manifest rotation are not yet routed
through it and must not race a coordinated attempt. The in-memory input
fingerprint is diagnostic only and provides no restart recovery. After process
loss, a partial tail still fails closed under the existing recovery non-goal
until a separately specified durable recovery mechanism exists.

## 12. Publication transaction

At every candidate boundary:

1. Reinsert the partition head even when processing returned an error.
2. Record whether the message accepted any datapoint or caused a successful
   segment flush.
3. If publication is not due, leave the current root unchanged.
4. Move each non-empty mutable fragment directly into a durable-in-memory
   `pending` queue, compact its arena, and replace the writer tail.
5. Strictly prepare shared catalog pages, live postings, and the candidate
   immutable catalog/sample roots under a unique candidate token.
6. Build structural descriptor carries from borrowed/`Arc` inputs into those
   candidate roots. Do not mutate the committed roots.
7. If a segment flush occurred since the previous root, incrementally
   validate/extend the exact manifest cut using the shared store runtime.
8. Construct a complete candidate `LiveQueryView` and readiness state.
9. Validate increasing generation/message/catalog cuts, non-overlapping run
   sequences, the at-most-one active partition owner per canonical series
   rule, pairwise-disjoint exact order sets whose head/handoff union equals
   `expected_unsealed`, and count/fingerprint equality for every
   recorded-sample ledger contribution at or below the candidate cut.
10. Acquire the short state write lock, validate the commit descriptor's base
    epoch, finalize `published_at`, replace exactly one
    `Arc<LiveQueryView>`, set `Ready`, clear the previous dirty/failure state,
    advance one status epoch, and release the lock. No other structure is
    modified under this lock. The anchor may precede external read-lock
    accessibility only by the remaining state assignments and unlock in this
    same measured critical section.
11. After commit, replace `expected_unsealed` with the exact non-handed head
    set, retire handed fragments, and drop superseded candidate/publisher
    bookkeeping; normal `Arc` ownership and inventory leases govern
    reclamation. Post-commit work does not mutate externally visible readiness
    for that commit.

Steps 4-9 are fallible. Step 4 is destructive only with respect to the mutable
tail: ownership enters `pending` before any later fallible work. Candidate
shared pages, persistent roots, descriptor carries, level maps, and outputs
are private until step 10 and are discarded on failure. Committed roots and
kind guards remain unchanged.

If a segment has manifest-published but refresh/validation fails, `pending`
retains both its frozen inputs and exact intended/observed manifest cut for
retry. The prior root may not cover samples accepted after its cut, so
the state atomically changes from `DirtySince` to `Failed`; readiness and new
queries then fail immediately. During normal candidate/refresh work, including
a pause after manifest append, the old coherent root remains queryable as
`DirtySince` until its deadline. It is not mislabeled as a failure.
A later publication retries without skipping a generation. The ingestion
error and publication error are both retained; one must not mask the other.

Failure-injection tests cover every boundary after fragment move, arena
conversion, catalog preparation, each descriptor carry, manifest validation,
new-segment open, coverage validation, and commit preparation. Each test then
ingests another message and proves an exact retry with no lost/duplicated
logical sample.

## 13. Shutdown

Shutdown:

1. stops admitting new source messages;
2. waits for any in-flight message to reach its reinserted message boundary;
3. attempts to publish any recorded mutable fragment;
4. drains/seals the head using frozen fragments plus tails even if step 3
   failed with the specifically recoverable cross-partition live-owner
   conflict;
5. refreshes the manifest-derived sealed reader;
6. publishes a final sealed-only live view;
7. stops HTTP admission; and
8. waits for admitted requests according to the existing server shutdown
   policy.

A legitimately empty ingester may publish a validated empty sealed-only final
view. An error must never be converted into an empty view.

Final shutdown has one semantics-preserving bulk-empty construction. It is
eligible only after the candidate sealed reader is bound to its exact current
manifest cut, with refresh mandatory for pending handoffs; coverage proves an
empty exact head-owned set; all pending fragments are handed off; no seal
attempt remains; and every mutable head reports no publishable fragment. The
identities of handed fragments that were already committed in the preceding
sample root must then equal that root's fragment certificate exactly; handed
fragments newly frozen during shutdown were never in the preceding root and
are excluded. The builder may replace the candidate sample map with an empty
root while preserving its catalog-revision floor, and may construct an empty
catalog successor after validating label lineage, revision, appended rows,
and generation. The normal `HeadReadView` root-pair validation still runs. A
mismatch fails before commit and preserves the old public root, retained
pending inputs, exact expected set, and unretired kind guards; a durable
manifest handoff that already succeeded remains recorded as handed off.

A live-owner conflict may be healed by deterministic final sealing; if seal,
manifest refresh, and final view publication all succeed, that transient live
conflict is logged but does not make shutdown fail. Integrity, catalog,
manifest, seal, or refresh errors remain terminal even though shutdown still
attempts safe drainage; the earliest integrity error is returned and later
errors are attached/logged without masking it. Tests cover no-data shutdown
and conflict healed by final seal.

## 14. Configuration

The ingester adds an optional top-level live API configuration. Defaults are:

```toml
[api]
enabled = false
listen = "127.0.0.1:9091"
head_publish_interval_ms = 1000
max_view_staleness_ms = 10000
# Required when enabled; no universal default is safe:
# live_memory_admission_bytes = <measured operator value>
```

The remaining query/read/concurrency fields use `chronoxide-api::ApiConfig`
defaults unless explicitly supplied. The top-level table exposes overrides for
all six `QueryLimits` fields, chunk-read mode/queue depth/coalescing gap,
cross-segment chunk reads, range-scalar-cache bytes, and maximum concurrent
queries; it does not maintain separate embedded-only query defaults.
Validation requires:

- nonzero publication interval;
- `max_view_staleness_ms >= head_publish_interval_ms`;
- an explicitly configured nonzero live-memory admission value derived from
  measured host/workload capacity;
- nonzero maximum concurrent queries;
- `labelset_store = "flat_interned"` for the versioned shared-row facade;
- a configured head and segment writer when enabled; and
- a listen address that parses before ingestion starts.

Disabling the embedded API must not allocate frozen fragments, selector
catalog/postings, store readers, locks, or publication telemetry, and must
retain current ingestion and segment bytes.

## 15. Errors and observability

Publication exposes counters/gauges for:

- successful and failed publications;
- current generation;
- view age;
- unpublished dirty age;
- frozen fragment count and bytes;
- mutable/frozen arena allocated and used bytes;
- catalog symbol/row/postings bytes;
- shared, current-exclusive, and old-generation-exclusive payload bytes;
- structural descriptor carries and duration;
- candidate/transient descriptor bytes;
- root-swap lock duration;
- live queries by generation;
- views retained by active queries; and
- manifest refresh failures.

Logs include the generation, visible message sequence, exact manifest cut,
head fragment count, and failure stage. They never claim a failed candidate
was published.

The implemented core primitive records read-lock wait/hold on each admitted
`LiveQueryPin`. `begin_commit_timed` reports the descriptor read's write-lock
wait/hold. `commit_timed` separately returns swap write-lock wait/hold and the
subsequent preceding-root `Arc` drop duration; this is full allocation
reclamation only when no pinned view or other owner remains. The existing
`begin_commit` and `commit` methods are compatibility wrappers that discard
these values. HTTP responses produced after a live pin expose that pin's
generation, age, completed-message cut, catalog revision, and read-lock
wait/hold. Successful query responses also expose every `QueryStats` field and
a compact same-session I/O profile. These are per-operation raw observations,
not the counters, histograms, or retained-view gauges required by the complete
target above.

The implementation must not turn a poisoned lock, failed catalog build,
descriptor validation failure, manifest parse/checksum/bounds/order error, or
incremental store-refresh failure into an empty result.

## 16. Correctness tests

Focused tests must cover:

- an empty initial manifest plus a head-only query;
- float and int samples;
- exact stale NaN versus ordinary NaN and infinities;
- Histogram, ExponentialHistogram, and Summary typed metadata;
- missing number values remaining absent rather than zero;
- delta/cumulative temporality, start time, flags, and reset hints;
- exact, negative, regex, and missing-label matchers;
- scalar and native histogram projections;
- instant and range queries whose selector spans multiple fragments;
- every currently supported recursive AST branch through the repeated range
  path, plus explicit rejection of one-pass range mode with a non-empty head;
- each query-label storage/materialization policy and chunk-read flow;
- an accepted message becoming visible only as a whole;
- an accepted prefix followed by a processing error becoming visible;
- empty, rejected-only, missing-value-only, and typed-invalid messages between
  two stored messages advancing the message cut without inventing coverage;
- an invalid first sample creating no visible series;
- missing/invalid lower `SeriesRef` rows followed by a higher visible ref,
  while an old catalog revision remains pinned;
- kind mismatch after a freeze behaving as before the freeze;
- OOO before seal landing in the normal segment;
- OOO after seal landing in the OOO segment;
- equal-timestamp last-write-wins within a fragment, across fragments, and
  across the seal handoff;
- multiple source partitions with disjoint series;
- equal numeric partition IDs in two topics remaining distinct;
- simultaneous cross-partition ownership of one canonical series failing
  readiness, including distinct raw refs whose label names normalize to the
  same complete canonical row, then successful ownership transfer after
  logical handoff while an old-generation query remains pinned;
- structural descriptor carries preserving raw typed and PromQL semantic
  fingerprints;
- maximum-depth descriptor trees using checked iterative traversal without
  flattening;
- exact frozen-arena `capacity == used`, multi-page `BufferRef` preservation,
  and decode equivalence;
- identical-shape in-order and OOO windows never sharing the wrong selector
  state in the legacy reference path;
- publication failure retaining the preceding generation;
- completed-window retention allocation failure returning ownership intact and
  succeeding on retry without losing its recorded coverage;
- sparse successful ordinals, including rejected gaps, split across multiple
  event-time ranges and both head lanes while retaining one exact candidate
  cut;
- duplicate exact ownership with an unchanged structural sample count failing
  before a commutative aggregate can be treated as proof;
- one missing exact order replaced by an unrelated order with the same count
  failing the `expected_unsealed` equality check;
- completed-order registration allocation failure retaining the whole
  completion, retrying it before the next message begins, and then publishing
  the later boundary without losing either exact set;
- manifest handoff leaving `expected_unsealed` unchanged on a pre-root failure,
  then removing exactly the handed orders only after the retry commits;
- an ambiguous append/sync/`CURRENT` outcome followed by a new same-range
  fragment: reconciliation retires exactly the attempt-bound fragments and
  leaves the later fragment in the head supplier;
- after that partial same-range handoff, the surviving fragment keeps its
  range/lane kind guard, and a later mismatched sample kind is still rejected;
- checked handoff-coverage failure occurring before any fragment changes to
  `handed_off`;
- store-refresh failure retaining head coverage and failing readiness;
- partial manifest append, manifest sync failure, CURRENT rewrite/sync failure,
  rotation, multi-record suffix, and tombstone reconciliation;
- restart with a missing `CURRENT` plus any `MANIFEST-*`, `CURRENT.tmp`, or
  top-level `seg-*` publication evidence failing closed instead of publishing
  an empty sealed inventory;
- generation overflow failing closed;
- status-epoch overflow and lock poisoning failing readiness closed rather than
  leaving a previously `Ready` state admissible;
- query budgets applying once across sealed data and every fragment;
- regex-value limits remaining independent of publication schedule;
- thousands of catalog-only label values near the regex limit consuming no
  regex budget or metadata output, alongside one stored series;
- incremental store refresh sharing one metadata/FD governor and unchanged
  segment-reader/cache state across generations;
- old-inventory lazy reads remaining valid until its lease drops, followed by
  physical deletion eligibility;
- head-containing cache entries never crossing a generation, including an
  empty sealed inventory;
- direct raw typed comparison for stale/reset/start/temporality/flags and
  non-finite delta sums, not only final PromQL values;
- disabled mode and live mode producing byte-identical replay output for the
  same successfully accepted samples; and
- final shutdown publication being sealed-only.

Rejected or failed samples must not mutate reset state or later output. The
implementation's transactional OTLP reset preparation corrects a pre-existing
failure-path leak; tests containing such a failure assert the corrected
behavior rather than requiring the old erroneous bytes.

Where supported, live query results are compared with:

1. a reference `HeadBuffer` that was never frozen;
2. the same data after deterministic seal and readback; and
3. the independent Prometheus golden oracle for affected PromQL cases.

## 17. Deterministic multithreading tests

Concurrency tests use barriers/channels or a test-only publication hook, never
timing sleeps:

1. **Pinned generation:** pause query A after it pins generation N, publish
   N+1, run query B, then release A. A must return N and B must return N+1.
2. **No writer lock during query:** pause query A during head decoding and
   prove the ingestion thread can accept and publish the next message.
3. **No reader lock during publication:** pause candidate construction, prove
   queries continue using N, then swap and observe N+1.
4. **Seal handoff:** pause after manifest append but before root swap. Already
   pinned and newly admitted queries use the old head-covered `DirtySince`
   root while within its deadline. Injecting an actual refresh error switches
   new requests to `Failed`. After a successful swap, the same logical result
   comes from the refreshed sealed reader.
5. **Two simultaneous queries:** both pin the same generation while a later
   generation publishes; dropping either request must not invalidate the
   other.
6. **Slow-reader reclamation:** retain N through several publications and
   descriptor carries, prove N stays valid, drop it, and prove obsolete
   fragments can be reclaimed.
7. **Failure race:** inject refresh failure while readers hold N; no reader
   panics, no partial root appears, readiness fails, and a later retry
   publishes exactly N+1.
8. **Staged atomicity:** pause between each catalog page, postings root,
   sample-root path, owner-validation, inventory, retirement/omission, commit
   descriptor, and root-swap stage. Generation N sees none of the candidate;
   after the one swap generation N+1 sees all of it.

Loom is optional for the tiny root slot state machine. End-to-end tests with
real `Arc`, `RwLock`, barriers, head codecs, manifest writer, and query engine
are mandatory because Loom cannot model the storage objects.

## 18. Performance acceptance

Correctness is the merge gate. Before enabling live mode by default, a
real-corpus A/B run must also establish:

- disabled mode has no statistically meaningful ingest regression and no
  material RSS increase;
- enabled mode records publication p50/p95/p99 duration and ingestion pause;
- root pin/swap lock hold time is measured separately from candidate build;
- visible run-root count is logarithmically bounded per series key;
- frozen leaves/blocks per selected series, leaf/block multiplier versus the
  unfrozen head, encoded bytes/sample, run/table/directory overhead, and query
  traversal CPU are reported for 1 Hz, 15-second, bursty, and high-rate series
  shapes;
- peak RSS includes several deliberately pinned generations and reports arena
  slack, shared interner row/symbol bytes, additional live postings/query-ID
  bytes, descriptor bytes, shared bytes, and exclusive bytes;
- incremental refresh work, retained file descriptors, governor totals, and
  cache reuse are reported;
- live query cold/warm latency, read/used amplification, and semantic
  fingerprints are reported; and
- complete segment/manifest output trees for the same successfully accepted
  samples are byte-identical to a live-view-disabled replay with identical
  configuration across several publication schedules. Rejected/failed-sample
  reset-state correction is the explicit compatibility exception described in
  §16.

No fixed percentage is asserted without a fresh baseline. A result that is
correct but too expensive remains opt-in and must be reported as such.

## 19. Implementation order

1. Add exact-used frozen arenas, frozen-fragment extraction, kind guards, and
   borrowed streaming fragment sealing.
2. Add the compact append-only series catalog, versioned sample descriptors,
   candidate-local active-owner validation, and structural descriptor levels.
3. Inject `HeadReadView` into all six production query-session source seams;
   preserve the legacy direct-head evaluator as a reference.
4. Add an incremental immutable manifest inventory that shares the one
   process-scoped runtime/governor and unchanged segment readers.
5. Add `LiveQueryHandle`, pending/candidate/commit publication transaction,
   admission/readiness state, and per-stage failure injection.
6. Add a live-capable API router and embedded ingester server.
7. Add correctness, edge, deterministic concurrency, replay-byte, raw typed,
   and Prometheus-oracle tests.
8. Update `storage.md`, crate-boundary documentation, and configuration
   examples.

## 20. Review record

This section is populated during the required three review/revision passes.

### Iteration 1

The reviewer rejected the initial draft as correctness- and
implementation-incomplete. The revision:

- replaces 4 MiB mutable arena retention with exact-used immutable pages;
- replaces per-fragment owned-string indexes with a one-time compact,
  version-cut series catalog;
- replaces synchronous payload re-encoding with MVCC structural descriptor
  carries;
- defines pending ownership and candidate rollback after every fallible stage;
- fixes the handoff invariant relative to a view's message cut;
- requires borrowed, per-series streaming seal and recoverable writer retry;
- moves live head integration into the production query session rather than
  the legacy direct evaluator;
- requires incremental inventory refresh with one shared metadata/FD governor;
- makes root and readiness one linearizable state;
- adds an explicit initial fail-closed rule for simultaneous cross-partition
  ownership of one canonical series;
- records read-horizon enforcement as a separate existing API gap instead of
  misusing source timestamps; and
- expands raw typed, session-policy, rollback, byte-equivalence, and
  multithreading coverage.

### Iteration 2

The second review found that the first revision still hid several ordering and
transaction details. The revision:

- makes the catalog revision an exclusive dense row count and mirrors every
  interner row, so missing values or failed first encodes cannot create a
  delayed lower-ref MVCC leak;
- uses the full `(topic, partition)` identity in every publisher/sample key and
  owner-validation comparison;
- defines descriptor carries as constant-size persistent concat nodes, defers
  duplicate resolution to ordered traversal, and distinguishes roots from
  leaf/block work;
- adds explicit generation leases and generation-scoped active-owner proofs;
- makes the initial root optional and defines one-step successful
  root-plus-`Ready` publication;
- specifies sorted per-fragment series directories, k-way streaming seal,
  per-series scratch failure, and fresh-writer retry after record-stage
  prefixes;
- defines a manifest prefix/hash-chain cut, ambiguous append/CURRENT
  reconciliation, manifest rotation, multi-record suffixes, tombstones, and
  lease-delayed physical deletion;
- requires `Arc<SegmentReader>` inventory sharing and one process-wide
  metadata/FD runtime; and
- binds the pinned view lifetime and generation to every session/cache path,
  including head-only early returns.

### Iteration 3

The final review rejected seven remaining overclaims. The closing revision:

- replaces multi-structure MVCC activation with fully staged immutable
  catalog/sample/inventory roots, candidate-local owner validation, and one
  root/status `Arc` swap;
- defines coverage over successfully recorded samples using structural
  ownership plus checked count/typed fingerprint ledgers, while empty/rejected
  messages may advance the message cut;
- keeps manifest bytes at version 1, uses segment identity as the idempotency
  key, and specifies only exclusive-writer in-process known-offset tail repair
  as a new `storage.md` rule;
- keeps a normal paused handoff queryable as `DirtySince` and uses `Failed`
  only for an actual publication/refresh error;
- filters regex charging and metadata through generation/time sample presence;
- changes the fragment-bound claim to bounded root fanout and explicitly
  reports linear micro-run leaf/block traversal as an opt-in v1 risk; and
- permits a legitimate empty final view and lets deterministic final sealing
  heal only the classified live-owner conflict.

The memory closure also replaces a duplicated catalog with versioned shared
FlatInterned row/symbol pages, makes live-only postings reclaimable, discards
frozen head hash tables in favor of sorted compact runs, removes the fictional
1 GiB default, and strengthens whole-tree byte/performance gates.

The same reviewer performed a narrow closure check after these amendments and
approved iteration 3 without further blockers.

## 21. Post-review implementation audit

This is an implementation-to-design audit, not a fourth review iteration. It
does not amend the three-pass approval record above.

Four details were made explicit while translating the accepted design into
code:

- an initial head-only view uses `ManifestCut::Absent`; absence before the
  first `CURRENT` is accepted only when no `MANIFEST-*`/`CURRENT.tmp` evidence
  exists, and the live startup preflight also rejects a top-level `seg-*` path.
  This proven-empty state is distinct from disappearance or corruption after a
  publication;
- root commit takes the preceding `Arc` out under the short state lock,
  releases the lock, and only then drops it, so recursive reclamation never
  lengthens the publication critical section; and
- frozen sealing is a two-pass indirect-order operation. It first computes the
  compact metric-query order, then decodes/deduplicates/writes one logical
  series at a time. Writing one series per fragment would make publication
  cadence affect segment structure and is forbidden; and
- live query admission first clones the exact ready root under the state read
  lock, explicitly releases that lock, and only then charges the independent
  `QueryRetention` token. Atomic governor work does not extend the root-lock
  critical section.

The opt-in implementation deliberately does not claim every broader
operational mechanism described as a target in this design:

- this documentation pass also corrects a pre-existing default-value drift:
  the ingester application's
  `SegmentWriterConfig::default_segment_duration_secs` is 15 minutes, and the
  ingester constructs the enabled writer's `HeadConfig` from that duration;
  the earlier one-hour wording was not the writer-backed/live-mode application
  default. The independent head-only path retains its separate one-hour
  default;
- immutable inventory refresh shares unchanged `Arc<SegmentReader>` objects,
  cache state, and one metadata/FD runtime, and old views safely retain removed
  readers; the lease-aware physical directory deleter is not implemented;
- the configured live-memory governor currently charges retained frozen
  payload/run estimates and one nominal `QueryRetention` byte per admitted
  query. It does not yet charge every catalog, postings, persistent-root,
  inventory, candidate-scratch, or actual query-retained byte and is therefore
  not a hard RSS bound;
- the production live path uses an ID-only persistent `LiveSeriesCatalog` over
  versioned shared FlatInterned rows/symbols. The legacy compatibility
  `FrozenHeadReadView` constructor still rebuilds a presence-filtered selector
  index per query and is not the embedded publisher path;
- active-owner validation first derives and sorts the exact identity of every
  non-handed-off fragment, rejects duplicates, and proves that list equal to
  the candidate sample root's independently maintained fragment certificate.
  It then examines the full `(topic, partition)` keys from that validated
  certificate. With zero or one distinct active partition, cross-partition
  simultaneous ownership is impossible and publication skips the per-series
  owner index; exact catalog/sample active-series binding still follows before
  the root swap. With two or more active partitions, the implementation still
  rebuilds a collision-safe `BTreeMap` from every retained run at each
  boundary. A generation-versioned persistent owner root is a possible future
  multi-partition optimization, not part of the version 1 contract;
- the persistent range/lane sample-kind guard is currently a
  correctness-equivalent `BTreeMap` keyed by
  `(start_ms, end_ms, lane, SeriesRef)`. It preserves §8.1's mismatch,
  transactional-install, lifetime, and retirement contract. The specialized
  dense/sparse `u8` representation described in §8.1 remains an opt-in
  performance gate pending a profile that justifies its additional
  implementation complexity. Until then, guard lookup/insert is
  `O(log guard_count)` and exact range/lane retirement scans the guard map;
- the frozen seal's first pass currently scans sorted fragment directories
  into one fallibly reserved flat run-key vector, then uses allocation-free
  unstable sort/dedup before applying borrowed metric-query ordering. It does
  not yet implement a k-way directory walk that avoids this `O(total runs)`
  scratch; this is an opt-in performance/acceptance gap, not a
  sample-ordering exception;
- the ID-only catalog currently resolves query strings by scanning active name
  IDs and the selected name's active value IDs before reading postings.
  High-distinct-value lookup cost remains an opt-in performance gate; no
  auxiliary string-keyed lookup is implemented;
- live observability currently consists of failure-stage logs, atomic
  readiness/status, pull-style `live_memory_stats`, raw per-operation core
  pin/begin-commit/commit/old-root-`Arc`-drop timings, and live response
  generation/age/message-cut/catalog-revision/pin-lock headers. Successful
  sealed and live queries additionally expose complete stats and compact
  same-session read/used I/O diagnostics. The
  publication counters, latency histograms, and per-generation gauges required
  by §15 are not yet wired;
- manifest retry/tail repair is process-local and depends on the retained
  exact append attempt. General readers reject a partial tail, and no
  after-restart repair is claimed;
- `ManifestCut::Present` currently stores one SHA-256 over the complete
  validated prefix. Refresh rereads and parses the manifest to validate it; an
  incremental hash-chain/suffix-only manifest reader is not implemented;
- incremental refresh currently accepts a changed `CURRENT` only when the new
  manifest retains the complete previous logical record prefix. Compacted
  manifests containing only the final live set require a later explicit
  predecessor/rotation contract;
- `ManifestCoordinator` is used by retryable segment-seal publication, but the
  legacy direct tombstone helper is not yet routed through it and must not race
  a seal attempt;
- publication interval checks currently occur only at completed message
  boundaries. There is neither an eager startup publication nor a background
  timer: readiness remains uninitialized until the first completed boundary,
  and a dirty view cannot publish merely because input becomes idle. Dirty age
  nevertheless starts at the first successfully committed head mutation in
  the in-flight message, not at that message's later completion; and
- read-horizon enforcement, cooperative query cancellation, and
  spill/backpressure remain outside the implemented slice.

These limitations preserve correctness by failing publication/readiness closed
or retaining the old coherent generation. They keep live serving experimental
and disabled by default until the remaining performance and operational gates
are complete.

### 21.1 Verification closure

The implemented correctness slice passed:

- `cargo test --workspace --all-targets --all-features`;
- both strict workspace Clippy profiles from `AGENTS.md`;
- the independent `promtool` golden suite;
- the literal real-ingestion/paused-arena-decode generation race; and
- disabled, coalesced-live, and per-message-live complete-tree byte comparison
  over scalar IEEE edges, every OTLP-persisted kind, typed metadata,
  pre-seal/post-seal OOO, and equal-timestamp last-write-wins.

LLVM coverage over the complete ingester library reported 90.17% region /
87.97% line coverage for `live_publisher.rs`, 89.01% / 86.38% for
`live_seal.rs`, and 86.92% / 86.07% for `segment_output.rs`. These figures are
evidence for the implemented slice, not a substitute for the real-corpus
performance/default-enablement gate in §18.
