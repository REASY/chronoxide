# Phase 5 multi-partition head-topology evidence

- **Date:** 2026-07-21
- **Status:** isolated 2x2 implementation and code-level gates complete; real
  replay and performance evidence intentionally not run while the host is busy
- **Scope:** adaptive per-window head-series tables and long-lived
  last-timestamp tables; no on-disk format or query-semantic change
- **Normative storage and clock contracts:** [storage.md](../storage.md) and
  [clock.md](../clock.md)

## Question

The promoted adaptive tables were measured on an effectively single-partition
capture. A global `SeriesRef` sequence can look dense in that topology while a
production partition-local head sees strided or skewed subsets. This gate asks
whether both adaptive structures remain beneficial and bounded when sixteen
partition heads coexist, pages stay sparse, promotion thresholds are crossed,
windows rotate for a long-lived replay, and accepted out-of-order samples use
the OOO lane.

It does not revisit event-time policy, segment bytes, or PromQL semantics. The
two adaptive tables are independent runtime factors. Each topology therefore
uses the four cells `pp`, `ap`, `pa`, and `aa`, where the letters name the
series-table and last-timestamp-table controls in that order. Exact ingest
counters, byte-identical same-topology corpora, factor-isolated structural
telemetry, and independent readback success are mandatory correctness gates.
With only one observation per cell, latency/RSS results are directional and
cannot promote either production default.

## Deterministic derived captures

`chronoxide-capture-repartition` reads an existing capture through the normal
global-sequence merge reader and writes complete records without decoding the
OTLP payload. The selected prefix receives dense output sequence numbers equal
to its zero-based global input ordinal. The reader exposes the sequence
persisted in each capture record; the transform requires source sequences to
be strictly increasing, hashes them, then reopens the output and requires its
persisted sequence to equal the dense ordinal. It never substitutes an
enumeration counter as proof of the stored value. For destination partition
`p`, the new offset is the number of earlier selected records assigned to `p`.

For `N = 16`, the exact mappings are:

```text
uniform:
  p(i) = i mod 16

skew80-20:
  if i mod 5 != 4: p(i) = 0
  otherwise:       p(i) = 1 + ((i / 5) mod 15)
```

Thus every complete five-record group sends four records to partition zero and
one record round-robin across partitions 1 through 15. The uniform mapping
differs by at most one record between partitions for any prefix. These rules
depend only on the global ordinal, not allocation, thread order, payload
contents, or host state.

The transform preserves, byte-for-byte:

- the raw OTLP payload;
- topic;
- source timestamp (diagnostic only); and
- trusted `captured_at_ms` policy time.

It deliberately replaces partition and offset. Those fields describe the
derived transport topology and are not asserted to be the original Kafka
identity. The report hashes the original partition/offset into the input and
mapping fingerprints so a changed source cannot pass as the same transform.

The report schema is `chronoxide-capture-repartition-v2`. Its physical and
mapping SHA-256 streams use fixed little-endian integers and u64-length-prefixed
topic/payload bytes. Concatenation below is literal and records repeat until
EOF:

```text
bytes64(x) = u64le(len(x)) || x

input = "chronoxide-repartition-input-v2\0" ||
        (u64le(ordinal) || u64le(source_sequence) || bytes64(topic) ||
         i32le(source_partition) || i64le(source_offset) ||
         i64le(source_timestamp_ms) || i64le(captured_at_ms) ||
         bytes64(payload))*

output = "chronoxide-repartition-output-v2\0" ||
         (u64le(ordinal) || u64le(output_sequence) || bytes64(topic) ||
          u32le(destination_partition) || i64le(destination_offset) ||
          i64le(source_timestamp_ms) || i64le(captured_at_ms) ||
          bytes64(payload))*

mapping = "chronoxide-repartition-mapping-v2\0" ||
          (u64le(ordinal) || u64le(source_sequence) ||
           i32le(source_partition) || i64le(source_offset) ||
           u32le(destination_partition) || i64le(destination_offset) ||
           u64le(output_sequence))*

content = "chronoxide-repartition-content-v2\0" ||
          (u64le(ordinal) || bytes64(topic) ||
           i64le(source_timestamp_ms) || i64le(captured_at_ms) ||
           bytes64(payload))*
```

The distinct streams therefore cover:

- input logical records, including original partition and offset;
- output logical records, including destination partition and offset; and
- the original-to-destination mapping.

A shared-domain canonical content encoding is also hashed independently from
the source record and the reopened output record. It contains global ordinal,
topic, source timestamp, `captured_at_ms`, and payload, but deliberately
excludes partition and offset. The input and output content hashes must match;
the distinct-domain physical stream hashes are evidence for their respective
layouts and are not compared to each other.

After close, the tool reopens source and output together and checks every
selected logical record and the absence of an output suffix. It also checks
manifest counts/bytes and saves a canonical byte-tree digest over sorted
relative file names, file lengths, and file-content SHA-256 values. Filesystem
metadata such as inode, mode, owner, and mtime is intentionally outside this
determinism claim. The formal runner creates two independent 8,192-message
Zstd-prefix outputs per topology and requires the output manifest, mapping,
logical output, and byte-tree digests to match. It creates only one full
four-million-message output per topology; repeating the full transforms would
consume approximately 41 GB without strengthening the byte rule already
exercised by the bounded real-payload prefixes.

## Same-binary comparator

The existing `adaptive_series_table` switch controls the per-window series
lookup. Phase 5 adds `adaptive_last_timestamp_table` for the per-partition
long-lived timestamp lookup. Defaults remain adaptive. The formal cells set
the controls independently, in `(series table, last timestamp table)` order:

- `pp`: `(false, false)`;
- `ap`: `(true, false)`;
- `pa`: `(false, true)`; and
- `aa`: `(true, true)`.

The last-timestamp wrapper retains the original plain hash table unchanged.
The adaptive implementation retains its 4096-ref pages, 2048-entry dense
promotion threshold, `2^24` directory bound, and sparse high-ref fallback.
The comparator changes representation only. Timestamp validation mutates state
only after a sample is accepted in either mode.

Periodic last-timestamp telemetry is an O(1) snapshot: insert/promotion paths
maintain the structural counters, and reporting scans no keys, pages, or
occupancy bitmaps. The per-window series table has the same requirement; its
sparse-page, high-ref, direct-page/series, and retained-capacity totals are
maintained on insert, promotion, and test-only removal. Tests independently
recompute both snapshots by scanning and require exact equality. Per partition
the reports record:

- series-table window count, in-order versus OOO window count, adaptive window
  count, explicit active-window rotation count, direct/sparse pages and series,
  high refs, and container capacities;
- last-timestamp mode, total series, dense/sparse pages and series, high refs,
  directory size/capacity, sparse capacity, and modeled paged bytes.

`HeadWindow` carries an in-memory lane marker solely so completed-window
telemetry distinguishes in-order rotation from OOO drainage. It does not enter
segment encoding or precedence decisions.
`in_order_rotations` increments only when an accepted later sample completes
the active in-order window. The final active-window drain and every OOO drain
remain visible in the lane/window totals but do not increment this counter.

At shutdown, partition heads are never drained in `HashMap` iteration order.
The processor collects all windows and establishes the total order
`(start_ms, end_ms, partition key, lane)`, with OOO before in-order for an
otherwise identical key, before touching the shared segment writer. A
fresh-process test varies hash seeds and reverses partition discovery, then
requires every emitted corpus file and path to be byte-identical.

## Formal schedule and gates

`phase5_head_topology_run.sh` first rejects ambient Cargo/compiler/allocator
configuration, dirty or untracked build inputs, hidden Git
`assume-unchanged`/`skip-worktree` state, and non-regular tracked inputs. It
creates a fresh `git archive HEAD`, rejects unsafe, duplicate, linked, special,
or noncanonical archive members, and extracts only regular HEAD files itself.
The exact path, type, executable-mode, size, and Git-blob graph is checked
against HEAD before extraction; every extracted file is made `0444` or `0555`
and every directory `0555`. Ignored and untracked files therefore cannot enter
the build even when their suffix is unknown or a tracked Rust file names them
with `include_bytes!`. Ancestor Cargo configuration outside the snapshot is
rejected. It performs one
`cargo build --locked --release --no-default-features` from that read-only
snapshot, with incremental builds disabled and isolated result-local `HOME`,
`CARGO_HOME`, target, and Cargo working directory under `env -i`. Cargo, rustc, the
resolved rustup toolchain executables, C compiler, build environment/command,
Cargo metadata, source tree/index/lockfile, and resulting binary hashes are
recorded. The ingester, repartitioner, query tool, and verifier are copied to
read-only preserved paths; only those four copies are probed or executed.
The live source, immutable archive, extracted source graph, harness,
configuration, helper-tool, and binary seals are checked around every
transform, sizing replay, measured replay, query, and verifier.
Each launch records its exact preserved-binary hash, sanitized environment,
and argument vector.
The frozen harness is an exact, read-only file allowlist with no bytecode-cache
entry. Every Python launch uses `-B -I -S`; post-hoc support modules are
compiled directly from their sealed source bytes rather than loaded through
importlib's bytecode-cache path. Thus `-B` prevents cache creation while the
source-only loader and repeated harness allowlist check prevent an unsealed
planted `.pyc` from becoming executable authority.

The runner then executes:

```text
seed observations: uniform-pp-01, skew80-20-pp-01
after dynamic capacity gate:
  uniform:   uniform-ap-01, uniform-pa-01, uniform-aa-01
  skew80-20: skew80-20-pa-01, skew80-20-ap-01, skew80-20-aa-01
```

Before this schedule the harness runs one untimed 250,000-record `pp` sizing
replay for each topology. Those retained diagnostic corpora establish a
conservative full-run bound by ceiling-scaling to four million records and
multiplying by two. The largest sizing `chunks.bin` receives the same scale and
multiplier (with a 1 GiB floor) as transient rewrite headroom. Sizing is not
performance evidence and never enters a factorial contrast.
Each sizing execution nevertheless retains its raw log, runtime identity,
parsed correctness/head report, exact corpus manifest, and an explicit
`performance_disabled` document; absence of perf data cannot be confused with
a measured pass.

The first `pp` corpus provides the measured byte size for the dynamic capacity
gate. All four cells
for a topology reuse its one immutable full capture. Initial `sync -f`
writeback of every bounded and full generated capture is recorded before any
replay, so dirty generated pages cannot leak writeback into a timed arm. Before
each measured replay the runner again applies `sync -f` to every file in that
full capture, then `POSIX_FADV_DONTNEED` to all sixteen capture files, and
requires zero resident bytes from `fincore`. All writeback and eviction work is
outside `/usr/bin/time` and perf. The runner records GNU time, perf task clock/
cycles/instructions/branches/cache events/faults, process-tree RSS samples,
pressure snapshots,
binary/source/harness hashes, and the complete run schedule. Builds, profilers,
footer scans, query processes, and unrelated Chronoxide/database processes are
forbidden immediately before and after every measured replay and again before
the final evidence seal. The forbidden set includes Rust/C/C++ build tools,
make/ninja/CMake/Meson, container CLIs, perf/profilers/tracers, and named TSDB/
database processes. QEMU system/KVM processes, Android emulators, `adb`, and
Android/Gradle workers are also conflicts.

Before the first generated-capture transform, and immediately before every
later workload launch, an exact `/proc` classifier must admit a quiet host.
Each of the six transforms, both sizing replays, and all eight measured
replays then run under continuous filesystem/process and process-tree RSS
monitors. The workload root is held before `exec`; an atomic read-only control
document binds the root, guardian, and RSS-monitor PIDs to their Linux
`starttime` identities. The guardian and RSS monitor publish separate
read-only readiness markers only after their first identity-bound raw sample
is durable. The runner publishes the launch marker only after both are ready.

Both monitors use a fixed 100 ms cadence, retain at least two samples, observe
the launch, and record the initial and terminal edges when enforcing a maximum
200 ms poll-start gap. The guardian excludes only the bound workload tree and
samples the result filesystem and `/proc`; the RSS monitor independently
samples that same tree. A forbidden compiler/build/container/profiler/
database process, a free-space reading below the workload reserve, a cadence
miss, or a handshake failure fails closed. Cleanup snapshots PID, PPID, state,
start time, and tree depth, signals deepest-first with TERM then bounded KILL,
and rechecks identity before every signal. Disappeared, zombie/exited
(`Z`/`X`/`x`), and PID-reused targets are never signalled. Bounded
`EXIT`/`HUP`/`INT`/`TERM` traps clean the workload first and then the RSS and
guardian jobs, including signals arriving in a fork-to-identity-binding
window. A guard violation writes `PARTIAL_MEASUREMENT_GUARD_BLOCKED` and
preserves every artifact.

### Predeclared factorial directional decision

Within each topology, the series-table effect is estimated by `ap/pp` with a
plain timestamp table and `aa/pa` with an adaptive timestamp table. The
last-timestamp effect is estimated by `pa/pp` with a plain series table and
`aa/ap` with an adaptive series table. CPU is perf `task-clock`; peak RSS is
the larger of GNU time's maximum RSS and the process-tree sampler maximum.
Each factor's two conditional ratios are combined by geometric mean, and the
ratio of ratios is retained as an interaction diagnostic.

The former `0.97`/`1.03` CPU and `1.05`/`1.10` RSS promotion bounds, plus the
`1.05` CPU and `1.15` RSS per-contrast rejection bounds, classify each factor
as `directionally_better`, `directionally_worse`, or `inconclusive`. They are
diagnostic thresholds only. One observation per cell cannot estimate
same-cell replay variance, so the machine-readable result always has
`promotion_eligible=false`, `production_default_conclusion=no_change`, and
`overall_disposition=defer`. A production default may change only after a
separate replicated, counterbalanced confirmation of the nominated factor.

### Fail-closed disk budget

The runner never deletes an artifact. Its initial gate measures the source
capture bytes and reserves:

- two full derived captures, each bounded by source bytes plus 64 MiB of
  partition-layout overhead;
- four independently generated 8,192-message determinism prefixes, each with
  an explicit 256 MiB upper bound that is checked after creation;
- two 250,000-record sizing corpora, each bounded by source-capture bytes;
- one source-capture-sized transient allowance while a sizing corpus may
  rewrite `chunks.bin`;
- 8 GiB for the isolated controlled-build target and build evidence;
- 4 GiB for reports, logs, frozen tools, and verifier output; and
- a non-reducible 16 GiB free-space safety reserve.

The historical single-partition corpus size is not used as a topology-corpus
bound. After sizing, the runner requires space for all remaining formal
corpora under the sizing-derived topology bounds, one transient rewrite
allowance, the harness allowance, and reserve before starting the first seed.
After the uniform and skew plain seeds finish, their exact retained sizes
replace the conservative bounds. Before the remaining six runs it requires
current free space to cover exactly three uniform seed sizes plus three skew
seed sizes, transient rewrite headroom, the 4 GiB harness allowance, and the
16 GiB reserve. The continuous guardian's threshold reserves future corpora,
harness, and safety space while allowing only the current corpus bound plus
the one transient rewrite allowance to be consumed. Every later corpus must
equal its same-topology seed size, and byte manifests must still match. A
failed capacity gate writes
`PARTIAL_DISK_BUDGET_BLOCKED` and preserves all created outputs.

Generated-capture creation is protected by the same continuous guardian, not
only by the initial admission snapshot. Before each transform, the capacity
plan reserves that transform's declared output bound, all later transform
bounds, both sizing bounds and sizing transient allowance, harness overhead,
and safety reserve. Its guardian floor is the same calculation excluding only
the current output bound. The six raw lifecycle streams and the capacity-plan
arithmetic are final evidence.

The gate rejects unless:

1. both mapping reports cover exactly sixteen non-empty partitions and the
   exact selected four-million-message prefix;
2. each full transform passes reopened logical verification, while each pair
   of bounded real-Zstd-prefix transforms passes complete repeat
   relative-name/length/file-byte determinism;
3. all eight runs match the pinned ingest, typed datapoint, drop, watermark,
   and recorded-sample counters;
4. toggling the last-timestamp factor leaves the complete series-table
   structure identical at each series-table level, and toggling the series
   factor leaves the complete last-timestamp structure identical at each
   last-timestamp level;
5. all four factorial cells within one topology have byte-identical segment
   trees, identical ingest counters, and identical logical work counts;
6. every partition reports at least two actual `in_order_rotations` (a final
   active-window drain cannot satisfy this gate) and each topology records at
   least one OOO window;
7. uniform adaptive heads retain sparse series and timestamp pages, while the
   skew topology crosses both the 128-series and 2048-timestamp promotion
   thresholds and still retains residual sparse pages;
8. separate exhaustive Schema 8 footer/postings validation succeeds outside
   timing, both topologies retain the pinned logical sample count, and their
   topology-independent persisted-record multiset fingerprints match;
   ordered-v2 semantic fingerprints, physical verifier fingerprints, and
   layout counts are recorded but are not required to match across
   repartitionings; and
9. the independent 38-case readback oracle has zero skips/mismatches in each
   topology independently. Its uniform/skew documents are not compared.

The source capture is content-inventoried before transforms and again after
them. Every derived capture is inventoried before replay and all source/derived
inventories are recomputed and byte-compared after the complete replay/query/
verification sequence. The exact run plan is validated before work and again
by the performance gate; the completed replay summary is separately validated.
The final artifact seal includes the root `run-plan.tsv` and
`replay-summary.tsv`. Its post-hoc validator requires the exact predeclared
file and directory matrix, rejects additional sealed or unsealed evidence,
recomputes every digest, reparses each raw replay/perf/report/corpus input,
reruns the repartition, structure, storage, readback, replay-summary, and
performance gates, and binds runtime argv and `CONFIG_FILE` to the run plan.
It also reparses every before/after process snapshot and early/immediate
conflict scan. For every transform, sizing replay, and measured replay it
reconstructs the control binding, marker causality, guardian and RSS maxima,
first/terminal cadence edges, stable capacity floor, and empty conflict stream
from raw rows; a saved summary cannot self-attest. Known wrapped/versioned
build tools such as `soong_ui.bash`, `cargo-nextest`, `ninja.real`, `ld.bfd`,
and `clang++.real`, Android build/emulator variants, and exact `btop`, `htop`,
and `top` process names are conflicts in both classifiers; lookalike names are
not rejected.
The only admitted decision is `PERFORMANCE_DEFER`; any promote/reject marker
contradicts the unreplicated design. Finalization is staged: evidence validation first emits
the exact `FINAL_SEAL_VALIDATED` document, then the runner writes the versioned
`COMPLETE` marker and reruns the validator in completed mode. Unknown root
entries, missing sizing/seed markers, extra/fabricated evidence, contradictory
decisions, or ambiguous completion markers fail closed.

The runner is dry-run capable: dry-run still validates the pinned input and
free-space plan; freezes the harness, template, source archive/snapshot, build,
tool, and preserved-binary provenance; probes the preserved CLI surfaces; and
renders/seals the configs and run plan. It does not transform or inventory a
derived capture, compile the fadvise helper, run the perf-event preflight,
evict/check capture residency, check measured-process overlap, replay, query,
or verify. Formal execution additionally refuses unavailable perf events,
resident capture pages after eviction, or overlapping measured tools.

## Focused code coverage

Rust tests cover:

- exact uniform and 80/20 mapping sequences;
- record preservation, partition-local offsets, manifest accounting, reopened
  canonical-content equality, repeat physical determinism, and a deterministic
  single-partition Zstd fixture matching the real capture shape;
- independently selected plain/adaptive last-timestamp equivalence through
  exact promotion, rotation, and an accepted OOO sample;
- adaptive table sparse/high-ref/dense behavior and deterministic differential
  traces;
- in-order versus OOO window telemetry, with a separate rotation counter so
  one rotation plus shutdown drainage is rejected while two rotations plus
  drainage is accepted; and
- equal-range partitions with same-range OOO/in-order windows across fresh
  processes, so deterministic drainage exercises partition and lane ordering
  rather than succeeding from distinct time ranges alone.

Python tests cover exact repartition counts and source identity, strict Markdown
telemetry parsing, four-cell work equivalence, cross-factor structural
isolation, missing OOO and work-mismatch rejection, promotion/sparse coverage,
exhaustive storage-report validation without false cross-topology ordered
identity requirements, and comparator config round trips. They also cover all
directional performance regions and the unconditional non-promotion result, exact partition
suffix/counter invariants, source-seal bypasses (including ignored extensionless
`.cargo/config`, hidden index flags, and tracked symlinks), ambient allocator/
build variables, run-plan/final-seal tampering, capture mutation, QEMU/Android/
Gradle conflict classification, descendant ownership, and a real fail-closed
disk violation that terminates only a spawned process tree.
Shell syntax and ShellCheck are separate required checks.

The verifier emits two deliberately different semantic identities. The
Phase 6 `decoded_semantic_fingerprint` is the ordered-v2 identity and retains
segment/lane/order detail; it is recorded independently for each topology and
is inherited by all four cells because their complete segment trees are
byte-identical. It is never compared across repartitioned topologies.

`topology_independent_decoded_semantic_fingerprint` hashes every decoded
canonical-label, kind, timestamp, and exact logical value record, including
typed metadata and duplicate multiplicity, as an order-independent multiset.
Its domains are exactly
`chronoxide-topology-independent-semantic-series-v1\0`,
`chronoxide-topology-independent-semantic-record-v1\0`,
`chronoxide-topology-independent-semantic-multiset-a-v1\0`,
`chronoxide-topology-independent-semantic-multiset-b-v1\0`, and
`chronoxide-topology-independent-semantic-corpus-v1\0`. The record digests are
summed modulo 2^256, so the comparison streams without materializing or sorting
the corpus. The JSON field is required and compared across uniform/skew.

That multiset proves persisted logical record content and multiplicity, not
duplicate winner ordering or query-result equivalence. Two streams containing
the same conflicting duplicate records in opposite order have the same
topology-independent fingerprint. The uniform and skew layouts are therefore
independent workload strata, not semantic A/B arms; their readback documents
must each pass but are not compared.

## Interpretation limits and risks

- The derived workload uses real payloads and capture/policy time, but the
  source capture has no Kafka key. Ordinal repartitioning therefore measures a
  controlled strided/skewed stress topology, not producer-key affinity or a
  live broker rebalance. A favorable result closes the representation gate;
  it does not prove every production partition distribution.
- Splitting records can make one logical series appear in more than one
  partition head. This is intentional stress coverage. The same-topology
  four-cell byte gate prevents representation changes from hiding behind it.
  The cross-topology multiset gate proves stored record identity and
  multiplicity only; it makes no duplicate-winner/query-equivalence claim.
- Structural byte estimates exclude allocator metadata. Peak/time-series RSS
  remains authoritative for capacity decisions.
- Periodic series-table and last-timestamp structural snapshots are O(1).
  Maintained counters are scan-checked in tests; a future structural field that
  requires a table or occupancy scan must not be added to the timed reporting
  path.
- One observation per factorial cell is insufficient for production
  promotion even on a quiet host. A favorable directional result must nominate
  one factor for a separate replicated, counterbalanced confirmation.
