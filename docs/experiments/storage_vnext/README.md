# Storage-format A/B experiment

`storage_format_ab_run.sh` replays one capture with pinned v7 and vNext
binaries. It creates four new corpora in this order:

```text
v7-a -> vnext-a -> vnext-b -> v7-b
```

It never removes or reuses output. The default prefix gate consumes two million
source messages, copies the exact binaries into run metadata, snapshots the
complete tracked source delta plus explicitly named untracked task sources,
records config/capture hashes and environment, then requires deterministic
same-format file manifests and equal cross-format segment IDs. Full mode
requires both query binaries. Prefix mode without query binaries finishes with
a visible coverage-gap marker rather than a passing `COMPLETE` marker.

The completed first prefix run and its noisy-host caveat are documented in
[2026-07-13-prefix-results.md](2026-07-13-prefix-results.md).
The implemented schema-7 inline-series replay, focused correctness gate, and
measured size/write/read results are documented in
[2026-07-14-schema7-prefix-results.md](2026-07-14-schema7-prefix-results.md).
That focused run was executed separately: this four-run harness still targets
the schema-6 paged-symbol A/B. The strict schema-7 PromQL path and its explicit
validated `schema6-ab` comparator use `schema7_query_ab_run.sh`; do not conflate
that paired query gate with this four-replay symbols experiment. That format
A/B pins `--label-materialization full` for both corpora so demand-driven label
ownership cannot confound the storage-layout comparison.

Schema 8 is now the no-flags writer/reader/query/API contract. Schema 7 remains
an explicit prior-format comparator. `schema8_query_ab_run.sh` compares fresh
Schema 7 and Schema 8 corpora with one preserved release binary, runs footer
and independent-readback validation outside timed queries, alternates layout
order, and requires exact/portable fingerprints, result shapes, and every
`QueryStats` field to match except `index_postings_bytes_read`. The number of
postings reads must still match. Its mixed instant/range manifest is
corpus-specific and must be checked against every fresh replay before use.

The 2026-07-23
[canonical cold-series plan result](2026-07-23-cold-plan-fastpath-results.md)
promotes a code-only seal fast path. It removes the normalization label clone
and sort, fuses cold-shape discovery, and reuses keyset scratch storage. The
real replay retained exact bytes and semantics, reduced whole-process
requested-live bytes at the selected large-window seal peak by 845.28 MiB and
whole-process allocation calls by 4.87%, and was end-to-end neutral under the
explicitly accepted noisy-host gate. The process-wide requested-live maximum
remained at an earlier phase. The earlier borrowed-only four-pass candidate is
superseded because it regressed locality.

The subsequent code-only seal-memory sequence continued through the
[compact tagged chunk-entry row result](2026-07-24-compact-chunk-row-results.md).
Safe 40-byte `Empty`/`One`/`Many` rows replace 56-byte inline-one rows while
preserving arbitrary multi-chunk and out-of-order behavior. On the accepted
250,000-message prefix, mean ingester high-water RSS fell 68.172 MiB and the
exact requested-live peak fell 67.255 MiB. Persisted bytes, footer validation,
and 40/40 independent readbacks remained exact; runtime was neutral.

The next isolated residual is documented in the
[active-segment seal-lifetime result](2026-07-24-active-seal-lifetime-results.md).
Explicitly releasing recording-only lookup, metadata-presence,
normalized-name-cache, and metadata scratch state before seal-time allocation
reduced mean ingester high-water RSS by 78.505 MiB and the event-exact
requested-live peak by 78.958 MiB. The predicted released family explains the
exact peak change within 12 bytes. Persisted bytes, footer validation, and
40/40 independent readbacks remained exact; runtime was neutral.

The writer-label follow-up is documented in the
[paged writer-label arena result](2026-07-24-paged-writer-label-arena-results.md).
A 24-byte writer row plus deferred 64 KiB label pages replaces 4.4 million
independent label vectors without changing persisted bytes. Mean ingester
high-water RSS fell 108.918 MiB and the event-exact requested-live peak fell
65.992 MiB. The exact row/page allocation delta explains the latter within
280 bytes. Two contiguous-arena variants were rejected because their large
mapping or lifetime overlap raised peak RSS. Footer validation and 40/40
independent readbacks remained exact; the QEMU-noisy run supports no
fine-grained runtime claim.

## Phase 1 current-head replay baseline

The accepted 2026-07-21 three-run baseline, deterministic corpus evidence,
exhaustive verification, readbacks, and separate CPU profile are documented in
[2026-07-21-phase1-replay-baseline.md](2026-07-21-phase1-replay-baseline.md).

`phase1_replay_run.sh` is the reusable four-million-message current-head
baseline harness. It consumes the capture and production Schema 8 template
whose hashes are sealed in `phase1_4m_expectations.json`, renders only the
capture, output, and `stop_after_messages = 4000000` fields, and refuses any
existing result or segment directory. A normal invocation creates three fresh
measured corpora. `--with-profile` adds a separate fourth `perf record` corpus;
that run is correctness-gated but is never included in replay latency.

Every measured replay records GNU time, the complete required perf-stat event
set, process-tree `/proc` RSS samples, pressure/load/process snapshots, and
capture residency before and after. The runner copies the three frozen release
binaries, complete Git patches and working-tree manifests, toolchain/LLVM,
kernel, CPU, memory, filesystem, build-command, capture, template, config, and
harness provenance before launching work. `POSIX_FADV_DONTNEED` plus `fincore`
must establish the configured capture-residency ceiling before each run.

Each corpus must independently match the historical 66-file,
5,569,314,896-byte tree and manifest SHA-256
`8b0789e2f6c404a144e0d2e87f152a83e9f0bedb9c5ab2c6512608056cae3289`.
The three measured trees, and the optional profile tree, must also be
byte-identical. Replay counters, OTLP type totals, rejection counts, event-time
ranges, and watermarks are checked exactly. After measurement, one
byte-identical representative corpus receives a separate exhaustive footer,
series, and exact-postings verifier pass plus the independent 38/38, zero-skip
readback pass. The gate pins both verifier fingerprints and a canonical
readback-result fingerprint before creating `COMPLETE`.
Exploratory runs that disable capture eviction or perf counters, or explicitly
allow a noisy host, can still finish the correctness gates but end at
`COMPLETE_WITH_COVERAGE_GAPS` instead.

Validate paths, binary interfaces, capture bytes, the template, provenance,
and all four rendered configs without launching replay or validation:

```sh
CAPTURE=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001 \
CONFIG_TEMPLATE=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717/config.toml \
REPO_ROOT=/home/user/github/REASY/chronoxide \
INGESTER_BIN=/home/user/github/REASY/chronoxide/target/release/chronoxide-ingester \
QUERY_BIN=/home/user/github/REASY/chronoxide/target/release/chronoxide-query \
STORAGE_VERIFY_BIN=/home/user/github/REASY/chronoxide/target/release/chronoxide-storage-verify \
RUN_NOTE='dry-run validation only; no measurements' \
RESULT_DIR=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-dry-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/phase1_replay_run.sh --dry-run --with-profile
```

The exact measured invocation is the same contract without `--dry-run`. Build
first; the runner deliberately never builds during a measured schedule:

```sh
CAPTURE=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001 \
CONFIG_TEMPLATE=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-adaptive-head-profile-20260716-223717/config.toml \
REPO_ROOT=/home/user/github/REASY/chronoxide \
INGESTER_BIN=/home/user/github/REASY/chronoxide/target/release/chronoxide-ingester \
QUERY_BIN=/home/user/github/REASY/chronoxide/target/release/chronoxide-query \
STORAGE_VERIFY_BIN=/home/user/github/REASY/chronoxide/target/release/chronoxide-storage-verify \
PERF_STAT_MODE=required \
EVICT_CAPTURE=1 \
MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT=0 \
RUN_NOTE='quiet host; no builds, profilers, replay, footer scan, query, or other database active' \
RESULT_DIR=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase1-4m-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/phase1_replay_run.sh --with-profile
```

`--validate-only` performs the expensive capture/template hash and binary
interface checks without creating `RESULT_DIR`. Run the focused local checks
with:

```sh
python3 docs/experiments/storage_vnext/test_phase1_replay_gate.py
bash -n docs/experiments/storage_vnext/phase1_replay_run.sh
```

`phase1_query_run.sh` is the dedicated single-Schema-8 baseline harness for the
storage-vNext program. It accepts only the byte-sealed
`phase1_query_matrix.json` and the deterministic four-million-message corpus
identity (66 files, 5,569,314,896 bytes, per-file manifest SHA-256
`8b0789e2f6c404a144e0d2e87f152a83e9f0bedb9c5ab2c6512608056cae3289`).
The 17 matrix entries cover eleven fixed expressions plus cache and exact Full
materialization controls. Every entry uses three four-process blocks:
`off,detailed,detailed,off`, `detailed,off,off,detailed`, then
`off,detailed,detailed,off`; each fresh process records one CLI-cold and two
warm evaluations. Detailed timings are attribution evidence, not latency
baselines.

The accepted corrected 2026-07-21 matrix, Off latency/RSS baseline, Detailed
stage attribution, metadata/cache evidence, and payload amplification are
documented in
[2026-07-21-phase1-query-baseline.md](2026-07-21-phase1-query-baseline.md).

The scalar selective rows use the physical Float/Int64
`container_cpu_usage_seconds_total` series. Virtual Histogram `_count` rows are
separate full-demand controls for typed-scalar decoding and the 0-versus-16 MiB
range-cache comparison. The nested Histogram and ExponentialHistogram p95
queries are also full-demand by design; only the proved root native
`count by(...)` rows are expected to omit labels.

The runner inventories and hashes every regular corpus file before and after,
runs footer validation and the independent 38/38 readback oracle outside timed
queries, and checks `fincore` after evicting every inventoried file before each
fresh process. That residency evidence describes process start. Store startup
and corpus-fingerprint work can touch files before the timed query, so the
harness deliberately does not claim that every artifact remains OS-cold at the
exact query boundary. The final gate requires raw v10 shape and stage
invariants, the fixed schedule, zero readback skips/mismatches, stable exact and
portable fingerprints, complete `QueryStats` equivalence across Off/Detailed
and Full controls, cache-on/off semantic equivalence, and an unchanged corpus.
Only then does the runner create `COMPLETE`.

Validate the full plan without launching query or validation processes:

```sh
SEGMENTS_DIR=/absolute/deterministic-4m/segments \
QUERY_BIN=/absolute/chronoxide-query \
RUN_NOTE='plan validation only; no measurements' \
RESULT_DIR=/absolute/new/phase1-query-dry-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/phase1_query_run.sh --dry-run
```

For a measured run, use a genuinely quiet host and add
`QUIET_HOST_CONFIRMED=1`. Run the isolated contract tests with:

```sh
python3 docs/experiments/storage_vnext/test_phase1_query_gate.py
bash -n docs/experiments/storage_vnext/phase1_query_run.sh
```

## Phase 2 compact query-label IDs

`phase2_compact_ids_ab_run.sh` is the same-binary promotion gate for governed
query-local compact IDs. It accepts the fixed Schema 8 Phase 1 corpus and the
sealed `phase2_compact_ids_queries.json` matrix. The eleven queries cover broad
full-label output, equality, sparse regex, negative and no-result controls,
scalar instant/range evaluation, and native Histogram and
ExponentialHistogram count/p95 range paths.

The default four-block schedule is counterbalanced. Odd blocks run
`OwnedStrings, CompactIds, CompactIds, OwnedStrings`; even blocks reverse that
order. Every fresh process performs one CLI-cold and two warm evaluations.
Footer validation and the independent readback oracle run before timing, and
the runner requires zero corpus residency after `POSIX_FADV_DONTNEED` before
each process. Raw v11 output records compact-arena budget/current/peak charges
and their atom, pair, hash-directory, and source-translation categories. The
gate rejects any compatibility materialization, admission refusal, accounting
imbalance, fingerprint/result/ordinary-`QueryStats` mismatch, changed corpus,
or material control regression.

The accepted 2026-07-21 run used 176 fresh processes and 528 evaluations. It
passed footer validation and 38/38 independent readbacks. Compact IDs improved
the broad selector by 14.82% cold, 10.20% warm, and 71.48% peak RSS, with zero
compatibility materializations or budget refusals. The design, accounting
contract, complete matrix, and promotion decision are documented in
[2026-07-21-phase2-compact-query-label-ids.md](2026-07-21-phase2-compact-query-label-ids.md).

Validate the plan without launching query or validation processes:

```sh
SEGMENTS_DIR=/absolute/deterministic-4m/segments \
QUERY_BIN=/absolute/chronoxide-query \
RUN_NOTE='plan validation only; no measurements' \
RESULT_DIR=/absolute/new/phase2-compact-ids-dry-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/phase2_compact_ids_ab_run.sh --dry-run
```

For a measured run, remove `--dry-run`, set `QUIET_HOST_CONFIRMED=1`, and use a
new result path. Run the isolated gate and shell checks with:

```sh
python3 docs/experiments/storage_vnext/test_phase2_compact_ids_ab_gate.py
bash -n docs/experiments/storage_vnext/phase2_compact_ids_ab_run.sh
shellcheck docs/experiments/storage_vnext/phase2_compact_ids_ab_run.sh
```

## Phase 3 payload coalescing

The accepted Phase 3 matrix and fixed-policy promotion are documented in
[2026-07-21-phase3-payload-coalescing.md](2026-07-21-phase3-payload-coalescing.md).
One preserved binary ran gaps `0`, `256`, `1024`, and `4096` separately under
forced `pread` and forced `io_uring`. Each backend artifact contains 352 fresh
processes in an eight-block Williams schedule, with one cold and two warm
evaluations per process. Footer validation, 38/38 independent readbacks, and a
real queue-depth-8 `io_uring` preflight run outside timed processes.

`phase3_payload_coalescing_run.sh` creates one new backend-specific artifact.
The accepted latency artifacts use frozen raw schema v12. Current harnesses use
raw schema v13, which only renames the scheduler's cumulative physical-byte
field to `total_physical_bytes_executed`. The strict gate requires its declared
raw schema, the exact schedule and corpus, zero configured post-eviction page
residency, matching semantic fingerprints and
all public `QueryStats`, monotone physical plans, and backend-specific
scheduler accounting. Current per-backend schema v2 and cross-backend schema v4
carry the v13 physical-byte field; accepted frozen-v12 backends remain schema
v1 and their sealed comparison remains schema v3.
`compare-backends` accepts only sealed result paths
whose report digest is present in their completed artifact checksum manifest;
it emits paired latency/RSS and physical/scheduler evidence for all 44
query/gap coordinates.

The default remains the bounded fixed 4096-byte gap. Lower fixed gaps remain
available, including zero. The available corpus did not produce a stable
adaptive rule, so no adaptive selector or on-disk scalar sidecar was promoted.

`phase3_payload_attribution_run.sh` is a separate observer-heavy Detailed
diagnostic over four representative queries, three gaps, and both backends.
Its stage walls are explicitly not latency-comparison evidence; the runner
exists to distinguish payload read-pipeline time from the honestly combined
decode/projection/result-processing leaf. The accepted attribution showed that
the broad/scalar win is dominated by that combined leaf, not kernel read time:
the current payload-batch slice lookup linearly scans physical spans for every
locator lookup. That is a code-audited mechanism consistent with the combined
stage trend, not proof of causal share. The Phase 3 report records it as the
next isolated code-side comparator before any adaptive policy or scalar
sidecar is reconsidered.

Validate the main plan without launching queries:

```sh
BACKEND=pread \
SEGMENTS_DIR=/absolute/deterministic-4m/segments \
QUERY_BIN=/absolute/chronoxide-query \
RUN_NOTE='plan validation only' \
RESULT_DIR=/absolute/new/phase3-dry-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/phase3_payload_coalescing_run.sh --dry-run
```

Run the focused contracts with:

```sh
python3 docs/experiments/storage_vnext/test_phase3_payload_coalescing_gate.py
python3 docs/experiments/storage_vnext/test_phase3_payload_attribution_gate.py
bash -n docs/experiments/storage_vnext/phase3_payload_coalescing_run.sh
bash -n docs/experiments/storage_vnext/phase3_payload_attribution_run.sh
shellcheck docs/experiments/storage_vnext/phase3_payload_coalescing_run.sh
shellcheck docs/experiments/storage_vnext/phase3_payload_attribution_run.sh
```

## Phase 7 on-disk activation audit

The read-only activation decision is documented in
[2026-07-21-phase7-format-activation-audit.md](2026-07-21-phase7-format-activation-audit.md).
Current measurements do not establish a device-I/O or residual byte-layout
bottleneck, so typed scalar/common columns, packed frames, compact routing, and
adjacent-segment packing all remain deferred. Packed frames are the leading
capacity-only candidate, with a 242,005,078-byte current frame-header upper
bound, but have no demonstrated general query-latency benefit. No new format
version or storage semantics were introduced.

The archived four-replay paged-symbol harness additionally requires:

- stable replay counters, policy outcomes, OTLP type counts, event/capture skew
  ranges, and source timestamp ranges to match across both repeats and formats;
- all cross-format bytes outside `symbols.bin` and `footer.bin` to be identical,
  with every allowed difference written to
  `comparisons/cross-format-allowed-diffs.tsv`;
- an independent readback oracle that executes at least one query and reports
  zero mismatches;
- zero skipped readbacks unless an exact, named, quantified isolation-check
  waiver is supplied before the run; and
- a semantic query returning nonzero series and samples, with identical exact
  and portable fingerprints across all four corpora.

Any waived readback skip remains a coverage gap. Such a run ends with
`COMPLETE_WITH_COVERAGE_GAPS`, not `COMPLETE`.

For the current Schema 7/8 gate, deterministic segment IDs and every file
outside `indexes.puffin` and `footer.bin` must match byte-for-byte. Those two
files are the only intentional differences because Schema 8 changes the exact
postings encoding and binds that new index version in the footer.

After each replay, the harness also writes two machine-readable inventories:

- `storage-artifact-inventory.tsv` aggregates file counts and bytes for each
  standard segment artifact, keyed by run label and implementation.
- `symbols-layout-inventory.tsv` divides `symbols.bin` bytes into disjoint
  physical components. Version 2 reports the header, `u64` offset table, and
  string bytes. Version 3 reports the root header, page descriptors, fence
  bytes, page headers, local `u32` offset tables, and page string bytes.

The same rows without the run-label columns are preserved as
`runs/<label>/storage-artifacts.tsv` and `runs/<label>/symbols-layout.tsv`.
Component bytes sum exactly to aggregate `symbols.bin` bytes. The inventory
parser rejects unsupported versions, non-regular standard artifact paths,
symlinked segment paths, inconsistent fixed lengths, non-contiguous v3 page
ranges, malformed fence ranges, and invalid page offset-table endpoints/order.
It is a structural size inventory, not a substitute for footer checksums, v3
CRC32C validation, UTF-8/string-order validation inside every page, or query
readbacks; those remain part of the optional query validation gate.

## Phase 5 multi-partition head topology

The unmeasured design and implemented evidence contract are documented in
[2026-07-21-phase5-head-topology-evidence-design.md](../../superpowers/specs/active/2026-07-21-phase5-head-topology-evidence-design.md).
`chronoxide-capture-repartition` creates deterministic 16-partition uniform
and exact 80/20 derived captures while preserving raw payloads, source
timestamps, capture-policy timestamps, and global input order. It reopens and
checks every selected persisted source/output sequence and record, requires
matching shared-domain canonical content
hashes for input and reopened output, saves separate physical/mapping/byte-tree
fingerprints, and is tested for repeat relative-name, length, and file-byte
determinism, including a single-partition Zstd source; filesystem metadata is
outside that claim.

`phase5_head_topology_run.sh` creates one full derived capture per topology and
uses two independent bounded Zstd prefixes per topology for byte-determinism
evidence. Before timing it retains one 250,000-record sizing corpus per
topology, derives conservative full-corpus and transient-rewrite bounds, and
then compares both adaptive head tables against their plain hash
controls with one binary in P-A-A-P order for each topology. Before any of
that, the runner validates a clean live worktree, safely extracts an exact
`git archive HEAD`, makes the extracted tree read-only, and performs one
locked, non-incremental release build only from that snapshot with fresh
result-local Cargo home/target state under a sanitized environment. It
preserves read-only copies of the ingester, repartitioner, query tool, and
verifier; external binary paths are rejected. Live-source, archive, snapshot,
toolchain, build, binary, and runtime identities are retained and hash-checked
around each executable phase. The frozen Python harness is itself an exact
read-only, cache-free allowlist; every interpreter uses `-B -I -S`, and
post-hoc helpers are compiled from sealed source bytes without consulting
sibling bytecode caches. The first plain run of each topology is retained
as a corpus-size seed; before the remaining
six runs, a dynamic capacity gate reserves three copies of each measured seed,
4 GiB of harness overhead, and at least 16 GiB of free-space safety margin. It
does not use the smaller single-partition corpus as a topology-size bound. The
strict gate requires per-partition direct/dense and sparse coverage, rotations,
OOO lanes, identical work/counters, same-topology byte-identical corpora,
separate exhaustive footer/postings validation with the pinned logical sample
count, topology-independent decoded-semantic fingerprint equality, and
independent readback equivalence. Physical verifier fingerprints are retained
as evidence but are not compared across different repartitionings. A failed
capacity or continuous-guardian gate marks and preserves the partial result.
An early quiet-host scan runs before capture transformation. Every transform,
sizing replay, and measured replay is held until separate guardian and RSS
monitors have emitted their first identity-bound sample. Both sample at a
fixed 100 ms cadence, include the initial and terminal edges in a 200 ms
maximum-gap gate, and observe the read-only launch marker. The transform
guardian continuously protects the predeclared remaining-output, sizing,
harness, and safety reserve rather than relying on the initial free-space
snapshot alone. Cleanup binds PID start times, rejects reused or dead
identities, signals descendants deepest-first, and is active for normal exit
and `HUP`/`INT`/`TERM` races. Build, container, compiler, profiler/tracer,
monitor (`btop`/`htop`/`top`), Android build/emulator, and database processes
are rejected before and continuously during controlled workloads, after each
timed replay, and before final sealing. All generated
capture files receive recorded untimed `sync -f` writeback; each measured
replay repeats writeback before fadvise/fincore requires zero capture
residency. Source and derived captures are content-inventoried before and after
use. The staged finalizer accepts only the exact result-root/artifact matrix,
reparses raw replay, perf, structure, storage, readback, repartition, and corpus
evidence, reconstructs every control/ready/launch binding plus guardian/RSS
cadence and transform-capacity row, reruns every decision gate, and requires
exactly one matching performance marker before admitting versioned final/
completion markers. A dry
run validates the pinned input/free-space plan, performs the same archive-only
build/provenance checks, probes the preserved CLI surfaces, and renders/seals
configs plus the run plan. It does not transform or inventory a derived
capture, build the fadvise helper, run perf-event or capture-residency
preflights, check measured-process overlap, or launch replay/query/verifier
processes:

```sh
CAPTURE=/absolute/pinned/capture \
CONFIG_TEMPLATE=/absolute/pinned/config.toml \
REPO_ROOT=/absolute/chronoxide-worktree \
RESULT_DIR=/absolute/new/phase5-head-topology-dry-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/phase5_head_topology_run.sh --dry-run
```

For a formal run, remove `--dry-run`, set `QUIET_HOST_CONFIRMED=1`, supply a
truthful `RUN_NOTE`, and use a new result path. Do not run the formal schedule
while the host is busy. Local contract checks are:

```sh
cargo test -p chronoxide-ingester --bin chronoxide-capture-repartition
python3 docs/experiments/storage_vnext/test_phase5_head_topology_gate.py
python3 docs/experiments/storage_vnext/test_phase5_head_topology_guard.py
bash -n docs/experiments/storage_vnext/phase5_head_topology_run.sh
shellcheck docs/experiments/storage_vnext/phase5_head_topology_run.sh
```

## Read-only series layout model

`storage_series_layout_model.py` scans existing `series.bin` v2,
`chunk_index.bin` v1, and index-container v7 roots/directories. It emits one
machine-readable JSON report for the conservative 56-byte screen, the selected
40-byte paged schema-7 model, and the exact structural v7-to-v8 index delta:

```sh
RESULT_DIR=/absolute/new/storage-series-layout-model-$(date +%Y%m%d-%H%M%S)
mkdir "$RESULT_DIR"
python3 docs/experiments/storage_vnext/storage_series_layout_model.py \
  --corpus /absolute/existing/segments-corpus \
  --output "$RESULT_DIR/model.json"
```

The model opens corpus artifacts read-only, rejects symbolic links and touched
structural corruption, and never creates, removes, or modifies anything under
`--corpus`. `--output` uses exclusive creation and refuses to reuse an existing
file; omit it to write JSON to standard output. Inline eligibility requires an
exact single-kind mask, a complete 40-byte chunk header, and every selected
width and scalar-lane invariant. The selected model also charges one CRC
descriptor for every hot page and every exact 16 KiB range of the unchanged
cold label byte stream. For v8 it validates the v7 root and both lazy-directory
roots, obtains the exact and auxiliary entry counts per segment, and applies
the canonical `ceil(exact_entries / 341)` page count and 48-byte record widths.
It does not encode v8 or model checksum CPU. The size gate is only a
pre-implementation materiality screen. Adoption still requires replayed bytes,
semantic equivalence, corruption coverage, and isolated query/replay evidence.
Preserve the JSON, exact script revision, command, and corpus fingerprint
together when citing a result.

The current v8-aware result and its scope are recorded in the
[schema-7 layout model report](2026-07-13-schema7-layout-model.md). Its raw JSON
is
`storage-series-layout-model-v8-20260714-113324/model.json` under the external
Chronoxide data root, with SHA-256
`f5bd76efde6ba3f36ae5a4c0aae8ed73b642fa153fb50fb2a1c3d84df52cd5f0`.
It projects a 2,257,877,360-byte net saving after charging 28,764,752 bytes for
v8: 10.48% of all modeled standard artifacts and 21.21% of modeled metadata.

Run its focused synthetic coverage with:

```sh
python3 docs/experiments/storage_vnext/test_storage_series_layout_model.py
```

## Complete adaptive-postings inventory

The implemented Schema-8 format and its initial capacity, correctness, and read
results are documented in the
[adaptive-postings result](2026-07-15-schema8-adaptive-postings-results.md).

`adaptive_postings_inventory.py` integrity-checks the schema-7/8 `series.bin`
v3 and `symbols.bin` v3 roots, then validates every `indexes.puffin` v8/v9 exact
directory, page, record, and postings payload in an existing corpus. It emits
a complete, unsampled JSON model of schema-8 RAW32 versus canonical delta
unsigned-LEB128 selection. RAW32 wins ties. For v9 it also checks that every
actual codec choice and encoded byte count equals that model. The report
includes per-segment and aggregate codec/size totals, reference-count
distributions, a SHA-256 fingerprint over the complete index bytes and
integrity-checked bound roots, and a format-independent fingerprint over all
decoded postings memberships for v8/v9 equivalence checks.

The corpus is opened read-only. The output uses exclusive creation and must be
outside the corpus. For the current four-million-message schema-7 corpus:

```sh
RESULT_DIR=/run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/postings-inventory-$(date +%Y%m%d-%H%M%S)
mkdir "$RESULT_DIR"
python3 docs/experiments/storage_vnext/adaptive_postings_inventory.py \
  --corpus /run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/storage-schema7-perf-4m-label-intern-20260714-234047/segments \
  --output "$RESULT_DIR/inventory.json"
```

The optional NumPy path only accelerates long-list validation and sizing; it
does not change the model. On this host, a compatible system Abseil CRC-32C
implementation accelerates payload integrity checks. The report records both
runtime choices. The portable Python CRC-32C fallback is exact but slower.

Run the focused synthetic corruption and sizing coverage with:

```sh
python3 docs/experiments/storage_vnext/test_adaptive_postings_inventory.py
```

The current application wiring uses the segment duration, 900 seconds, as the
normal head duration whenever the segment writer is enabled. The configured
3,600-second head duration is therefore not effective in this experiment; the
3,600-second out-of-order window is effective.

The schema-neutral metadata facade routes verified Schema 6, Schema 7, and
Schema 8 series identities, canonical labels, and exact chunk locators through
the aggregate `MetadataGovernor`. The PromQL query reader uses that facade.
Its default policy is strict Schema 8; Schema 7 remains the explicit
prior-format comparator, and Schema 6 is available only through the explicit,
footer-validated `schema6-ab` comparator. All three policies share the
aggregate metadata and file-descriptor governance used by the paired query
gates.

The pinned source capture currently has these hashes:

```text
manifest.json        84181ec8e9959166bc01224cb031c90980286f9945ba6dca5368942490db070d
partition-1.capture  1ecebab16fc68b984949810f32c2778857940530336554872d775215fdd28dc4
```

The harness computes and preserves the hashes again for every real run rather
than trusting this documentation.

## Source and binary provenance

`V7_REPO_ROOT` and `VNEXT_REPO_ROOT` are required. For each worktree the run
preserves the commit, remotes, index manifest, porcelain status, combined
`HEAD`-to-worktree binary patch, staged patch, unstaged patch, and hashes of all
provenance files. Every non-ignored untracked file must be classified explicitly
as task source in the corresponding colon-separated
`*_UNTRACKED_TASK_SOURCES` value. The harness copies those regular files with
their relative paths and verifies their bytes. It deliberately excludes and
does not copy generated `chronoxide-ingester/ingestion_stats_*.md` and Python
bytecode.

The ingester and query binaries are copied into `metadata/binaries/` before any
run. Replays and validation use those preserved copies, not the original paths.
`binary-sources.tsv` records source paths, preserved paths, and verified
SHA-256 hashes.

Review the untracked list manually before turning it into the colon-separated
allowlist; do not include runtime reports:

```sh
git -C "$VNEXT_REPO_ROOT" ls-files --others --exclude-standard
```

## Validate the plan without replaying

All paths must be absolute and `RESULT_DIR` must not exist:

```sh
CAPTURE=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001 \
V7_INGESTER_BIN=/absolute/v7/chronoxide-ingester \
VNEXT_INGESTER_BIN=/absolute/vnext/chronoxide-ingester \
V7_QUERY_BIN=/absolute/v7/chronoxide-query \
VNEXT_QUERY_BIN=/absolute/vnext/chronoxide-query \
V7_REPO_ROOT=/absolute/v7-worktree \
VNEXT_REPO_ROOT=/absolute/vnext-worktree \
VNEXT_UNTRACKED_TASK_SOURCES='chronoxide-core/src/storage/symbols.rs:docs/experiments/storage_vnext/2026-07-13-prefix-results.md:docs/experiments/storage_vnext/README.md:docs/experiments/storage_vnext/ab_gate.py:docs/experiments/storage_vnext/fadvise_regular_dontneed.c:docs/experiments/storage_vnext/query_ab_gate.py:docs/experiments/storage_vnext/query_ab_run.sh:docs/experiments/storage_vnext/storage_format_ab_run.sh:docs/experiments/storage_vnext/storage_inventory.py:docs/experiments/storage_vnext/test_ab_gate.py:docs/experiments/storage_vnext/test_query_ab_gate.py:docs/experiments/storage_vnext/test_storage_inventory.py:docs/reviews/2026-07-13-storage-read-layout-review.md:docs/superpowers/specs/archive/storage/2026-07-13-storage-vnext-paged-symbols-design.md' \
RESULT_DIR=/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-dry-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/storage_format_ab_run.sh --dry-run
```

Inspect `run-plan.tsv`, `configs/`, and `metadata/`. A real prefix run uses the
same command without `--dry-run`.

The known two-million-message prefix produced 16 skipped oracle cases, all
reported as `Isolation Check Skips`. If intentionally reproducing that exact
coverage gap, the waiver must be explicit and exact:

```sh
READBACK_SKIP_WAIVER_KIND=isolation_check \
READBACK_SKIP_WAIVER_COUNT=16 \
READBACK_SKIP_WAIVER_REASON='two-million-message prefix cannot isolate these readbacks' \
  docs/experiments/storage_vnext/storage_format_ab_run.sh --prefix
```

The gate rejects a different skip count, any non-isolation skip, zero executed
queries, or a mismatch. Do not carry the prefix waiver into a full run without
first establishing that the full corpus has the same specific coverage gap.

Dry-run creates both aggregate inventory TSVs with headers only; it does not
attempt to parse corpora that have not been replayed.

The isolated synthetic v2/v3 parser fixture can be run without Rust builds or
real corpora:

```sh
python3 docs/experiments/storage_vnext/test_storage_inventory.py
python3 docs/experiments/storage_vnext/test_ab_gate.py
bash -n docs/experiments/storage_vnext/storage_format_ab_run.sh
shellcheck docs/experiments/storage_vnext/storage_format_ab_run.sh
```

## Full-corpus gate

Full mode deliberately requires an extra acknowledgement because it creates
four full corpora and never cleans them automatically. Include the same
reviewed `V7_UNTRACKED_TASK_SOURCES` and `VNEXT_UNTRACKED_TASK_SOURCES`
allowlists used by the dry run (they are omitted below only for readability):

```sh
CAPTURE=/absolute/kafka-capture-001 \
V7_INGESTER_BIN=/absolute/v7/chronoxide-ingester \
VNEXT_INGESTER_BIN=/absolute/vnext/chronoxide-ingester \
V7_QUERY_BIN=/absolute/v7/chronoxide-query \
VNEXT_QUERY_BIN=/absolute/vnext/chronoxide-query \
V7_REPO_ROOT=/absolute/v7-worktree \
VNEXT_REPO_ROOT=/absolute/vnext-worktree \
RESULT_DIR=/absolute/new/storage-vnext-full-$(date +%Y%m%d-%H%M%S) \
ALLOW_FULL_REPLAY=1 \
  docs/experiments/storage_vnext/storage_format_ab_run.sh --full
```

Do not run builds, profilers, footer validation, or another database alongside
the measured replay. `POSIX_FADV_DONTNEED` is applied to the capture before
each replay by default; residency snapshots are saved, but this does not flush
the storage device's internal cache. Set `HOST_NOISE_NOTE` to describe known
concurrent workloads; it is copied to `metadata/run-note.txt` and the
environment record. If the host is noisy, treat elapsed time, throughput, seal
latency, and RSS deltas as exploratory; deterministic bytes, counters,
validation, and semantic fingerprints remain useful correctness evidence.

## Archived paged-symbol query A/B

`query_ab_run.sh` is the query-performance follow-up for a completed replay
A/B root. It compares `runs/v7-a/segments` and `runs/vnext-a/segments` with one
copied historical vNext `chronoxide-query` binary. It is intentionally frozen
to raw benchmark schema v5 and the removed `--experimental-storage-layout-ab`
flag, so a current query binary is not compatible with this archived harness.
Use `schema8_query_ab_run.sh` for current Schema 7/8 comparisons;
`schema7_query_ab_run.sh` remains the prior Schema 6/7 comparison. With its
matching historical binary, `query_ab_run.sh` isolates the on-disk symbol
layout and its reader backend from unrelated code-version differences.

The machine currently has unrelated workloads, so validate only the plan for
now. `RESULT_DIR` must be absolute, its parent must exist, and the directory
itself must not exist:

```sh
AB_ROOT=/absolute/completed/storage-format-ab \
QUERY_BIN=/absolute/vnext/chronoxide-query \
END_MS=1782980413585 \
REPEATS=10 \
QUERY_NAMES_OVERRIDE='scalar_count native_quantile' \
RUN_NOTE='plan validation while host is noisy; no measurements' \
RESULT_DIR=/absolute/new/storage-query-ab-dry-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/query_ab_run.sh --dry-run
```

Dry-run inventories and hashes both corpora, proves that every regular corpus
artifact other than `symbols.bin` and `footer.bin` is byte-identical, validates
the v2/v3 symbol headers, copies the exact query binary and harness provenance,
and writes the alternating process schedule. It does not evict cache or launch
a query process. Inventory traversal is NUL-safe and rejects symbolic links,
FIFOs, devices, sockets, and file-identity changes while hashing.

On a genuinely quiet host, remove `--dry-run` and explicitly acknowledge the
measurement conditions:

```sh
AB_ROOT=/absolute/completed/storage-format-ab \
QUERY_BIN=/absolute/vnext/chronoxide-query \
END_MS=1782980413585 \
REPEATS=10 \
QUERY_NAMES_OVERRIDE='scalar_count native_quantile' \
QUIET_HOST_CONFIRMED=1 \
RUN_NOTE='quiet host; no builds, replay, profiler, or other database active' \
RESULT_DIR=/absolute/new/storage-query-ab-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/query_ab_run.sh
```

Before every measured process, the runner applies `POSIX_FADV_DONTNEED` and
checks `fincore` residency for every inventoried file. The default hard
threshold is zero resident bytes; `MAX_RESIDENT_BYTES_AFTER_EVICT` can make an
explicit nonzero allowance. This controls Linux page-cache residency, not
device or controller caches. `REPEATS` must be even; format order alternates by
repetition so each layout occupies each order equally. Each fresh process
executes exactly two runs in one query session: cold first, then warm.
The range scalar cache defaults to disabled; set `STEP_MS` for a range query
and keep `RANGE_SCALAR_CACHE_MAX_BYTES` explicit.

The equivalence gate requires all repetitions, formats, and cold/warm runs to
have identical exact and portable semantic fingerprints, result shapes, full
canonical `QueryStats`, and effective schedule. Payload-read accounting,
range-cache accounting, and logical symbol calls/bytes must each match across
layouts and repetitions within the same run kind: cold is compared with cold
and warm with warm. Cold and warm accounting may legitimately differ because
the shared query session's caches can satisfy work in the warm run.
It also requires the v7 eager-symbol path and vNext paged-symbol path to be
exercised, and rejects touched corruption or resource-snapshot errors. The
summary reports latency, process peak RSS, payload used/read bytes and
amplification, symbol logical/physical reads and amplification, page-cache
counters, retained readers/files, source bytes, and retained memory charges.
The reported physical-read counts are process-issued reader operations, not
kernel syscall or storage-device I/O counts; the byte fields are the cleaner
layout comparison.
Peak RSS is process-wide, so the same process maximum applies to its cold and
warm rows. Raw JSON, logs, `/usr/bin/time -v`, residency snapshots, environment,
source patch, binary hashes, inventories, and final artifact hashes remain in
the new result directory. Footer validation is intentionally not part of the
timed query path.

The built-in `native_quantile` expression targets the known OTLP Histogram in
this corpus; it is not ExponentialHistogram coverage. If the selected corpus
contains an ExponentialHistogram, supply its exact expression explicitly and
select the reserved query name:

```sh
# Add these variables to either complete command above.
EXPONENTIAL_QUANTILE_QUERY='histogram_quantile(0.95, sum by (group_label)(rate(actual_exponential_histogram_metric[15m])))' \
QUERY_NAMES_OVERRIDE='scalar_count native_quantile exponential_quantile' \
  docs/experiments/storage_vnext/query_ab_run.sh --dry-run
```

Do not invent a metric name merely to fill that row: every selected query must
return nonzero series and samples or the gate fails.

The isolated parser, corruption, inventory, schedule, and synthetic dry-run
fixtures do not require real corpora or a Rust build:

```sh
python3 docs/experiments/storage_vnext/test_query_ab_gate.py
bash -n docs/experiments/storage_vnext/query_ab_run.sh
shellcheck docs/experiments/storage_vnext/query_ab_run.sh
```

## Phase 4 diagnostic one-pass range comparator

`phase4_range_one_pass_run.sh` compares the established repeated range executor
with diagnostic `one-pass-assume-scalar` in one source-bound binary. It is
explicitly nonpromotable: union-result allocation is not admitted before
allocation, finite-limit/error semantics are not exercised, `QueryStats` use
different work meanings, and the pinned corpus has no dense 24-hour range.

Footer validation, independent readback, and all 64 timed query processes use
the same held-root lifecycle. An atomic mode-0444 control binds runner, root,
and guardian PID/starttime/PPID identities. The guardian `fsync`s a first clean
raw sample before publishing mode-0444 readiness; the runner then invokes the
frozen helper to publish the mode-0444 launch marker. Sampling is fixed at 100
ms, with independently
reconstructed start/consecutive/terminal gaps no greater than 200 ms and a
mandatory final root-absent sample. Transient btop/htop/top, build, Android,
profiler, database, replay, or other classified conflicts terminate the bound
root. Active-child traps clean root before guardian with identity-revalidated,
reparent-safe TERM/KILL and bounded reaping. Final admission reconstructs every
raw sample stream, empty conflict stream, control, marker transition, terminal
edge, cadence, status, leaf seal, and the exact 66-lifecycle artifact matrix.

The comparator is read-only with respect to the corpus and writes only its
small evidence output, so it deliberately has no automated disk-capacity
admission. Manually confirm ample result-filesystem space before running.
The non-dry runner requires that acknowledgement as
`DISK_SPACE_CONFIRMED=1`; it does not infer a byte requirement or reserve
capacity.

Status: the hardened formal diagnostic completed and was admitted. It found
large latency reductions but retained the mandatory defer verdict. The
invocation and contract are in
[`phase4_range_one_pass_plan.md`](phase4_range_one_pass_plan.md), and the
reviewed evidence is in
[`2026-07-23-phase4-range-one-pass-results.md`](2026-07-23-phase4-range-one-pass-results.md).

The isolated gate and guardian tests require neither a Rust build nor a real
corpus:

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -B \
  docs/experiments/storage_vnext/test_phase4_range_one_pass_gate.py
PYTHONDONTWRITEBYTECODE=1 python3 -B \
  docs/experiments/storage_vnext/test_phase4_range_one_pass_guard.py
bash -n docs/experiments/storage_vnext/phase4_range_one_pass_run.sh
shellcheck docs/experiments/storage_vnext/phase4_range_one_pass_run.sh
```

## Phase 5 bounded allocator-policy screen

`phase5_allocator_screen_run.sh` is the frozen 250,000-message diagnostic
screen for the system allocator and four linked-jemalloc policies. The screen
uses the diagnostic `jemalloc-stats` feature; the production-facing
`jemalloc` feature selects the allocator without compiling stats or mallctl
diagnostics. The bounded application parser is active only for an explicit
stats-enabled allocator preflight, runtime-policy diagnostic, or post-drop
diagnostic. Ordinary system and plain-`jemalloc` startup does not reinterpret
or reject the production
`_RJEM_MALLOC_CONF` surface. Its exact
mirrored order is `S,J0,J1,J2,J3,J3,J2,J1,J0,S`. Requested jemalloc policy
text is not enough: the application preflight reads all eight fixed effective
`opt.*` values with `mallctl`, performs a live 64 MiB global-allocation probe,
and the measured structured startup record must reproduce the complete policy
exactly. All five jemalloc configuration sources are audited. J0 is the truly
unset linked-jemalloc default and is comparator-only; it cannot advance. J1-J3
additionally emit exact `confirm_conf` evidence. J1 fixes four arenas; J2 holds four
arenas while adding dirty decay 1000 ms, immediate muzzy decay, and one
background purger; J3 changes only the arena bound to two.

The 30-second hold begins after the `Ingester` has dropped. The checkpoint
workload wall ends before that hold. The external monitor records process-tree
`utime+stime`, `SC_CLK_TCK`, boundary uncertainty, and phase-bounded RSS. The
CPU gate uses the first post-drop CPU snapshot; sampled workload RSS and the
kernel-retained boundary VmHWM are separate gates. Full-process perf task-clock
and total peak RSS include the hold and remain separate lifecycle evidence.
Jemalloc also emits epoch-refreshed
allocated/active/resident/mapped/retained snapshots at drop and hold complete;
system fields are explicit null. Internal resident and aligned external RSS
are reported as non-equivalent measures. The runner requires capture eviction,
all perf counters, identical replay correctness and corpus bytes, exhaustive
footer/postings validation, exact 40/40 independent readbacks with 14 canonical
PromQL rows, and sealed selection/postings/readback fingerprints.

Structured allocator runtime-policy JSON and extended policy logging are
diagnostic-only. Ordinary startup does not parse the bounded experiment policy,
read effective `mallctl` options, or emit that record. The measured release
hold enables it implicitly; a selected-policy CPU profile uses the strict
`CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY=1` trigger without adding a
hold. System and plain-`jemalloc` builds report unavailable policy fields when
that trigger is explicitly requested.

Before the first measured observation, the preserved system binary performs a
fresh, untimed 250,000-message calibration replay. Its exhaustive, unsampled
Schema 8 verifier report and independent readback report are retained as raw
inputs and hashed. The canonical PromQL-row fingerprint is derived from that
raw 250k report; the Phase 1 four-million-message fingerprint is never copied
into this contract. Final validation must match the calibrated corpus,
semantics, postings, query counts, and row fingerprint. `seal-screen` rereads
the raw storage, readback, correctness, corpus, and calibration files and
recomputes the validation summary, so a fabricated or stale reduced summary
cannot complete the result.

Allocator, loader, glibc malloc, Rust wrapper/flag, compiler, and linker
overrides are rejected before the runner creates a result. Runtime commands use
`env -i` allowlists. Formal execution also runs a continuous 100 ms guardian
for the hardened Phase 4 vocabulary of concurrent build tools, container
builders, profilers/tracers, unrelated databases, QEMU/Android emulators,
`adb`, Gradle daemons/workers, versioned or `.real` compilers/Ninja,
`cargo-nextest`, `ld.bfd`/`ld.gold`, Soong, kati, and Android build tools. The
interactive monitors `btop`, `htop`, and `top` are excluded as well. The
guardian also enforces the exact remaining-corpus capacity budget plus an 8 GiB
reserve and terminates the owned launcher tree on either violation. Every run
begins and ends with sync plus a three-sample `Dirty + Writeback` quiescence
gate.

Measured launches are held until both monitors participate. One atomic
mode-0444 control binds the root, RSS monitor, and guardian PIDs/starttimes plus
canonical RSS-ready, guardian-ready, and launch paths. RSS sample one creates
RSS-ready; only then does guardian poll one create guardian-ready. The runner
releases launch afterward, and the held shell independently requires an empty
exact mode-0444 marker before `exec`. RSS evidence must retain at least two
strictly increasing 100 ms sample-start timestamps and observe launch after
readiness. Final admission reconstructs the edge-inclusive maximum gap through
the monitor's terminal elapsed time and rejects more than 200 ms, as well as
role, starttime, marker, digest, status, or causal drift. Untimed calibration
uses only the held root-plus-guardian control; it does not invent RSS evidence.

Use `--validate-only` to validate immutable inputs and the plan without creating
output or building. `--dry-run` performs the controlled same-clean-commit
system/`jemalloc-stats` builds and freezes build provenance, source, harness,
preflight records, hashes, and rendered configs without launching replay,
perf, cache eviction, or validation. Cargo never builds from the live
worktree. The runner proves it clean, creates a sealed `git archive HEAD`,
safely extracts an exact path/mode/blob-equivalent Git tree outside the
worktree, makes the complete source tree non-writable, and runs both builds
from that recorded CWD with `--manifest-path Cargo.toml` and an external target
directory; every Cargo manifest `path` reference must remain inside the
snapshot. This exact tree boundary prevents even arbitrary ignored files
named by `include_bytes!` from entering the build. The formal live-source seal
still rejects untracked or known ignored build inputs, hidden index flags,
symlinks/gitlinks, ambient Git overrides, and ambient Cargo configuration. The
live and extracted-source seals must remain identical before and after both
builds, around every ingester/query/verifier invocation, and at finalization.
The completed artifact manifest includes the archive and every extracted source
file. `build-target/**` is explicitly excluded non-evidence: Cargo and native
build systems may leave platform-generated links and other disposable
intermediates there. Final traversal still requires `build-target` itself to be
a real top-level directory and never follows it; the source archive, build
logs/provenance, and four preserved executables are the build authorities. The
four preserved executables are non-writable and their complete hash
set is checked at those same boundaries. Every Python helper, inline parser, and
background monitor uses one resolved, hash-pinned interpreter with `-I -S -B`;
the runner probes and records those flags and checks the interpreter before and
after every invocation. Helper scripts and the Phase 1 sibling module are
compiled from exact `.py` bytes, so ambient site customization and preexisting
`.pyc` files are never consumed. External binary paths are deliberately not accepted.
The executing screen runner must itself come from the selected repository and
must byte-match the read-only archived HEAD copy. Two independently pinned,
read-only control seals cover the frozen harness/plan/source/build authorities
and the complete measurement input set, respectively. The latter includes all
rendered configs and render records, the run plan, capture/template records, and
the compiled cache-eviction helper. Every fixed file has exact mode 0444 or
0555, and both seals are checked before and after each consumer invocation.
Do not start a formal run unless the host is quiet. All paths must be absolute
and `RESULT_DIR` must not exist:

```sh
RESULT_DIR=/absolute/new/external/allocator-screen \
REPO_ROOT=/absolute/clean/chronoxide-worktree \
RUN_NOTE='quiet host; no competing builds, profilers, scans, or databases' \
  docs/experiments/storage_vnext/phase5_allocator_screen_run.sh --dry-run
```

A partial directory is not evidence. Calibration, every run, and canonical
validation are immediately frozen read-only under exact tree seals that hash
every segment payload. Final admission independently reconstructs raw replay
counters, corpus inventories, RSS/perf/time/allocator records, observations,
the comparison, verifier/readback gate, and final decision. It then requires
an exact NUL-delimited file and directory inventory and hashes all evidence,
including payloads, before and after writing the versioned read-only
`COMPLETE` marker. A completed screen requires that marker,
`metadata/FINAL_SEAL_VALIDATED.json`, and
`comparisons/final-screen-decision.json`; the final decision still says that
production promotion is unauthorized. The full design is
`docs/superpowers/specs/active/2026-07-21-bounded-allocator-policy-screen-design.md`.
At most one of J1-J3 is deterministically nominated. That policy still requires
a stats-enabled four-million-message gate and a separate build-and-test
revalidation with the plain no-stats `jemalloc` feature before production; this
250k screen satisfies neither later gate.

### Phase 5 allocator full candidate gate

`phase5_allocator_full_run.sh` consumes exactly one J1-J3 nomination from a
completed screen. It does not hardcode a policy or a Phase 1/Phase 6 helper
hash: the completed screen's final inventory, source archive, control seals,
and frozen helpers are its authority. Run the copy inside the screen's
read-only `build-source` tree. Up front, the gate builds plain `jemalloc` from
that same source snapshot and runs all three allocator preflights. It then
measures the preserved system and `jemalloc-stats` binaries in 4M `S,C,C,S`
order, followed by the system and plain-`jemalloc` binaries in 4M `S,N,N,S`
order.

Both stages require at least 3% workload-CPU improvement, no more than 5%
workload RSS/HWM/released-RSS regression, and no more than 5% pair dispersion.
All eight corpora and replay-correctness documents must agree. Separate stats
and no-stats candidate corpora receive exhaustive footer/exact-postings checks
and independent readbacks outside replay timing. Raw authorities include every
segment payload. Final admission reconstructs the time/perf reductions,
quiescence and capture-residency summaries, corpus manifests, observations,
comparisons, and validations from raw inputs before writing its digest-bound
completion certificate. Exact file and directory inventories are revalidated
after `COMPLETE` exists, so neither a changed byte nor an added empty directory
can be admitted as completed evidence.

The initial result-filesystem admission is exactly 66,029,355,648 bytes: eight
frozen expected 4M corpora plus 10 GiB of build headroom and 10 GiB of
operational headroom. Immediately before each replay, the gate requires room
for every corpus not yet started (including the current one) plus the
operational headroom. After the first corpus is sealed, later checks use the
larger of its observed size and the frozen expected size. The continuous
guardian reserves the remaining corpora after the current one plus the same
operational headroom and terminates the measured process tree immediately if
that reserve is crossed or a conflicting process appears. All of these checks
refer to the filesystem containing `RESULT_DIR`; the capture may remain on a
different data filesystem. A successful guardian must retain at least two raw
monotonic poll-start timestamps. Final admission recomputes their maximum gap
over the guardian-start boundary, every poll start, and the guardian-finish
boundary. It rejects a gap above exactly 200 ms: the requested 100 ms interval
plus one explicit 100 ms scheduler-edge allowance. The measured shell is held
before `/usr/bin/time`, `perf`, or the ingester can execute. An exact read-only
control binds the held-root, guardian, and RSS-monitor PIDs and `/proc`
starttimes plus all three canonical handshake markers. It is fully written,
changed to mode 0444, and fsynced under a
private same-directory name before an exclusive atomic hard link publishes the
canonical path, so neither monitor consumes partial or writable control JSON.
RSS sample one creates the RSS-ready marker; the guardian waits for it, then
creates guardian-ready on poll one. The runner releases an exact mode-0444
launch marker only afterward, and both monitors must observe it later. Final
admission independently derives RSS and guardian cadence across their start,
middle, and terminal edges, rejects a gap over 200 ms, and proves the roles,
starttimes, markers, digests, causal ordering, and exact zero statuses.
Allowed-tree traversal rechecks the root starttime and every discovered child's
PPID. The conflict classifier separately retains identity-bound zombie
descendants while wrappers remain live, but excludes them from RSS and terminal
membership; it rereads PPID and starttime before every scan exclusion, so a
reparented or reused PID remains a conflict. Emergency termination snapshots
`(pid, ppid, state, starttime, depth)`,
signals deepest children first, and refuses `Z`, `X`, or lowercase `x` dead
states and reused PIDs. Runner
interrupt cleanup applies that identity-safe measured-tree termination before
stopping and reaping the RSS monitor and guardian; a rejected control falls back
to the already captured starttimes. Spawn/bind critical sections defer and then
immediately honor a pending signal, so no newly started job escapes binding.
Cleanup never signals an unbound raw PID, and its identity-aware reap uses only
200 polls with 10 ms delays before recording a still-live refusal in the
interrupted run directory.

Passing means only `eligible_for_manual_promotion_review`; the harness always
records `production_promotion_authorized: false`.

```sh
SCREEN_RESULT_DIR=/absolute/completed/allocator-screen
RESULT_DIR=/absolute/new/external/allocator-full-gate
CAPTURE=/absolute/data-filesystem/capture
CONFIG_TEMPLATE=/absolute/config.toml
SCREEN_RESULT_DIR="$SCREEN_RESULT_DIR" RESULT_DIR="$RESULT_DIR" \
CAPTURE="$CAPTURE" CONFIG_TEMPLATE="$CONFIG_TEMPLATE" \
QUIET_HOST_CONFIRMED=1 \
RUN_NOTE='quiet host; no competing builds, profilers, scans, or databases' \
  "$SCREEN_RESULT_DIR/build-source/docs/experiments/storage_vnext/phase5_allocator_full_run.sh"

PYTHONDONTWRITEBYTECODE=1 python3 -B \
  docs/experiments/storage_vnext/test_phase5_allocator_full_gate.py
bash -n docs/experiments/storage_vnext/phase5_allocator_full_run.sh
shellcheck docs/experiments/storage_vnext/phase5_allocator_full_run.sh
```

Use fresh sibling directories such as `/var/tmp/chronoxide-results/<screen>`
and `/var/tmp/chronoxide-results/<full-gate>` for the screen and result; do not
nest either result in the other. `CAPTURE` may point at its existing data mount.
`--validate-only` performs the screen, input, plan, and initial-capacity checks
without creating `RESULT_DIR`. `--dry-run` creates evidence and performs the
source-bound no-stats build and preflights, but runs no replay, perf measurement,
footer verification, or query readback.

The full contract is
`docs/superpowers/specs/active/2026-07-22-bounded-allocator-policy-full-gate-design.md`.

Allocation stacks are collected only in a separate untimed result directory.
`phase5_allocator_profile_run.sh` always runs Heaptrack against the exact
preserved system-allocator binary from a completed screen; that is the heap
allocation-stack authority. It first validates the completed screen's
canonical artifact manifest, live plus archived/extracted source seals, and all
four preserved executable hashes, then keeps that screen seal unchanged around
every profiled ingester, query, and verifier invocation. It gates replay correctness, a canonical
byte-identical corpus manifest, exhaustive validation, and absence of lost or
failed profiler events. Heaptrack runs with `--record-only`, and the gate
requires at least one positive, usable multi-frame collapsed allocation stack
containing a Chronoxide frame; a summary or leaf-only row is not evidence. It
records no A/B timing or RSS metrics. An optional
`perf record` call-graph replay may use either the system binary or the policy
nominated by the completed screen. A selected J1-J3 replay must additionally
rerun the application preflight, audit all five jemalloc configuration sources,
set the explicit runtime-policy diagnostic trigger, and match the structured
runtime effective policy and confirmation output.
Candidate-specific linked-jemalloc heap
profiling is explicitly deferred because Heaptrack interposition is not treated
as authority for the prefixed linked allocator.

The profile must be launched through the read-only runner copied into the
completed screen result. Each rendered profile config and its render record are
made read-only, semantic-checked, and covered by a separately pinned profile
control seal. The configured profile reserve (16 GiB by default, with an 8 GiB
floor) is separately published as an exact mode-0444 metadata authority and
included in every profile control seal. Final raw reconstruction uses that
authority, rather than a hard-coded floor, to reproduce each guardian's exact
reference-corpus-plus-reserve threshold. Optional perf evidence is accepted
only when `perf script` contains a usable multi-frame callchain with a
Chronoxide frame. Formal profile
launches require `QUIET_HOST_CONFIRMED=1` and `RUN_NOTE`; each replay gets the
same continuous quiet-host/capacity guardian. Profile subtrees and all segment
payloads are immediately immutable, final profile evidence is independently
reconstructed, and exact file/directory authorities are revalidated after the
versioned profile completion marker.
Heaptrack and optional `perf record` share one held root-plus-guardian path;
because profiles intentionally collect no RSS metric, their exact control has
no RSS-monitor role or RSS-ready marker. Each shell still verifies the launch
marker is exact mode 0444 before entering the profiler.
An active profile lifecycle is also covered by a status-preserving,
nonrecursive `EXIT` handler. Its emergency path uses the already captured
interpreter directly, so failure of the normal fail-fast interpreter wrapper
cannot skip sealed-control cleanup, starttime-bound fallback termination, or
bounded reaping.

```sh
SCREEN_RESULT_DIR=/absolute/completed/allocator-screen
RESULT_DIR=/absolute/new/external/allocator-profile \
SCREEN_RESULT_DIR="$SCREEN_RESULT_DIR" QUIET_HOST_CONFIRMED=1 \
RUN_NOTE='quiet host; no competing builds, profilers, scans, or databases' \
  "$SCREEN_RESULT_DIR/metadata/harness/phase5_allocator_profile_run.sh"

# Optional CPU stacks in a second fresh replay:
SCREEN_RESULT_DIR=/absolute/completed/allocator-screen
RESULT_DIR=/absolute/new/external/allocator-profile-with-perf \
SCREEN_RESULT_DIR="$SCREEN_RESULT_DIR" ENABLE_PERF_RECORD=1 PERF_POLICY=selected \
QUIET_HOST_CONFIRMED=1 \
RUN_NOTE='quiet host; no competing builds, profilers, scans, or databases' \
  "$SCREEN_RESULT_DIR/metadata/harness/phase5_allocator_profile_run.sh"
```

The isolated gate tests do not build Rust or replay data:

```sh
python3 docs/experiments/storage_vnext/test_phase5_allocator_screen_gate.py
python3 -m py_compile docs/experiments/storage_vnext/phase5_allocator_screen_gate.py
cargo test -p chronoxide-ingester allocator_policy --no-default-features
cargo test -p chronoxide-ingester allocator_policy --no-default-features --features jemalloc
cargo test -p chronoxide-ingester allocator_policy --no-default-features --features jemalloc-stats
cargo test -p chronoxide-ingester --no-default-features --test allocator_preflight
cargo test -p chronoxide-ingester --no-default-features --features jemalloc --test allocator_preflight
cargo test -p chronoxide-ingester --no-default-features --features jemalloc-stats --test allocator_preflight
bash -n docs/experiments/storage_vnext/phase5_allocator_screen_run.sh
bash -n docs/experiments/storage_vnext/phase5_allocator_profile_run.sh
shellcheck docs/experiments/storage_vnext/phase5_allocator_screen_run.sh
shellcheck docs/experiments/storage_vnext/phase5_allocator_profile_run.sh
```

## Phase 6 codec A/B lifecycle gate

`phase6_codec_ab_run.sh` is the frozen real-corpus RawF64/Gorilla comparator.
Each replay begins as a held process. An atomically published read-only control
binds that root, its RSS monitor, and its capacity monitor by PID, PPID, and
`/proc` start time. The monitors flush separate first samples and publish
distinct mode-0444 ready markers before the runner publishes the launch marker.
Both monitors must later observe launch, retain at least two root-bound samples,
and write a terminal boundary. Final admission independently rebuilds their
edge-inclusive `[start, samples..., terminal]` cadence and rejects any gap over
200 ms at the exact 100 ms interval.

The run-wide conflict/capacity guardian applies the same edge-inclusive raw
cadence and binds the runner parent identity. Parent disappearance, zombie
state, PPID change, or PID reuse fails closed. Interrupted replay cleanup stops
the measured root before the monitor jobs, snapshots descendants with depth and
PPID, rechecks start times before every signal, skips `Z`/`X`/`x` states, tolerates
legitimate descendant reparenting after TERM, and bounds every cleanup wait.
Controls, ready/launch markers, raw sample streams, monitor logs/statuses, and
terminal evidence are all creation-time sealed and reconstructed at final
admission.

Formal source-bound completion also fixes the cache-state admission controls:
capture residency after eviction must be exactly zero bytes, corpus residency
after eviction must be exactly zero bytes, and global Linux
`Dirty+Writeback` must be at most 67,108,864 bytes. The producer records
`getconf PAGESIZE` in the controlled plan. Because `fincore --bytes` reports
resident pages rather than logical EOF bytes, each file row may be no larger
than its logical size rounded up to that recorded page size; final admission
reconstructs the exact inventoried path order, file and total rows, ceiling,
and page-rounded bound. The canonical matrix contains eight capture-residency
admissions, 40 query pre-run/post-eviction corpus admissions, 40 post-run
corpus observations, and 50 writeback admissions (eight replay, two verifier,
and 40 query).

The formal source-bound path accepts only the committed
`phase6_codec_queries.json`; every range entry fixes the range scalar cache at
zero bytes. It requires `PERF_STAT_MODE=required` and effective perf `on`, with
the exact ordered event set `task-clock`, `cycles`, `instructions`, `branches`,
`branch-misses`, `cache-references`, `cache-misses`, `page-faults`,
`context-switches`, and `cpu-migrations`.
The runner resolves one canonical `perf` executable, records its absolute
path, SHA-256, and one-line version in the controlled plan/settings, invokes
only that path, and rechecks its identity at every seal boundary and admission.
Query read counts and coalesced bytes measure file reads issued by the process,
while residency describes observed operating-system page-cache state for the
inventoried files. Neither is block-device traffic, an operating-system
cache-miss count, or proof of a cold device/controller cache.

Run the isolated lifecycle, corruption, admission, and codec-gate coverage
without building Rust or replaying the corpus:

```sh
python3 docs/experiments/storage_vnext/test_phase6_codec_ab_gate.py
python3 -m py_compile docs/experiments/storage_vnext/phase6_codec_ab_gate.py
bash -n docs/experiments/storage_vnext/phase6_codec_ab_run.sh
shellcheck docs/experiments/storage_vnext/phase6_codec_ab_run.sh
```

The result template, capacity contract, formal command contract, and explicit
non-promotion rules are in
[`2026-07-22-phase6-codec-results.md`](2026-07-22-phase6-codec-results.md).
