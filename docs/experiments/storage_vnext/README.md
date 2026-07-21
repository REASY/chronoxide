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
VNEXT_UNTRACKED_TASK_SOURCES='chronoxide-core/src/storage/symbols.rs:docs/experiments/storage_vnext/2026-07-13-prefix-results.md:docs/experiments/storage_vnext/README.md:docs/experiments/storage_vnext/ab_gate.py:docs/experiments/storage_vnext/fadvise_regular_dontneed.c:docs/experiments/storage_vnext/query_ab_gate.py:docs/experiments/storage_vnext/query_ab_run.sh:docs/experiments/storage_vnext/storage_format_ab_run.sh:docs/experiments/storage_vnext/storage_inventory.py:docs/experiments/storage_vnext/test_ab_gate.py:docs/experiments/storage_vnext/test_query_ab_gate.py:docs/experiments/storage_vnext/test_storage_inventory.py:docs/superpowers/specs/2026-07-13-storage-read-layout-review.md:docs/superpowers/specs/2026-07-13-storage-vnext-paged-symbols-design.md' \
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
