# Phase 6 sample and timestamp codec results

**Status:** measurement template; no result has been admitted yet.

This report must be completed only from a result directory containing both the
zero-length `RAW_GORILLA_COMPLETE_TIMESTAMP_AB_BLOCKED` completion marker and
`TIMESTAMP_CODEC_AB_BLOCKED.txt`. A dry run, a partial run, a run with guardian
conflicts, or an artifact that lacks the strict equivalence JSON is not evidence
for promotion. `metadata/result-artifacts.sha256` must also validate every
retained input, authority, raw observation, comparison, and completion marker;
the result-local Cargo cache and build target are explicitly non-evidence.
The marker is invalid unless `metadata/final-admission.json` reports `pass`.
That admission must validate the exclusive, read-only
`metadata/raw-authorities.tsv` and its checksum, rehash every listed raw leaf
before and after recomputation, and byte-rebuild every gate-derived JSON/TSV
from the frozen harness, controlled plan, and sealed raw observations. Missing,
extra, stale, or tampered evidence is a gate failure; inventorying it is not an
admission decision. The frozen harness and direct formal-build metadata use
fixed filename matrices. Only the isolated build home, Cargo home, build target,
and sealed source snapshot are dynamic trees, and none may smuggle an extra
top-level evidence file into the admitted or final-sealed result. After the
checksum authority is written, the frozen gate reinventories the exact matrix
and rehashes every retained artifact to reject late additions or mutation.

The formal marker is available only when `binary_provenance_mode=internal` and
`promotion_eligibility=formal_source_bound`. That path requires a clean sealed
Git tree and Cargo.lock, captures one immutable commit OID, and builds only from
its exact read-only `git archive` extraction. The sanitized isolated
`--locked --release` build rejects Cargo configs above the external build root
or in its fresh `CARGO_HOME`. The runner continuously rechecks the live source,
archive, extracted-tree, frozen-harness, controlled-input, and preserved-binary
authorities. The `external-exploratory` mode cannot emit the marker and is
non-promotable even when every behavioral comparison passes.

## Decision

- RawF64 versus Gorilla: **TBD after controlled real-corpus A/B**.
- Adaptive per-chunk Float selection: **deferred/open under this evidence
  contract**. The verifier reports exact aggregate totals, wins, selections,
  and histograms, but not a streamed row for every physical chunk. The
  whole-corpus RawF64/Gorilla A/B is valid; a literal per-block selection-policy
  audit is not.
- Timestamp codec: **runtime A/B blocked**. The current verifier can calculate
  exact evidence-only candidate sizes, but no versioned writer/reader/config
  selector can emit and query those layouts. Native-payload timestamp evidence
  also excludes duplicate typed scalar-lane timestamps.

Do not turn the timestamp size model into a capacity or performance claim.
Activation requires a byte-exact versioned layout, deterministic selection and
tie rule, writer/reader support, corruption tests, and the same replay/query
gate used for RawF64/Gorilla.

## Experiment contract

Record the following directly from `metadata/settings.txt`,
`metadata/binaries.tsv`, and `metadata/source/`:

| Field | Value |
| --- | --- |
| Result root | `<absolute path>` |
| Git commit and source patch | `<commit; patch SHA-256>` |
| Git tree / tracked-index / Cargo.lock seal | `<digests>` |
| Sealed commit archive / extracted-tree identity | `<digests; pass>` |
| Cargo config isolation / frozen harness binding | `<pass>` |
| Binary provenance / promotion eligibility | `<internal; formal_source_bound>` |
| Build command, profile, features, target, sanitized env | `<metadata/build/*>` |
| Build log/status and pre/post/final source checks | `<pass>` |
| Ingester/query/verifier SHA-256 | `<digests>` |
| Capture inventory SHA-256 | `<digest>` |
| Config template SHA-256 | `<digest>` |
| Query manifest | `committed metadata/harness/phase6_codec_queries.json only` |
| Replay schedule | `odd Raw,Gorilla,Gorilla,Raw; even reversed` |
| Query schedule | `odd Raw,Gorilla,Gorilla,Raw; even reversed` |
| Range scalar cache | `0 bytes for every range query` |
| Host/kernel/toolchain | `<metadata/environment.txt summary>` |
| Producer page size | `<getconf PAGESIZE; plan/settings match>` |
| Capture/corpus residency ceilings | `0 / 0 bytes after eviction` |
| Dirty + Writeback ceiling | `67,108,864 bytes` |
| perf availability | `<on; source-bound execution fixes PERF_STAT_MODE=required and effective-on>` |
| perf identity | `<canonical path; SHA-256; one-line version>` |
| Conflict guardian | `<pass; zero conflict rows>` |
| Guardian cadence | `<samples; terminal boundary; edge-inclusive maximum gap; fixed 100 ms interval>` |
| Capacity contract | `<digest; prebuild/postbuild/final pass>` |
| Replay launch / RSS / capacity monitors | `<both first samples before release; launch observed; terminal boundaries; minimum free bytes; all pass>` |

The continuous guardian rejects build systems and wrapped build tools (including
Soong/Android, Cargo/nextest, versioned Ninja/compiler/linker names), profilers
and tracers, database servers, other Chronoxide processes, emulators, and
interactive system monitors. Generic Java/IDE activity is rejected when its
command identifies Android or Gradle work; other unrecognized activity remains
part of the operator's quiet-host confirmation. Phase 6 conservatively treats
every `qemu-system*` or `qemu-kvm` process as a conflict.

The conflict precheck and continuous guardian use the same classifier and
exclude only the runner's exact observed PID ancestry. The guardian binds the
runner's PID, PPID, and `/proc` start time and fails closed if that identity
disappears, becomes a zombie, or is reused. The continuous scan is fixed at
100 ms. Its raw heartbeat records start-relative samples plus one terminal
boundary, and final admission reconstructs the edge-inclusive maximum over
`[start, samples..., terminal]`; fewer than two samples, non-monotonic
timestamps, or any gap above 200 ms fails. A conflict or
operational-disk-floor crossing terminates the owned runner/process tree before
the guardian fails. The header-only conflict file is therefore necessary but
not sufficient evidence: `guardian-samples.tsv` and its independently rebuilt
summary must also pass. A measured or validation child cannot start until a
bounded ready handshake proves the guardian has completed and flushed its
first full process/capacity scan.

Every replay has an additional causal launch barrier. The runner first starts
a held wrapper, then binds that root plus distinct RSS and capacity monitor
processes by PID, PPID, and start time in an atomically published mode-0444
control. The two monitors each flush one root-starttime-bound sample and publish
their own immutable ready marker. Only after both markers validate does the
runner atomically publish the launch marker. Both monitors must subsequently
observe that marker, record at least two samples, and emit a terminal boundary;
their independently rebuilt cadence also covers `[start, samples..., terminal]`
with the exact 100 ms interval and 200 ms ceiling. Interrupted cleanup stops
the root before its monitor jobs, signals only captured live identities in
deepest-first order, tolerates legitimate descendant reparenting after TERM,
and uses bounded reaps. Partial controls, marker/control mutation, PID reuse,
and `Z`/`X`/`x` process states fail without signalling an unbound or reused PID.

Formal measurement admission fixes
`max_capture_resident_bytes_after_evict=0`,
`max_corpus_resident_bytes_after_evict=0`, and
`max_dirty_writeback_bytes=67108864`; a different value is not a formal
comparison. The producer binds `getconf PAGESIZE` into the admission plan and
settings. Residency TSVs use exact phase/sequence/kind/resident/size/ceiling/
path columns plus one total row. Final admission matches every file row to the
canonical NUL-delimited inventory and permits `fincore --bytes` residency only
up to `ceil(logical_size / producer_page_size) * producer_page_size`, because
the kernel accounts resident pages beyond a non-page-aligned EOF. The zero
aggregate ceiling remains authoritative for the two pre-run eviction gates;
post-run rows are observations, not admissions.

The canonical raw matrix is exactly eight capture-residency admissions, 40
query pre-run/post-eviction corpus-residency admissions, 40 query post-run
corpus observations, and 50 writeback admissions: eight before replay, two
before the exhaustive verifiers, and 40 before queries. Writeback TSVs must
contain one to 30 ordered attempts, recompute bytes as
`(Dirty_kib + Writeback_kib) * 1024`, label every nonterminal row `retry`, and
stop at the first `pass` row at or below 67,108,864 bytes. Final admission
reparses every raw TSV and reproduces these counts and controls in
`measurement_preconditions`; an empty, malformed, reordered, missing, extra,
or phase-swapped artifact fails even when its checksum is internally
consistent.

Before creating the result directory, the runner uses the pinned Phase 1 gate
to hash-validate the exact capture, config template, and four-million-message
prefix. The frozen `phase1_4m_expectations.json` and current explicit Float
framing authorities derive this fail-closed capacity model:

| Capacity fact | Bytes |
| --- | ---: |
| Pinned Gorilla corpus bound | 5,569,314,896 |
| Float points / Raw value bytes | 141,374,001 / 1,130,992,008 |
| Raw corpus bound (`baseline + 8*N`) | 6,700,306,904 |
| Gorilla / Raw reserve after exact 110% ceiling | 6,126,246,386 / 7,370,337,595 |
| Four Gorilla plus four Raw reserves | 53,986,335,924 |
| Operational floor | 21,474,836,480 (20 GiB) |
| Build/source/result allowance | 10,737,418,240 (10 GiB) |
| Initial free-space requirement | 86,198,590,644 |
| Post-build free-space requirement | 75,461,172,404 |

The proof requires zero Int64 chunks in the pinned verifier facts, subtracts
accepted Histogram, ExponentialHistogram, and Summary points from all recorded
samples to derive Float points, and binds the 40-byte common header plus
eight-byte Raw value framing to reviewed source/spec digests at the sealed
HEAD. Any authority or derivation drift fails before output allocation. Every
replay-plan row then carries the current codec bound, safe reserve, future
scheduled reserve, pre-run requirement, and monitor floor. The root-bound
100 ms capacity monitor kills that replay tree if free space would consume the
future reserve or 20 GiB floor. Its raw heartbeat and terminal boundary must
pass the causal launch and edge-inclusive cadence gate above; the post-run
corpus must also remain at or below its unsafetied mathematical bound. Final
admission reconstructs the launch control and markers, both monitor summaries,
the capacity ledger, snapshots, corpus checks, pinned-input validation, and
capacity contract from creation-time-sealed raw evidence.

Both codecs must use the same non-writable preserved binaries, sealed harness,
controlled configs/plans, and source capture. Their expected hashes must pass
immediately before and after every Chronoxide child process and at finalization;
every run must retain its binary hash and sanitized-runtime-environment
identity. Every replay must write to a new isolated output directory. The
normalized configs must share one `controlled_config_sha256`; only codec and
output paths may differ.

The only accepted query schedule is the committed
`phase6_codec_queries.json`; callers cannot substitute another manifest, and
every normalized range query must retain
`range_scalar_cache_max_bytes=0`. A source-bound result requires
`PERF_STAT_MODE=required`, `metadata/perf-effective.txt` equal to `on`, and the
exact ordered perf rows `task-clock`, `cycles`, `instructions`, `branches`,
`branch-misses`, `cache-references`, `cache-misses`, `page-faults`,
`context-switches`, and `cpu-migrations`. The frozen gate strictly rebuilds
the preflight, all eight replay perf records, and all 40 query perf records
from their raw seven-column TSVs; an unavailable, reordered, duplicated, or
noncanonical counter invalidates formal completion. One canonical `perf`
executable is resolved before setup; its absolute path, SHA-256, and one-line
version are plan/settings authorities, every launch uses that path, and every
seal/admission rechecks the tuple.

Each replay, verifier, readback, and query process retains a read-only invocation
and runtime identity. Its raw outputs are sealed before their first downstream
transform (and before the next measured launch). The final raw authority is
exclusive-created from creation-time seal digests only after the guardian has
stopped, then checksum-sealed before final source checks and independent
admission. Corpus/capture traversal is fail-closed: inaccessible entries,
symlinks, non-regular files, or unsafe paths invalidate the result.

## Correctness and determinism

Source: `comparisons/replay-equivalence.json`,
`comparisons/verifier-equivalence-and-codec-inventory.json`, both readback
JSON files, and before/after corpus inventories.

| Gate | Result |
| --- | --- |
| Same-codec corpus bytes deterministic across repetitions | `<pass/fail>` |
| Replay counters and event-time policy identical | `<pass/fail>` |
| Segment path set and deterministic IDs identical | `<pass/fail>` |
| Exhaustive footer and exact-postings verification | `<pass/fail>` |
| Decoded semantic fingerprint identical | `<digest/pass>` |
| Physical selection fingerprint differs as intended | `<raw / gorilla>` |
| Independent readbacks | `<executed/expected; skips; mismatches>` |
| Timed query semantic/portable fingerprints | `<pass/fail>` |
| QueryStats | `<pass; bytes_read is the sole declared Float-query difference; typed controls equal>` |
| Corpora unchanged during verification/query | `<pass/fail>` |

Unexpected differences outside `chunks.bin`, `ooo_chunks.bin`, `series.bin`,
`chunk_index.bin`, and `footer.bin` fail the replay gate. Those allowed physical
differences are not themselves correctness evidence; the decoded fingerprint,
readbacks, and query fingerprints remain mandatory.

## Corpus and codec inventory

Source: representative verifier reports, per-replay `artifacts.json`, and
`verifier-equivalence-and-codec-inventory.json`.

| Metric | Raw | Gorilla | Delta / ratio |
| --- | ---: | ---: | ---: |
| Complete corpus bytes | `<n>` | `<n>` | `<...>` |
| `chunks.bin` bytes | `<n>` | `<n>` | `<...>` |
| Logical indexed chunk bytes | `<n>` | `<n>` | `<...>` |
| Float chunks / points | `<n / n>` | `<n / n>` | `equal` |
| Float native payload bytes | `<n>` | `<n>` | `<...>` |
| Non-Float native payload bytes | `<n>` | `<n>` | `equal` |

Report the aggregate candidate inventory, not only its savings percentage:

| Candidate evidence | Chunks | Points | Indexed bytes | Payload bytes |
| --- | ---: | ---: | ---: | ---: |
| Existing corpus selection | `<n>` | `<n>` | `<n>` | `<n>` |
| All RawF64 | `<n>` | `<n>` | `<n>` | `<n>` |
| All Gorilla | `<n>` | `<n>` | `<n>` | `<n>` |
| Per-chunk adaptive minimum | `<n>` | `<n>` | `<n>` | `<n>` |

Include Raw wins, Gorilla wins, ties, adaptive selections, IEEE
zero/finite/infinity/ordinary-NaN/stale-NaN counts, repeated XORs, reused/new
Gorilla windows, and the XOR significant-width histogram. Ties select RawF64;
decode cost must still be considered before adaptive activation.

Bounded evidence waiver: current rows are accumulated by `(kind, encoding)`
and accompanied by aggregate histograms. They are not a literal per-physical-
chunk/schema-shape inventory. Use them to screen all-Raw, all-Gorilla, and an
adaptive upper bound, but do not promote an adaptive policy until an exhaustive
streamed per-block sidecar (or equivalent independently auditable artifact)
records the candidate bytes, selected codec, tie outcome, kind, encoding, point
count, and timestamp shape for every chunk.

## Replay and seal performance

Source: `replay-summary.tsv`, per-run GNU time, `/proc` RSS samples, seal
telemetry, and perf JSON.

Report every observation plus paired/blocked medians; do not quote only the
best run.

| Metric | Raw median | Gorilla median | Raw/Gorilla | Dispersion |
| --- | ---: | ---: | ---: | ---: |
| Replay wall | `<...>` | `<...>` | `<...>` | `<...>` |
| User CPU | `<...>` | `<...>` | `<...>` | `<...>` |
| Process-tree peak RSS | `<...>` | `<...>` | `<...>` | `<...>` |
| Head-window total | `<...>` | `<...>` | `<...>` | `<...>` |
| Seal decode | `<...>` | `<...>` | `<...>` | `<...>` |
| Record samples | `<...>` | `<...>` | `<...>` | `<...>` |
| Writer flush | `<...>` | `<...>` | `<...>` | `<...>` |
| Cycles per stored sample | `<...>` | `<...>` | `<...>` | `<...>` |
| Branch misses per stored sample | `<...>` | `<...>` | `<...>` | `<...>` |
| Cache misses per stored sample | `<...>` | `<...>` | `<...>` | `<...>` |

The deployed config couples head Float compression and sealed Float encoding,
so replay CPU is an end-to-end policy comparison. Attribute seal/write phases
from the emitted telemetry; do not label the whole replay difference as codec
CPU.

## Query performance

Source: `query-summary.tsv`, `comparisons/query-equivalence.json`, per-process
GNU time/perf, and residency snapshots. The CLI's first evaluation in a fresh
process is called cold; this means a fresh query session after the harness's
`POSIX_FADV_DONTNEED`/`fincore` gate, not a guaranteed cold storage device.
The residency gate describes observed operating-system page-cache residency
for the inventoried files. Process-issued read calls and coalesced span bytes
do not measure operating-system cache misses, block-device requests, media
traffic, or controller-cache state.

| Query | Decode path | Run kind | Raw median | Gorilla median | Raw/Gorilla | Peak RSS ratio | Raw/Gorilla bytes read |
| --- | --- | --- | ---: | ---: | ---: | ---: | ---: |
| `float_full_last` | Float full | cold | `<...>` | `<...>` | `<...>` | `<...>` | `<...>` |
| `float_full_last` | Float full | warm | `<...>` | `<...>` | `<...>` | `<...>` | `<...>` |
| `float_scalar_rate_instant` | Float scalar evaluation | cold/warm | `<...>` | `<...>` | `<...>` | `<...>` | `<...>` |
| `float_scalar_rate_range` | Float range | cold/warm | `<...>` | `<...>` | `<...>` | `<...>` | `<...>` |
| `typed_scalar_lane_control` | Typed scalar lane control | cold/warm | `<...>` | `<...>` | `<...>` | `<...>` | `equal` |
| `typed_full_control` | Typed full control | cold/warm | `<...>` | `<...>` | `<...>` | `<...>` | `equal` |

Also report logical payload-used bytes, process-issued coalesced bytes,
process-issued read calls, read/used amplification, samples decoded, cycles,
branches, branch misses, and cache misses. The typed controls should remain
neutral; they detect unrelated corpus or query-path drift. For both typed
controls,
`QueryStats.bytes_read` and therefore logical payload-used bytes must be equal.
Process-issued coalesced span bytes and physical-read counts may differ because
Float payload-size changes shift otherwise identical typed chunks on disk; do
not mislabel such an offset/coalescing effect as typed logical work.

## Timestamp candidate inventory

Source: `timestamp_candidates` in the verifier comparison. Preserve its
`scope`, `tie_rule`, and `selector_bytes_included=false` fields.

| Candidate | Native-payload bytes | Unique-win chunks/points | Adaptive selections |
| --- | ---: | ---: | ---: |
| Current offset ULEB | `<n>` | `<n / n>` | `<n / n>` |
| Adjacent delta ULEB | `<n>` | `<n / n>` | `<n / n>` |
| Delta-of-delta ZigZag ULEB128 | `<n>` | `<n / n>` | `<n / n>` |
| Fixed-step residual bitpack | `<n>` | `<n / n>` | `<n / n>` |
| Adaptive minimum | `<n>` | n/a | n/a |

Break the same totals down by timestamp shape and `(kind, encoding)`. State
the scalar-lane exclusion prominently. This section is a format-candidate
screen only; there are no replay, decode, corruption, or query measurements for
the three non-current layouts. These breakdowns are aggregate rows, not the
per-block sidecar required for a literal adaptive-policy audit.

## Promotion analysis

Promote Gorilla, RawF64, or a deterministic adaptive Float policy only if all
correctness gates pass and the candidate is on an acceptable end-to-end
capacity/CPU/RSS/query frontier. Reject or defer when any of the following is
true:

- the capacity change is small relative to the complete corpus;
- replay/seal CPU or RSS regresses materially;
- full/scalar cold or warm query latency regresses materially;
- the win depends on one noisy observation or order position;
- perf and latency disagree without an explained mechanism;
- adaptive bookkeeping/version complexity exceeds the measured benefit;
- the literal per-block adaptive-policy evidence gap above remains open; or
- verifier peak RSS approaches an unacceptable decoded amplification bound.

Record the final choice, production default, retained comparator, exact
thresholds, and whether a storage-spec/version change is required. If evidence
is neutral or mixed, defer; Phase 6 does not require inventing a winner.

## Reproduction

```sh
CAPTURE=/absolute/capture \
CONFIG_TEMPLATE=/absolute/config.toml \
REPO_ROOT=/absolute/chronoxide-worktree \
BINARY_PROVENANCE_MODE=internal \
PERF_STAT_MODE=required \
MAX_CAPTURE_RESIDENT_BYTES_AFTER_EVICT=0 \
MAX_CORPUS_RESIDENT_BYTES_AFTER_EVICT=0 \
MAX_DIRTY_WRITEBACK_BYTES=67108864 \
QUIET_HOST_CONFIRMED=1 \
RUN_NOTE='quiet host; no builds, Android/Docker, profiler, or database workload' \
RESULT_DIR=/absolute/new/phase6-codec-result \
  docs/experiments/storage_vnext/phase6_codec_ab_run.sh
```

The worktree must be clean, the harness must be committed, and the runner must
use its committed Phase 6 query manifest before this formal mode can run.
Caller-supplied binaries are accepted only with
`BINARY_PROVENANCE_MODE=external-exploratory`; those runs deliberately produce
`EXPLORATORY_EXTERNAL_BINARIES_NON_PROMOTABLE.txt` instead of the formal marker.

Run the harness contracts first:

```sh
python3 docs/experiments/storage_vnext/test_phase6_codec_ab_gate.py
bash -n docs/experiments/storage_vnext/phase6_codec_ab_run.sh
shellcheck docs/experiments/storage_vnext/phase6_codec_ab_run.sh
```
