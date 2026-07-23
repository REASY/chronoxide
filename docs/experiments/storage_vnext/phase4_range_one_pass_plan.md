# Phase 4 repeated versus one-pass-assume-scalar range comparator

This is a diagnostic same-binary comparator, not a production promotion gate.
It answers whether avoiding per-step storage planning and decode is worth
finishing the governed and finite-limit design. It cannot establish that the
current `one-pass-assume-scalar` implementation is safe as a default.

## Fixed matrix

All queries use a 15-minute `rate()` window and a 5-minute outer step. The
grouping label is `service_name_x55e50a58f9befba7` and the exact metric is
`container_cpu_usage_seconds_total`.

| Query | Root aggregation | Outer range | Evaluations | Evidence class |
| --- | --- | ---: | ---: | --- |
| `scalar_rate_sum_range_30m` | `sum by(...)` | 30m | 7 | dense real window |
| `scalar_rate_count_range_30m` | `count by(...)` | 30m | 7 | dense real window |
| `scalar_rate_sum_range_6h` | `sum by(...)` | 6h | 73 | sparse scheduler control |
| `scalar_rate_sum_range_24h` | `sum by(...)` | 24h | 289 | sparse scheduler control |

The accepted corpus contains only 4,500,000 ms (1.25 hours) of dense
event-time coverage. The manifest and result validator therefore reject any
attempt to label the 6-hour or 24-hour points as dense evidence. They measure
step-scheduling overhead over a mostly empty outer interval; they are not
24-hour capacity, memory, or latency evidence.

The corpus identity is not inferred from a path. The sealed manifest pins the
Phase 1 segment-manifest digest
`8b0789e2f6c404a144e0d2e87f152a83e9f0bedb9c5ab2c6512608056cae3289`,
the gate's canonical inventory digest
`28547c0fc2b738eb58948400602640c017844cd57bd49917bffdf100a6e14a0b`,
the query binary's corpus fingerprint
`7e5cf252e5df9bdb786e1b9deb9248f09667962ac559f339ba47312c5c0e3ca3`,
66 regular files, and 5,569,314,896 bytes. Inventory creation fails before a
measurement if any value differs.

## Controlled execution

One query binary built by the result itself runs both
`--range-execution-mode repeated` and
`--range-execution-mode one-pass-assume-scalar`. The build input is a
read-only extraction of one clean Git object ID, not the live checkout. Cargo
uses an isolated home and target directory and executes the exact sealed argv
`cargo build --locked --release --target <host> -p chronoxide-ingester --bin
chronoxide-query` with default features. The Cargo/rustc/rustup paths, bytes,
versions, build environment, Cargo metadata, build log/status, source archive,
and snapshot seal are bound to the preserved binary. Four alternating
ABBA/BAAB blocks place each arm twice in every schedule position, yielding
eight fresh processes per arm and query. Every process records one cold and
two warm evaluations. A cold CLI run means the first query in a fresh session,
not a cold storage device.

The fixed settings are:

- Schema 8, DemandDriven labels, CompactIds, and a 512 MiB compact-label arena;
- pread with queue depth 128 and a 4 KiB payload-coalescing gap;
- query instrumentation off and range scalar cache budget zero;
- unlimited public `QueryLimits`; and
- footer validation and independent readbacks outside timed processes.

Before every process, `POSIX_FADV_DONTNEED` is applied to the inventoried
corpus and `fincore` must report exactly zero resident corpus bytes. The formal
bound is fixed at zero and cannot be relaxed by configuration. Both the
maximum after-eviction and after-run observations are retained in the result.
This is Linux page-cache evidence only; it does not flush device or controller
caches.

A non-dry run requires explicit quiet-host confirmation and a single-line run
note. Immediately before every timed process, a new process snapshot is
checked by the frozen gate. Footer validation, independent readback, and every
timed query use one shared held-root primitive. Before release it atomically
publishes an exact mode-0444 JSON control binding the runner, workload root, and
guardian by PID and Linux `/proc/<pid>/stat` starttime; the root and guardian
PPIDs must both remain the runner PID, and the runner's own observed PPID is
also sealed. The workload cannot execute until the guardian has written and
`fsync`ed its first clean raw sample and then published an empty mode-0444 ready
marker. The runner subsequently invokes the frozen helper to publish the empty
mode-0444 launch marker.

The guardian samples at the fixed 100 ms interval. Raw monotonic timestamps
must independently reconstruct an edge-inclusive maximum gap of at most 200 ms
from monitor start to the first sample, between every pair, and from the final
sample to terminal elapsed time. The first sample has the held root live and no
launch; launch observation is later and monotonic; the final mandatory sample
has the exact root identity absent or dead. Runner/root/guardian identity or
parent drift, marker mutation, cadence failure, or any transient forbidden
process terminates the captured workload identity and fails the run. Cleanup
captures descendants deepest-first, revalidates PID/starttime before every
TERM/KILL so reparenting is safe, and cleans the workload root before the
guardian. `EXIT`, `HUP`, `INT`, and `TERM` traps cover active children with
bounded identity-safe reaping, including the fork-to-starttime-binding window.

Known build, database, profiler, replay, Soong, Kati, Android tool, adb, btop,
htop, top, or other Chronoxide processes fail the formal run. Classification
covers executable and command-path variants, including versioned or `.real`
compiler/Ninja names, `cargo-nextest`, linker variants, and the
`soong_ui.bash` wrapper. QEMU/KVM and Java processes fail when materially active
or when their command line identifies Android, redroid, artracer, Soong, or
related build/VM work; an idle generically named QEMU process is not rejected
solely by name. The operator must separately confirm no unrecognized
conflicting workload is active; there is no noisy-host override. All static
snapshots, immediate scans, raw samples, empty conflict streams, controls,
markers, summaries, statuses, and logs are sealed and independently
reconstructed at final admission for all 66 workload lifecycles.

This experiment reads an immutable corpus and writes only a comparatively small
evidence tree. It deliberately has no automated disk-capacity admission or
self-selected free-space floor. The operator must manually verify ample free
space on the result filesystem before a non-dry run; this limitation is not a
model for replay or transform experiments that create large corpora.

## Raw-v14 and correctness contract

Every finalized range call exposes a `range_execution` summary with the
requested/effective mode, fallback and terminal reasons, evaluation count,
union bounds, source series/sample counts, estimated retained-byte peak,
post-finalize retained bytes, preallocation-governance capability, and scalar
cache bypass status.

The candidate arm must execute as `one-pass-assume-scalar` with no pre-I/O
fallback and no post-decode terminal reason. Typed source observed after union
decode is a terminal failure, never a repeated-executor retry. Candidate union
bounds must be exactly `[outer_start - 15m, outer_end]`, all scheduled steps
must be evaluated, and retained bytes after finalization must be zero.

Any successful comparator run that reports a typed scalar or full chunk is
also rejected. A candidate summary that says the scalar cache was bypassed
must have zero cache hits, admissions, charges, misses, and bypass bytes;
repeated execution on this CompactIds path must report zero cache misses and
streaming-budget bypasses, one unsupported bypass per logical chunk read, and
miss-or-bypass bytes equal to logical payload bytes.

Exact and portable result fingerprints, result series/sample shape, and result
order must agree for every cold/warm repetition and every process. Storage
work may not drift from cold to warm inside a process. QueryStats, payload
reads and read/used amplification, scheduler counters, label materialization,
compact-label storage, symbol reads, metadata runtime, scalar-cache reports,
off-mode stage counters, and range-execution summaries must be deterministic
within each arm and are copied into the canonical result. Ordinary
`QueryStats` are deliberately not required to be equal: repeated execution
charges per-step logical work, while the comparator reports union work. The
gate requires every field difference to be emitted as
`union-work-vs-repeated-logical-work` (or `equal`) and rejects a
`one-pass-assume-scalar` value that exceeds the corresponding repeated work
counter.

Every non-`QueryStats` component is compared by canonical digest at each
query/run coordinate and classified as equal or an explicit
execution-strategy accounting difference. The result validator recomputes all
component digests and payload amplification values from embedded evidence.

The untimed independent readback pass must execute at least one Phase 4
multi-step range case. Expected and executed multi-step counts must match,
with zero skips and zero mismatches; generic one-step readback coverage is not
sufficient for this comparator.

## Why promotion is forbidden

The result gate always emits `production_promotion_verdict: forbidden` and
`candidate_disposition: defer`, regardless of observed latency. It validates
four unresolved blockers:

1. `one-pass-assume-scalar` union-result preallocation is estimated after
   decode but is not governed;
2. finite query limits and their error precedence are not exercised;
3. public `QueryStats` have intentionally different work semantics; and
4. this corpus supplies no dense 24-hour range.

A later promotable implementation needs reservation or bounded streaming
before allocation, specified finite-limit/stat semantics with error-precedence
tests, and a fingerprinted corpus containing at least 24 dense event-time
hours. This diagnostic can justify that engineering work; it cannot substitute
for it.

## Reproduction and artifact contract

The executable entry point is `phase4_range_one_pass_run.sh`. A formal run
requires an ordinary clean tracked index/worktree and rejects untracked or
ignored source/build inputs. The only excluded untracked files are the named
runtime ingestion reports and Python bytecode caches; Python never trusts
those caches. The pinned absolute interpreter always runs with `-I -S -B`, and
the gate compiles every transitive Python dependency directly from the exact
read-only frozen `.py` bytes. A planted valid `.pyc` therefore cannot replace
gate code.

The frozen harness is an exact allowlist with a shell-held checksum authority.
The source seal, archive checksum, extracted snapshot, Cargo-configuration
isolation, build records, binary, query controls, inventory, and eviction
helper have independent read-only authorities that are asserted around gate
use and measured processes. Immediately after each validation or timed query
process, before result transformation or the next measured launch, its exact
raw leaf allowlist is made read-only and covered by an exclusive
`*-leaves.sha256` authority. Every later experiment seal check rechecks all
prior leaf authorities.

Final admission does not trust generated TSV or JSON merely because it was
hashed. Starting from the frozen manifest, per-process argv/time/exit/raw JSON,
residency triples, validation Markdown, and process snapshots, it independently
reconstructs and byte-compares normalized queries, the run plan, inventory path
streams, raw index, residency summary, validation JSON, comparison result, and
human summary. This includes exact RSS/time field validation and a new
comparison run over the reconstructed raw index.

The final artifact walk uses `scandir` and is fail-closed. It rejects every
symlink, FIFO/socket/device, unsupported root file or directory, traversal
failure, missing required directory, and unexpected path. Separate canonical
NUL inventories cover all regular files and all directories, including the
read-only source snapshot; `metadata/result-artifacts.sha256` then covers the
exact file inventory plus both inventory authorities. `COMPLETE` exists before
that traversal and is itself inventoried and hashed. Evidence paths and
directories must equal the derived matrix; only `build-target/` and
`metadata/build/cargo-home/` are explicitly dynamic non-evidence subtrees, and
source-snapshot membership comes from the one-OID snapshot seal. The final
verifier repeats the traversal, source/snapshot/toolchain/build checks,
leaf-authority checks, and leaf-derived recomputation before accepting the
artifact. Paths used in NUL/TSV evidence must not contain tabs, CR, or LF.

## Diagnostic invocation and result slot

Status: one formal diagnostic completed and passed final admission. The
experiment remains nonpromotable; its reviewed evidence and defer decision are
recorded in
[`2026-07-23-phase4-range-one-pass-results.md`](2026-07-23-phase4-range-one-pass-results.md).

```sh
RUN_NOTE='quiet host; disk space manually confirmed; no build, replay, profiler, monitor, or unrelated database work' \
QUIET_HOST_CONFIRMED=1 \
DISK_SPACE_CONFIRMED=1 \
RESULT_DIR=/absolute/new/storage-vnext-phase4-range-one-pass-$(date +%Y%m%d-%H%M%S) \
  docs/experiments/storage_vnext/phase4_range_one_pass_run.sh
```

Result: [2026-07-23 Phase 4 one-pass range-query results](2026-07-23-phase4-range-one-pass-results.md).
The diagnostic found large, repeatable latency reductions with exact semantic
equivalence, but the gate deferred the candidate for the four blockers above.
