# Bounded Allocator-Policy Full Gate Design

**Date:** 2026-07-22

**Status:** Implemented harness; formal measurement not started

**Scope:** Phase 5 allocator candidate confirmation; Linux GNU replay ingestion

## Decision

The 250,000-message allocator screen may nominate at most one bounded J1-J3
policy. A nomination is not a production decision. The nominated policy must
pass two source-bound four-million-message stages:

1. the screen's exact `jemalloc-stats` binary in `S,C,C,S` order;
2. a newly built plain `jemalloc` binary in `S,N,N,S` order.

`S` is the screen's preserved system-allocator binary, `C` is its preserved
stats-enabled candidate, and `N` is the no-stats binary built by this gate.
Both stages retain two observations per role and use the same midpoint,
dispersion, CPU, RSS, HWM, and released-RSS bounds as the screen.

The harness always records `production_promotion_authorized: false`. Passing
both stages means only that the policy is eligible for manual promotion review.
Changing the default allocator still requires an explicit reviewed code and
specification decision.

## Admission boundary

The full gate is not a second policy selector. It consumes one completed
screen and fails unless:

- `COMPLETE` and the screen's final decision are present;
- all screen artifacts match the screen artifact authority;
- the final decision names exactly one eligible J1, J2, or J3 policy;
- the screen summary names the same policy;
- the bounded policy text comes from the screen's frozen plan;
- the screen decision remains explicitly non-promotional;
- source archive, extracted-source seal, build provenance, control seals,
  Phase 1 helper/expectations, and all four preserved executables remain bound;
- the executing full-gate runner, gate, plan, and tests byte-match the files in
  the screen's read-only Git-archive extraction.

Initial and final admission invoke the screen's own frozen
`validate-final-artifacts --stage complete` authority. The required screen
completion bytes are `chronoxide/allocator-screen-complete/v1\n` at mode 0444,
and its exact file/directory inventories, digest manifest, and
`metadata/FINAL_SEAL_VALIDATED.json` must all pass. An empty legacy marker or a
manifest-only screen is rejected.

The full plan deliberately contains no Phase 1 or Phase 6 helper digest. The
completed screen's frozen artifact and control authorities supply those
identities. This avoids a moving helper hash in a later experiment while still
binding every helper byte that can influence evidence.

## Source and binary provenance

The stats stage reuses the exact system and `jemalloc-stats` executables already
preserved by the completed screen. The query and storage-verifier executables
also come from that controlled build.

The no-stats binary is built with exactly:

```text
cargo build --manifest-path Cargo.toml --locked --release --no-default-features --features jemalloc -p chronoxide-ingester --bin chronoxide-ingester
```

Cargo runs from the completed screen's read-only `git archive HEAD` extraction,
never from a live worktree. It uses a fresh external target directory,
`CARGO_INCREMENTAL=0`, the screen build's toolchain shape, and a sanitized
environment. The build log records the exact command, CWD, and environment
before Cargo output. The preserved no-stats executable must have a new hash and
must not alias any screen binary.

The build toolchain is also byte-bound. `cargo`, `rustc`, and `rustdoc` must be
the canonical `$HOME/.cargo/bin` rustup proxies, resolve to the same rustup
binary and digest, and report the exact Cargo/rustc versions recorded by the
screen's frozen `metadata/environment.txt`. Version probes run with the
read-only source snapshot as their working directory so its pinned toolchain is
authoritative. Immediately after Cargo returns, the gate performs another full
screen-artifact validation and records that result in the no-stats build
provenance before accepting the new executable.

Three fresh application preflights are required:

- system: system Rust allocator, no jemalloc policy or telemetry;
- stats candidate: jemalloc Rust allocator, live 64 MiB probe passed, exact
  nominated requested/effective policy, stats available;
- no-stats candidate: jemalloc Rust allocator, requested/effective policy and
  mallctl telemetry unavailable, live probe explicitly unavailable without
  `jemalloc-stats`.

For both candidate binaries, jemalloc's `confirm_conf:true` output must show
sources 1, 2, 3, and 5 empty, source 4 equal to the nominated environment
policy, and exactly one confirmation for every policy entry. This proof is
required again from every measured candidate process. The no-stats application
does not reinterpret `_RJEM_MALLOC_CONF`; jemalloc's own confirmation output is
the actual-process policy authority for that production-shaped build.

## Workload and schedules

Every observation replays exactly 4,000,000 messages from the capture and
configuration authority frozen by the completed screen. The fixed schedules
are:

```text
stats:    S C C S
no-stats: S N N S
```

The no-stats stage follows the stats stage. Each observation uses the same
schema-8 writer configuration, deterministic segment IDs, capture prefix,
logging, 30-second post-`Ingester` hold, 100 ms external RSS sampling, perf
events, and cache-eviction contract.

Before any 4M replay, a process-scoped `perf stat` probe must demonstrate that
the complete frozen event list can be collected and parsed. Its raw TSV, log,
exit status, and reconstructed JSON are retained as input controls. A failed or
unsupported event aborts before measured work.

The timing scopes remain those defined by the screen:

- workload wall ends at the flushed `ingester_dropped` checkpoint;
- workload CPU is process-tree `utime+stime` at the first post-drop sample;
- workload peak RSS covers only samples labeled `workload`;
- boundary VmHWM is the first post-drop sample's retained kernel HWM;
- end released RSS is the last sample within the post-drop hold;
- GNU time and perf cover the complete process, including the diagnostic hold.

The artificial release hold is never relabeled as ingest latency.

## Host and capacity controls

Formal execution requires a quiet host. A fail-closed preflight scan rejects
build tools, compilers/linkers (including versioned and `.real` variants),
Soong, Cargo Nextest, container builders, profilers/tracers, unrelated TSDBs,
QEMU/Android emulators, `adb`, Gradle workers, and interactive process monitors.

A separate 100 ms guardian runs continuously for every measured process. The
measured launcher subtree is allowed; unrelated matching processes are not.
The guardian must see the root process, complete at least two polls, observe no
conflict, and keep the result filesystem above its schedule-derived free-space
floor. On the first conflict or capacity violation it immediately terminates
the owned measured process tree with bounded `TERM` then `KILL` handling and
records the termination evidence.

There is no unguarded measured prefix. The runner first starts a shell wrapper
that can only wait for a fresh launch marker; `/usr/bin/time`, `perf`, and the
ingester have not executed. It starts the RSS monitor and guardian, then writes
an exact read-only control binding the held-root, guardian, and RSS-monitor PIDs,
their Linux `/proc/PID/stat` starttimes, the 100 ms interval, and the canonical
RSS-ready, guardian-ready, and launch paths. The control is first fully written
and fsynced as a private
same-directory temporary file, changed to mode 0444, and only then published at
the canonical path by an exclusive atomic hard link. The guardian can therefore
observe either no control or the complete finalized control, never writable or
partial JSON. A bounded readiness wait repeatedly verifies the same non-zombie
process identities. The RSS monitor validates the finalized control, samples
the still-held starttime-bound root tree, and exclusively creates an empty mode
0444 RSS-ready marker after its first valid sample. The guardian waits for that
marker before starting its cadence, then exclusively creates its empty mode
0444 ready marker after the first clean root-observing process/capacity poll;
that marker therefore remains guardian poll one. Only after both monitors are
ready does the runner exclusively create the empty mode 0444 launch marker.
The held wrapper independently verifies that launch is regular, non-symlink,
empty, and exact mode 0444 before it enters the timed command. Both monitors
must observe launch on a later poll/sample. Their final JSON binds the control
and marker digests, roles/starttimes, causal poll/sample numbers/timestamps,
and absence of handshake violations; missing, writable, nonempty, premature,
or unobserved markers invalidate the run.

The guardian must also prove its cadence rather than merely report its requested
interval. Every poll records a nonnegative, strictly increasing monotonic
elapsed timestamp. Successful evidence requires at least two polls, the raw
timestamp count must equal the poll count, and every timestamp must lie within
the guardian's measured elapsed time. Final admission reconstructs the maximum
gap across the ordered boundaries `[0, *poll_starts, elapsed_ns]` and requires
the saved derived value and saved allowance to be exact. Including both edges
rejects a startup stall and a terminal deschedule during which the replay exits,
not only a missed interval between two observed polls. For the 100 ms requested
interval, the explicit scheduler-edge allowance is another 100 ms, so any
boundary gap above 200,000,000 ns invalidates the observation.

RSS cadence is independently raw-derived under the same edge-inclusive rule.
The monitor records each sample-start monotonic elapsed timestamp at an exact
100 ms requested interval, must retain at least two samples, creates RSS-ready
on sample one, and observes launch only on a later sample. Final admission
reconstructs its maximum gap over `[0, *sample_starts, monitor_elapsed_ns]`, so
a middle stall, reordered timestamp, or terminal deschedule above 200 ms fails.
It also revalidates the RSS root starttime, full three-role control, canonical
marker paths and digests, and exact zero replay/RSS-monitor/guardian statuses.

Emergency termination is PID-reuse safe. The guardian snapshots each owned
process as `(pid, ppid, state, starttime, depth)`, orders the snapshot by
descending ancestry depth, and sends `TERM` then `KILL` child-before-parent.
Before every signal and during both bounded waits, `/proc/PID/stat` must retain
the captured starttime and a state other than zombie/dead (`Z`/`X`/`x`). A missing,
zombie, or changed-starttime PID is never signaled and cannot keep a guardian
alive; identity refusals and any surviving same-identity processes remain in
the raw termination evidence. Continuous allowed-tree traversal also rechecks
the root starttime after queueing and admits each discovered child only while
its current PPID is the parent from whose `/proc/.../children` file it was read.

Runner signal/error cleanup uses the same sealed control without requiring the
guardian or RSS monitor still to be live. It snapshots and terminates the
starttime-bound measured tree first, then independently stops the starttime-bound
RSS monitor and guardian, and the shell reaps all three jobs. If the control is
absent or rejected, the runner falls back to starttimes captured immediately
after each job started, in the same root-tree/RSS/guardian order. Signal traps
are temporarily changed to record a pending interruption across each
spawn/PID/starttime critical section, then re-armed and the pending signal is
honored immediately; there is no startup interval with a live unbound job.
Before control publication the held wrapper cannot have entered the timed
command. No cleanup path signals a raw unbound PID. Cleanup disarms its signal
traps on entry so a second signal cannot recursively interrupt that sequence.
Reaping also remains bounded: the runner waits only after `/proc/PID/stat`
proves the captured identity absent or dead, refuses a reused identity, polls a
live matching identity for at most 200 iterations with a 10 ms delay, and
records any unbound, unreadable, reused, or still-live job in the interrupted
run directory.

Before creating evidence, the gate requires free space for eight corpora using
the completed screen's frozen expected 4M corpus size, plus 10 GiB build and
10 GiB operational headroom. For the frozen 5,569,314,896-byte expectation,
that initial requirement is exactly 66,029,355,648 bytes. This is a capacity
admission bound, not a claim about final compression ratio.

Immediately before each replay launch, after writeback quiescence and capture
cache eviction, available space on the filesystem containing `RESULT_DIR` must
cover every corpus remaining including the current one plus 10 GiB operational
headroom. The guardian floor covers the remaining corpora after the current
one plus that headroom. The first run uses the frozen expected corpus size;
after its corpus summary is sealed, all later checks use the larger of the
frozen expectation and that first observed size. The extra 10 GiB build
headroom is an initial, prebuild reserve and is not double-counted in per-run
floors. The screen result and full-gate result may be fresh siblings on one
large result filesystem while the capture resides on a different data mount;
there is deliberately no same-filesystem requirement for the capture.

Each replay begins with capture `POSIX_FADV_DONTNEED` and a zero-resident-byte
`fincore` check. Pre-run and post-run global sync/writeback quiescence require
three consecutive 250 ms samples with `Dirty + Writeback <= 65,536 KiB`.

## Correctness and deterministic equivalence

Performance evidence is invalid unless every run has:

- exactly the frozen four-million-message replay-correctness document;
- zero process-tree swap;
- all required perf events available;
- sufficient workload and post-drop RSS phase coverage;
- CPU-boundary uncertainty no greater than one sampling interval;
- a valid two-row checkpoint and bounded 30-second hold;
- role-correct two-row allocator telemetry;
- a conflict-free quiet/capacity guardian;
- passing pre-run and post-run writeback quiescence;
- an exact complete corpus manifest.

All eight replay-correctness documents and corpus summaries must be identical.
The byte manifests therefore prove system, stats-enabled, and no-stats builds
created the same deterministic storage corpus.

Two candidate corpora receive separate, untimed canonical validation:

- the first stats-enabled candidate corpus;
- the first no-stats candidate corpus.

Each receives exhaustive schema-8 footer and exact-postings verification plus
independent `chronoxide-query --verify-readbacks`. The normalized verifier
semantics, selection/postings fingerprints, 38 executed readbacks, zero skips,
zero mismatches, 14 canonical PromQL rows, and row fingerprint must match the
screen-frozen 4M authority and each other. Footer/readback work is outside
measured replay timing.

## Decision thresholds

For each stage, the arithmetic midpoint of the two candidate observations is
compared with the midpoint of the two system observations. Both roles must
have no more than 5% relative pair spread for each gated metric.

A stage passes only when all of these hold:

- workload CPU improves by at least 3%;
- workload-phase peak RSS regresses by no more than 5%;
- workload-boundary VmHWM regresses by no more than 5%;
- end-of-post-drop-hold RSS regresses by no more than 5%;
- all candidate and system pair-dispersion checks pass.

Both stages must pass for `eligible_for_manual_promotion_review: true`.
Diagnostic wall, full-process time/perf, faults, lifecycle peak RSS, raw RSS
samples, and allocator stats remain retained even when they are not thresholds.

## Raw evidence and completion

Every measured run and canonical validation is sealed immediately after its
last derived document. A raw authority records the exact nested file set,
mode, size, and SHA-256 digest, including all segment payload files. Files and
directories become non-writable. Adding a nested file, changing bytes or mode,
removing an entry, or introducing a symlink/special file invalidates admission.

Separate raw authorities cover the frozen full-gate harness, input controls,
preflights, no-stats build evidence, preserved binaries, rendered configs,
eight runs, two canonical validations, and final capture reinventory.

Final admission reconstructs every preflight, build document, observation,
stage decision, and canonical validation from raw files. Saved summaries must
be byte-equivalent JSON to those reconstructions. This includes reparsing GNU
time and perf raw streams, quiescence samples, capture `fincore` output, raw RSS
samples, replay output, every segment payload, verifier/readback output, and the
screen-frozen comparison helper's input. The screen receives one full artifact
revalidation again at final admission; lightweight pinned-source and binary
checks surround measured work, and the post-build screen validation is itself
bound into no-stats build provenance.

Focused tests mutate raw RSS timestamp order, middle and terminal cadence
edges, root starttime, marker mode, and control roles; mutate guardian causal
and cadence evidence; and source-audit held-launch ordering, first-sample
readiness arguments, lowercase dead-state refusal, mode-0444 shell admission,
identity-safe cleanup, and the reconstructed final artifact matrix.

The complete result artifact manifest has exact evidence-tree coverage. The
fresh Cargo target is explicitly named as non-evidence `build-target/**`; no
other dynamic exception exists. Before completion, exact NUL-delimited file and
directory inventories, a mode/size/SHA-256 manifest, and
`metadata/FINAL_SEAL_VALIDATED.json` bind the admitted decision and complete
tree. The final `COMPLETE` JSON then binds that certificate, decision, both
inventories, and the artifact manifest. Admission and exact-tree validation run
again after `COMPLETE` exists; failure removes the marker. Thus a changed byte,
mode, missing entry, added file, added empty directory, symlink, or special file
cannot remain admitted evidence. A partial directory, saved stage summary, or
final decision without the validated completion certificate is not evidence.

## Invocation

The runner must be executed from the completed screen's read-only source
snapshot:

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
```

`--validate-only` validates the completed screen, capture/config, plan, and
capacity without creating the requested result. `--dry-run` additionally
builds and proves the no-stats binary, freezes preflights/configs/authorities,
and exits before replay, perf, footer validation, or readbacks.

The screen and full result should be fresh sibling directories on the large
result filesystem (for example under `/var/tmp/chronoxide-results`); the
capture may stay on its data filesystem. Formal execution requires explicit
`CAPTURE`, `CONFIG_TEMPLATE`, `QUIET_HOST_CONFIRMED=1`, and a one-line
`RUN_NOTE`. Neither formal mode nor either preflight mode silently chooses
developer-local input paths.

## Verification

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -B \
  docs/experiments/storage_vnext/test_phase5_allocator_full_gate.py
python3 -m py_compile \
  docs/experiments/storage_vnext/phase5_allocator_full_gate.py \
  docs/experiments/storage_vnext/test_phase5_allocator_full_gate.py
bash -n docs/experiments/storage_vnext/phase5_allocator_full_run.sh
shellcheck docs/experiments/storage_vnext/phase5_allocator_full_run.sh
```
