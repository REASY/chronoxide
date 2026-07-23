# Bounded Allocator-Policy Screen Design

**Date:** 2026-07-21

**Updated:** 2026-07-22

**Status:** Implemented diagnostic machinery; measurement not started

**Scope:** Phase 5 of `improve_new.md`; Linux GNU replay ingestion only

## Decision

Chronoxide will keep the Rust system allocator as the default. This phase adds
an opt-in, bounded comparison of that default against four linked-jemalloc
startup policies. It does not change storage bytes, query semantics, the
default allocator, or production configuration.

The first experiment is deliberately a 250,000-message screen. A policy that
passes this screen is only a candidate for a later full gate. Neither a passing
screen nor a completed full gate authorizes production promotion by itself.

No formal measurement may run concurrently with a build, container build,
profiler/tracer, replay, footer scan, database, or other material host workload.
The runner checks the hardened Phase 4 process vocabulary immediately before
launch and runs a separate 100 ms `/proc` guardian continuously for the entire
measured process lifetime. QEMU system emulators, `qemu-kvm`, Android emulator
processes, `adb`, and Gradle daemons/workers are explicitly included. A
transient observed conflict fails the run. The finalized Phase 4 classifier is
shared by the static snapshots and continuous guardian, including
`cargo-nextest`, `.real`/versioned Ninja and compiler names, `ld.bfd`,
`ld.gold`, Soong, kati, Android build-tool variants, and the interactive
process monitors `btop`, `htop`, and `top`. The same guardian continuously
enforces a per-run free-space reserve and terminates the owned process tree if
either authority fails. The current harness is code and test infrastructure
until a quiet-host run is explicitly started.

## Motivation and evidence

The current Phase 1 replay baseline is the authority for the next experiment:

- `docs/experiments/storage_vnext/2026-07-21-phase1-replay-baseline.md` reports
  allocator entry/free symbols at well over 30% of sampled self CPU.
- The same report records a stable repeated system-allocator baseline and
  explicitly identifies the bounded Phase 5 matrix as the next allocator test.
- `docs/experiments/storage_vnext/2026-07-16-jemalloc-ingester-results.md` is
  historical evidence. Linked jemalloc reduced ingest CPU in that experiment,
  but its default arena/decay policy increased RSS enough that it remained
  opt-in.

This makes allocator policy a measured hypothesis, not a general claim that
jemalloc is better. The experiment asks whether a linked jemalloc policy can
retain the CPU benefit while bounding peak and released-state RSS.

## Non-goals

This phase does not:

- alter any on-disk format or storage semantics;
- change the package's default allocator;
- add per-allocation, per-message, or periodic allocator instrumentation;
- compare `LD_PRELOAD` interposition with Rust's linked global allocator;
- tune arbitrary jemalloc controls;
- use allocator counters from the system allocator, which has no equivalent
  supported interface;
- claim OS-cold storage, device-cache control, or production representativeness
  from a 250,000-message replay;
- promote a policy from two screen observations.

## Runtime diagnostic surface

`chronoxide-ingester` gains two additive diagnostic interfaces:

1. `--allocator-preflight` prints one JSON record and exits before loading the
   application configuration or initializing ingestion.
2. A bounded post-`Ingester`-drop hold is enabled only when both
   `CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_HOLD_SECS` and
   `CHRONOXIDE_DIAGNOSTIC_POST_INGESTER_DROP_CHECKPOINT` are set, together with
   `CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_TELEMETRY` for the two release snapshots.

The hold defaults to zero. The zero path performs no checkpoint file operation,
clock read, or sleep. Nonzero holds require a fresh absolute checkpoint path
and are capped at 30 seconds. The checkpoint is created with `create_new` and
contains exactly two flushed rows:

```text
schema  phase             main_elapsed_ns  unix_time_ns  hold_secs
...     ingester_dropped  ...              ...           30
...     hold_complete     ...              ...           30
```

The first row is emitted only after the branch-local `Ingester`, source, and
processor have left scope. Tokio's signal task and telemetry providers are
still alive, so this is specifically an after-`Ingester` release observation,
not a claim about an otherwise empty process.

After Tokio is constructed but before logging or workload setup, an explicit
runtime diagnostic emits one structured JSON policy record. A nonzero
post-drop hold enables that diagnostic for measured screen replays. The
selected-policy `perf record` replay, which intentionally has no release hold,
enables it with the strict
`CHRONOXIDE_DIAGNOSTIC_ALLOCATOR_RUNTIME_POLICY=1` trigger. Any other value is
invalid. During the stats-enabled Phase 5 diagnostic the record contains the
complete eight-field effective policy, not only fields explicitly requested by
the comparator. The gate requires that object to equal the preflight snapshot
exactly. A system or plain-`jemalloc` binary may expose the same explicit
diagnostic record, but reports requested/effective policy as unavailable.

Outside an explicit preflight, runtime-policy diagnostic, or post-drop
diagnostic, the application neither parses nor rejects `_RJEM_MALLOC_CONF`,
performs no effective-policy `mallctl` read, emits no structured runtime-policy
JSON, and retains its prior minimal allocator-identity startup log. Ordinary
system and plain-`jemalloc` startup therefore retain the production
configuration and output surfaces. Diagnostic human-readable startup logging
also names:

- the Rust global allocator (`system` or `jemalloc`);
- the raw and canonical `_RJEM_MALLOC_CONF` policy;
- the effective fixed jemalloc options read with `mallctl`;
- whether allocator-internal telemetry is available;
- the diagnostic hold duration.

The separate preflight record states whether `LD_PRELOAD` or unprefixed
`MALLOC_CONF` was present. Formal commands run from an `env -i` allowlist and
require both to be absent.

The release telemetry file contains exactly two JSON records labeled
`post_ingester_drop` and `hold_complete`. A jemalloc record refreshes `epoch`
and captures `stats.allocated`, `stats.active`, `stats.resident`,
`stats.mapped`, and `stats.retained`. A system-allocator record reports every
one of those fields as explicit `null` with availability `unavailable`, not as
invented equivalents.

Both snapshots are captured before the telemetry file or JSON `BufWriter` is
opened. The first follows the flushed `ingester_dropped` boundary; the second
follows the sleep. Telemetry serialization therefore contaminates neither
endpoint. The checkpoint writer already exists, and refreshing jemalloc's epoch
may itself allocate while refreshing cached statistics; this unavoidable
self-observation must be disclosed with the results.

Diagnostic preflight also performs a binary-level global-allocator probe. A
64 MiB Rust allocation is page-touched and kept live across a refreshed
`stats.allocated` snapshot. A `jemalloc-stats` binary must show at least 48 MiB
of growth. This proves that the executable's Rust global allocator, rather
than merely a linked library or cfg label, routes live allocations through the
mallctl instance. The system comparator and the production-facing plain
`jemalloc` build report the probe as explicitly unavailable.

The production-facing `jemalloc` feature remains allocator selection only: it
does not enable jemalloc statistics or compile the mallctl diagnostic module.
Phase 5 uses the separate `jemalloc-stats` feature, which includes `jemalloc`
and adds the stats-enabled live probe, effective-policy reads, and release
telemetry. The later production revalidation command uses plain `jemalloc`, so
the screen cannot silently add statistics overhead to the existing feature.

## Linked jemalloc policy verification

`tikv-jemallocator` builds jemalloc with the `_rjem_` symbol prefix. Its linked
runtime configuration input is therefore `_RJEM_MALLOC_CONF`; unprefixed
`MALLOC_CONF` is rejected by the harness as provenance ambiguity. The runner
refuses allocator, glibc malloc, dynamic-loader, Rust flag/wrapper, compiler,
and linker overrides in its ambient environment. Formal preflight, runtime,
and validation commands use `env -i` allowlists with frozen locale, timezone,
and logging. A diagnostic ingester run
fails startup if `LD_PRELOAD` or `MALLOC_CONF` is present, so a formal replay
cannot merely log and continue with an allocator confounder.

For an explicit stats-enabled allocator diagnostic, the parser accepts only
this bounded option set:

- `abort_conf` and `confirm_conf`;
- `narenas` in `1..=64`;
- `dirty_decay_ms` and `muzzy_decay_ms` in `-1..=60000`;
- `background_thread`;
- `max_background_threads` in `1..=16`, only with
  `background_thread:true` in the same policy;
- `retain`.

Unknown keys, duplicates, whitespace, empty input, invalid types, and values
outside the bounds fail that diagnostic startup. They are not application-level
restrictions on ordinary production startup. J0 leaves `_RJEM_MALLOC_CONF`
truly unset.
J1-J3 require `abort_conf:true` and `confirm_conf:true`; those diagnostic
controls are not injected into the default comparator.

Requested text alone is not accepted as proof. On Linux GNU builds with the
`jemalloc-stats` feature, one cfg-gated module performs eight startup `mallctl` reads
of `opt.*`. Every explicitly requested option must equal its effective value.
The same audited module refreshes `epoch` and reads the five release statistics
at the two diagnostic boundaries. It contains all `tikv_jemalloc_sys` and
unsafe use; there is no mallctl call in either a system-allocator build or a
plain `jemalloc` build, and no allocator-control call in the ingestion hot
path.

For configured J1-J3 policies, the runner requires four pieces of
policy evidence:

1. the application preflight JSON contains matching effective `mallctl` values;
2. jemalloc's `confirm_conf:true` stderr echoes the exact environment source and
   every configured entry;
3. confirmation sources #1 (`--with-malloc-conf`), #2 (global `malloc_conf`),
   #3 (`/etc/malloc.conf`), and #5 (`malloc_conf_2_conf_harder`) are exactly
   empty while source #4 contains exactly the comparator policy;
4. the measured process's structured startup record contains all eight
   effective values and exact requested policy again.

Any missing, malformed, ignored, or changed option fails the run. For J0, the
runner instead requires absent requested-policy bytes and no confirmation
output in the actual comparator process. A separate fail-closed J0 source audit
uses only `abort_conf:true,confirm_conf:true` to prove that sources #1, #2, #3,
and #5 are empty; the real J0 preflight and replay remain truly unset. The
complete effective `opt.*` snapshot must match between preflight and runtime.
The selected-policy CPU profile sets the explicit runtime-policy trigger so it
gets this proof without enabling a release hold; requested environment text
alone is not evidence.

## Frozen screen matrix

The machine-readable authority is
`docs/experiments/storage_vnext/phase5_allocator_screen_plan.json`.

| Policy | Linked allocator | `_RJEM_MALLOC_CONF` |
|---|---|---|
| S | system | unset |
| J0 | jemalloc | unset |
| J1 | jemalloc | `abort_conf:true,confirm_conf:true,narenas:4` |
| J2 | jemalloc | J1 plus `dirty_decay_ms:1000`, `muzzy_decay_ms:0`, background thread, one background thread |
| J3 | jemalloc | J2 with only `narenas:4` changed to `narenas:2` |

J0 is the linked allocator's true default policy. Its complete fixed option
snapshot is retained even though no configuration string is supplied. Because
its host-scaled defaults include an automatic arena count, J0 is comparator
evidence only and can never advance to the full gate.

The matrix is shaped to isolate decisions rather than combine them silently.
J1 adds the fail-closed configuration diagnostics and bounds arenas at four;
the diagnostic controls do not change normal allocation policy, although their
small startup-output cost remains inside the measured process. J2 holds four
arenas constant while adding a one-second dirty-page decay, immediate
muzzy-page decay, and one background purger; `muzzy_decay_ms:0` is intentionally
more aggressive than repeating the dirty decay and tests whether retained RSS
is actually returned. J3 holds that decay/background policy constant and
changes only the arena bound from four to two. This permits J0→J1, J1→J2, and
J2→J3 to be interpreted separately.

The exact mirrored order is:

```text
S J0 J1 J2 J3 | J3 J2 J1 J0 S
```

There are exactly two observations per policy. With two values, the reported
pair median is their arithmetic midpoint. Every policy also reports the
relative spread of workload CPU, workload RSS, boundary VmHWM, and released
RSS. A spread above 5% makes that policy ineligible; unstable S also prevents
advancement. The screen exposes drift and removes a simple one-direction order
bias, but it does not estimate a robust population distribution.

## Binary identity and provenance

The runner accepts no external comparator binaries and never invokes Cargo from
the live worktree. After proving the task changes are committed and the live
HEAD/index/worktree are clean, it creates a `git archive HEAD` tarball outside
that worktree. The frozen gate safely decodes only directory and regular-file
members, rejects links, special entries, duplicate or escaping paths, and
requires every archived path, executable bit, and blob digest to equal the
sealed Git tree. It then writes a fresh extracted tree outside the worktree,
makes every file and directory non-writable, and seals the complete inventory.

Cargo runs with that extracted tree as both its recorded CWD and explicit
`--manifest-path Cargo.toml` base. Its target directory and Cargo/Rustup homes
are outside the source tree, and Cargo configuration in the extracted tree's
ancestors or Cargo home is forbidden. The only accepted project configuration
is the exact tracked root `.cargo/config.toml` in the archived tree. Every
parsed Cargo manifest `path` reference must resolve inside the extracted tree.
The runner builds the system ingester, query, and verifier together,
immediately preserves them, then builds and preserves the diagnostic
`jemalloc-stats` ingester from
the same read-only snapshot, Cargo.lock, release mode, target directory, and
sanitized environment. The exact `--locked` commands, CWD, environment, build
logs, Git HEAD/tree, archive hash and embedded commit, extracted-tree manifest,
Cargo.lock hash, and four binary hashes are sealed in one build-provenance
object. The system and jemalloc ingesters must have different SHA-256 hashes.

All four preserved executables are made non-writable. All S observations must
use the one preserved system hash. All J0-J3
observations must use the one preserved jemalloc hash. The runner checks the
complete four-executable seal and both source seals immediately before and
after every ingester, query, and verifier invocation. Preflight records and
observation records carry the ingester hash, and the aggregate gate rejects a
role whose hash changes between observations.

The executing screen runner must resolve inside the selected repository, and
its copied harness must byte-match the read-only archived HEAD tree. A pinned
read-only core-control seal covers the gate, both runners, plan, helpers, source
and extracted-source authorities, build provenance, binary inventory/seal, and
all four binaries. After config rendering and cache-helper compilation, a
second pinned read-only measurement-control seal covers the first authority,
capture/template records, run plan, every rendered config and render record,
and the compiled `fadvise` helper. Fixed files have exact mode 0444 or 0555;
both control authorities are hash-pinned in the running shell and recomputed
around every consumer. Render records are additionally checked against the
config hash and exact capture, fresh segments path, and message limit before
they enter the measurement seal.

Before archiving, the runner writes a formal live-source seal. It rejects
assume-unchanged or skip-worktree entries, symlinks, gitlinks, nonzero index
stages, untracked build files, known ignored source/build candidates, and
ambient Git overrides. In particular, an ignored worktree `.cargo/config` is
forbidden because Cargo gives it precedence over `.cargo/config.toml`. The
archive's exact Git-tree equivalence is the exhaustive boundary: arbitrary
ignored extensions cannot enter the extracted build source, even if tracked
Rust uses `include_bytes!` to name them. The live HEAD/source seal is stable
before and after archive creation, and both the live and extracted-source seals
are required before and after each build, around every executable invocation,
and at finalization. The completed artifact manifest includes the read-only
archive, extracted-source seal, and every extracted source file. The external
Cargo `build-target/**` subtree is disposable non-evidence and is pruned without
traversal; only its exact top-level, non-symlink directory is admitted. Native
build-system links inside that subtree therefore cannot enter or invalidate the
evidence inventory. Build authority instead comes from the archived source,
recorded commands and logs, build provenance, and four preserved executable
hashes.

The runner also records the Git commit, status, index, patches, tracked and
untracked file inventories, frozen harness files, rendered configurations,
binary notes, tool versions, capture manifest, host state, and artifact hashes.
Cargo incremental compilation is disabled; ambient Rust flags, wrappers,
compiler/linker flags, and build-time jemalloc configuration are absent. Every
Python helper, inline parser, and background monitor runs through one resolved,
hash-pinned interpreter with `-I -S -B`. The required isolation flags are
fail-fast probed and recorded, the interpreter identity is checked before and
after each invocation, and its read-only evidence record is part of the core
control seal. Helper entrypoints and the Phase 1 sibling are compiled from
exact `.py` source bytes; neither site customization nor a preexisting
`__pycache__`/`.pyc` can become executable authority. Dry-run output is marked
separately and is never measurement evidence.

## Workload and timing contract

Each observation replays exactly 250,000 messages from the Phase 1 pinned
capture with Schema 8 and the same deterministic writer configuration. The
Phase 1 expectation file and helper hashes are frozen in the plan.

Timing has intentionally different scopes:

| Metric | Start | End | Includes 30 s hold? |
|---|---|---|---:|
| Workload wall | synchronous `main` entry, before policy parsing and Tokio construction | flushed `ingester_dropped` checkpoint | no |
| Workload CPU | process-tree cumulative `utime+stime` | first externally sampled post-drop state | at most one sampling interval |
| Workload peak RSS | first external workload sample | last sample labeled `workload` | no |
| `perf stat` | complete launched process | process exit | yes |
| GNU time | complete launched process | process exit | yes |
| Total-lifecycle external RSS | launcher process tree first seen | process tree exit | yes |
| Allocator release stats | just after drop boundary | after 30 s sleep, before complete boundary | release phase only |

The workload wall value is the first checkpoint's `main_elapsed_ns`; no
subtraction of noisy shell or log timestamps is used. The hold's monotonic and
wall-clock deltas must both be between 30 and 60 seconds.

RSS and CPU are sampled every 100 ms from `/proc` for the complete launcher
process tree, including the `perf`/GNU-time wrappers and ingester. CPU is the
sum of each live process's fields 14 and 15 (`utime+stime`) from `/proc/PID/stat`;
child-time fields are deliberately excluded, so simultaneously live parent and
child processes are not double-counted. `SC_CLK_TCK`, raw ticks, derived
seconds, the sample-window bounds, and the PID set are recorded. The first
sample labeled `post_drop_hold` is the workload CPU boundary; its worst-case
distance from the checkpoint must be no greater than one 100 ms interval.
The first post-drop sample also retains the kernel's process VmHWM as a
workload-boundary high-water mark. VmHWM is not substituted for sampled RSS;
both are separate gates because VmHWM retains short peaks a 100 ms sampler can
miss.

Every measured screen launch is held before GNU time, `perf`, or the ingester
can execute. The runner starts the RSS monitor and conflict/capacity guardian,
captures all three `/proc/PID/stat` starttimes, and atomically publishes one
exact mode-0444 control binding those identities plus canonical RSS-ready,
guardian-ready, and launch-marker paths. The RSS monitor validates that
control, samples the still-held root tree, and exclusively creates an empty
mode-0444 RSS-ready marker after its first valid sample. The guardian waits for
that marker before starting its cadence, so its own mode-0444 ready marker
still corresponds to guardian poll one. Only after both monitors are ready may
the runner create the launch marker. The held shell independently requires
that marker to be a regular, non-symlink, empty, exact mode-0444 file before
`exec`.

Successful RSS evidence contains at least two samples and observes launch on a
sample after RSS readiness. Raw sample-start monotonic timestamps must be
strictly increasing at the exact 100 ms requested interval. Final admission
reconstructs the maximum gap over `[0, *sample_starts, monitor_elapsed_ns]`,
including the terminal edge, and rejects any gap above 200 ms. It also
revalidates the control role, root starttime, marker paths/modes/digests, causal
sample numbers, exact zero exit statuses, and absence of handshake violations.
Calibration is not RSS evidence and therefore uses the same held root plus
guardian lifecycle without fabricating a dummy RSS-monitor role or RSS-ready
marker.

Liveness and cleanup are PID-reuse safe: root traversal and every signal bind
the captured starttime, discovered children must retain the observed PPID, and
Linux zombie/dead states `Z`, `X`, and lowercase `x` are never treated as live
or signaled. Spawn/bind signal deferral prevents an unbound job from escaping;
cleanup terminates the measured tree depth-first before the bound RSS monitor
and guardian and reaps each identity with a bounded wait. Guardian conflict
classification uses a separate identity-bound tree: exiting zombie descendants
remain attributable to the measured wrappers but do not contribute RSS or keep
the lifecycle live. Before excluding any such PID from the global scan, the
guardian rereads its starttime and PPID; reuse or reparenting fails closed.
Rejected conflicts record PID, PPID, state, starttime, command name, and command
line for direct diagnosis.

Before every measured launch, the harness performs a global `sync`, then
requires `Dirty + Writeback <= 65536 KiB` for three consecutive 250 ms samples.
After every replay and artifact parse, it fsyncs every corpus file and
directory, performs the same global sync, and repeats that quiescence gate
before advancing. All samples are preserved. Failure to quiesce within 120
seconds fails closed.

Capacity admission is two-stage. Before calibration, the conservative Phase 1
four-million-byte authority reserves `11/4` of that corpus plus 8 GiB. Once the
fresh 250k calibration corpus exists, each remaining launch uses its exact
payload size: free space must cover every not-yet-created measured corpus plus
8 GiB. During a launch, the 100 ms guardian preserves the reserve needed for
the remaining future corpora plus 8 GiB. During calibration that floor is
`10/4` of the frozen Phase 1 corpus plus 8 GiB; afterward it is the exact
calibration size times the unstarted measured-run count plus 8 GiB. A
capacity breach is preserved in raw guardian evidence and kills the owned
launcher tree; it cannot degrade into a partial successful observation.

Each external sample is labeled
`workload`, `post_drop_hold`, `hold_complete`, or transient
`checkpoint_incomplete` from checkpoint state. The gate requires at least one
workload sample, at least 20 post-drop samples, exact phase accounting, and
post-drop timestamps bounded by the two checkpoint rows.

Each internal allocator snapshot is aligned to the nearest post-drop external
RSS sample within one interval. The report records both values and their signed
difference, but labels them non-equivalent: jemalloc `stats.resident` covers
allocator-resident pages in the ingester, whereas external RSS covers the
sampled launcher process tree, including non-allocator mappings and wrappers.
No equality assumption is used as a gate.

GNU time and `perf stat` include the hold by predeclared design. Consequently,
their total-lifecycle task-clock includes allocator background-thread activity
during release and is not the CPU-benefit gate. The external boundary CPU is
the workload CPU comparator. The checkpoint wall value remains the latency
comparison that excludes the artificial wait. Reports must preserve all scopes
and must not relabel full-process elapsed or perf task-clock as ingest latency.

The required perf event set is task-clock, cycles, instructions, branches,
branch misses, cache references/misses, page/minor/major faults, context
switches, and CPU migrations. An unavailable counter fails the screen.

Before every replay, the runner applies `POSIX_FADV_DONTNEED` to each capture
file and requires `fincore` to report zero resident capture bytes. This controls
capture page-cache residency only; it does not flush device/controller caches.

## Pre-run 250k semantic calibration

The row fingerprint is not copied from the Phase 1 four-million-message
expectations. After the controlled binaries are preserved and before the first
measured observation, the runner performs one fresh, untimed 250,000-message
replay with the preserved system-allocator binary and the same source/config
contract. It then runs exhaustive unsampled Schema 8 footer/postings
verification and independent readbacks. The calibration record is created only
from those raw files and binds:

- the raw storage verifier, readback, replay-correctness, and corpus-summary
  SHA-256 values;
- the controlled system, query, and storage-verifier binary hashes;
- the exhaustive selection and exact-postings fingerprints;
- exact corpus identity and full storage counts/evidence;
- exact readback query coverage and the canonical PromQL-row fingerprint.

The calibration is explicitly ineligible as A/B timing or RSS evidence. It is
completed before `observation_args` and the measured loop are entered. Capture
eviction and the normal per-run quiescence gate prevent this semantic
calibration from silently changing the measured cache contract.

Final canonical validation must match the calibration's corpus, selection,
postings, exact storage structure, readback coverage, and
PromQL rows. Its summary contains the raw storage and readback hashes. The
final seal does not trust that summary: it rereads the final raw reports and
the raw calibration inputs, recomputes both documents, and requires byte-equal
JSON. Mutation or fabrication of either a raw input or reduced summary fails
completion.

## Correctness and equivalence gate

Performance is discarded unless all ten observations satisfy:

- successful process, RSS monitor, GNU-time parse, and every required perf
  counter;
- exact replay report correctness;
- identical deterministic corpus manifest and byte size;
- byte-identical corpus manifests and replay-correctness documents across all
  ten observations;
- zero swap;
- valid two-row release checkpoint and sufficient externally labeled RSS
  samples;
- exact workload CPU ticks/`CLK_TCK`, with boundary uncertainty no greater than
  one sample interval;
- exact two-phase allocator telemetry, advancing jemalloc epochs, valid
  allocated/active invariants, explicit unavailable system fields, and bounded
  external-RSS alignment;
- unchanged comparator hash and exact allocator evidence.
- a continuously conflict-free guardian record and passing pre/post writeback
  quiescence records.

"Exact replay correctness" means exactly 250,000 messages plus full counter
algebra: observed equals accepted plus the three rejection classes; rejection,
storage, type, event-skew, and watermark totals cross-check;
accepted-not-recorded equals accepted minus recorded and equals the
missing-number count. A merely positive or self-consistent one-message result
cannot pass.

After the timed screen, one canonical byte-identical corpus receives exhaustive
Schema 8 footer/postings verification and independent query readbacks. Storage
samples must equal measured `Recorded Samples`. Expected, executed, and checked
query counts must be exactly 40, with exactly 14 PromQL result rows. Skips,
isolation skips, and mismatches must all be zero. Selection, postings, and
canonical PromQL-row SHA-256 fingerprints are sealed. These validations are
outside measured replay timing. The 40-query count is specific to the frozen
250k calibration corpus; the historical four-million-message count of 38 is
not reused.

## Screen candidate gate

Each bounded policy J1-J3 is compared with S using the midpoint of its mirrored
pair. J0 is never eligible. A policy is a candidate for a later full gate only
when all are true:

- boundary workload CPU improves by at least 3%;
- workload-phase peak process-tree RSS regresses by no more than 5%;
- workload-boundary VmHWM regresses by no more than 5%;
- end-of-post-drop-hold RSS regresses by no more than 5%;
- both its and S's mirrored-pair dispersion gates pass.

At most one policy advances. Selection is deterministic: greatest CPU
improvement, then lowest HWM regression, then lowest sampled-RSS regression,
then frozen policy order.

Workload wall, total-lifecycle perf task-clock/cycles/instructions/faults,
total-lifecycle peak RSS, allocator allocated/active/resident/mapped/retained
levels and deltas, full-process time, first/minimum post-drop RSS, and raw
external samples remain required diagnostic evidence even where they are not
candidate thresholds.

## Separate untimed allocation/CPU profiles

Profiles are not launched by, nested inside, or charged to any A/B
observation. `phase5_allocator_profile_run.sh` consumes a separately completed
screen and creates a fresh external result directory. It never records GNU
time, sampled RSS, `perf stat`, release-hold telemetry, or any value eligible
for the allocator candidate thresholds.

Heaptrack always receives the preserved system-allocator binary from the
screen's controlled build. Its preload remains present when the binary is
launched; an inner `env -i` is not allowed to erase the profiler interposition.
The headless invocation uses `--record-only`, so it cannot auto-launch
`heaptrack_gui`; `heaptrack_print` remains the explicit analysis step.
Before the profile consumes a frozen helper or binary, it validates the
completed screen's exact file/directory inventory, versioned read-only
`COMPLETE` marker, formal
live plus archived/extracted source bindings, and all four non-writable
executable hashes. The same screen seal is checked around every profiled
ingester, query, and verifier
invocation. The profile runner itself must be the exact read-only copy inside
the completed screen, and each profile config plus render record is covered by
a separately pinned read-only control seal. The resulting allocation trace must
be nonempty, analyzable by `heaptrack_print`, contain a positive allocation
count and at least one usable multi-frame collapsed stack with a Chronoxide
frame, and
have no error, failed-initialization, segmentation-fault, lost-event, or
dropped-sample diagnostic. The binary hash is checked against controlled build
provenance. Its replay must reproduce the exact measured correctness document
and corpus-manifest bytes, and its unprofiled post-run footer/postings and
readback validation must match the pre-run calibration.

Heaptrack over the system allocator is the only allocation-stack authority in
this phase. Candidate-specific linked-jemalloc heap profiling is deferred: the
screen does not treat general `malloc` preload interposition as proof that the
prefixed Rust `_rjem_` global allocator was traced completely.

An optional second fresh replay may run `perf record` call-graph sampling over
the system binary or the single selected bounded policy. It requires a
nonempty `perf script` stream, at least one usable multi-frame callchain with a
Chronoxide frame, and zero recorded lost events and is subject to the same
binary, correctness, corpus, and semantic gates. Its stacks are diagnostic CPU
attribution only and never retroactively enter screen timing or RSS.
For a selected jemalloc policy, the profile additionally rejects host jemalloc
configuration sources, reruns the frozen application preflight, audits all five
configuration sources, enables the strict runtime-policy diagnostic trigger,
and gates the profiled process's structured effective policy and `confirm_conf`
output. Requested environment text alone is not accepted as proof of the
selected CPU-profile policy.

Formal profiles require a one-line `RUN_NOTE`, an immediate
`QUIET_HOST_CONFIRMED=1` acknowledgement, and at least the configured capacity
reserve (16 GiB by default, never below 8 GiB) in addition to space for the
reference-sized corpus. The same static classifier and continuous 100 ms
conflict/capacity guardian surround each profiled replay. Each completed
Heaptrack or perf subtree is immediately made read-only and sealed with an
exact directory inventory plus the SHA-256, size, and mode of every raw file
and every segment payload. Final profile admission independently rebuilds
`profile-evidence.json` from those raw leaves, requires an exact result-tree
matrix, writes NUL-delimited file and directory authorities, validates them,
then writes and revalidates the versioned read-only profile `COMPLETE` marker.
The exact configured reserve is published before profiling as the exclusive,
mode-0444 `metadata/profile-capacity-control.json` authority. Every profile
control seal includes that file. Final raw reconstruction validates its exact
schema, mode, and digest, derives each guardian floor as reference-corpus bytes
plus that recorded reserve, and records the reserve and authority digest in the
final reconstruction. It never substitutes the screen's 8 GiB minimum for a
larger configured profile reserve.

Heaptrack and optional `perf record` both enter one root-plus-guardian
held-launch function. Because profile RSS is deliberately not a measurement,
that control has exactly the root and guardian roles and no RSS PID or
RSS-ready marker. The guardian creates its ready marker on poll one, the runner
releases the exact mode-0444 launch marker, and final profile reconstruction
requires causal guardian evidence plus exact zero replay/guardian statuses for
each enabled profile.
The status-preserving nonrecursive `EXIT` handler cleans an active lifecycle
for ordinary failures and signals, including failure of the fail-fast Python
integrity wrapper itself. Emergency cleanup invokes the captured interpreter
path directly, first attempts sealed-control cleanup, then falls back to the
already captured root and guardian starttimes, and always performs bounded
reaping. A cleared successful lifecycle performs no cleanup.

## Completion and promotion safety

A partial result is never promotable. At result creation the runner writes
`PARTIAL_UNLESS_COMPLETE.txt`. A screen summary can be written only after ten
valid observations, but that summary is still not a completed result.

Immediately after calibration, every measured run, and canonical validation,
the corresponding evidence tree is checked against its exact allowed path
matrix, made read-only, and sealed outside that tree. These seals record every
directory plus the SHA-256, size, and mode of every file, including every
segment payload. They also independently reconstruct corpus inventories and
replay counters from payloads and the raw ingestion report.

The final gate independently reconstructs RSS summaries from the raw sample
TSV, quiescence decisions from raw samples, GNU-time/perf summaries, allocator
checkpoints and telemetry, runtime-policy records, all ten observations, the
screen comparison, calibration, exhaustive verifier/readback validation, and
the final decision. It also rechecks source, controls, executables, capture
identity, and the immutable evidence seals. Only then does it write
`comparisons/final-screen-decision.json`. The result receives exact
NUL-delimited file and directory inventories and a digest manifest that now
includes every segment payload. A read-only `FINAL_SEAL_VALIDATED.json` is
required before the runner writes
`chronoxide/allocator-screen-complete/v1` to a read-only `COMPLETE` file. Both
the exact inventory and raw reconstruction run again after that marker. Outside
the explicitly excluded `build-target/**` non-evidence subtree, a crash,
unexpected nested entry, mutation, special file, unreadable directory, or
failure at any earlier point leaves no valid completion pair.

The screen jemalloc build intentionally enables allocator stats so the live
probe and release telemetry are available. The final decision explicitly
records:

- `production_promotion_authorized: false`;
- `partial_runs_promotable: false`;
- the single selected full-gate policy, if any;
- that a stats-enabled four-million-message gate remains required;
- that a separate no-stats build revalidation remains required before any
  no-stats production deployment;
- that neither later gate has been satisfied by this screen.

The screen itself always records `production_promotion_authorized: false`.
A default allocator change requires both later gates, review of their evidence,
and an explicit code and documentation decision.

## Verification

The diagnostic implementation and harness require all three allocator build
shapes. The plain `jemalloc` checks prove allocator selection still compiles
and explicitly reports unavailable mallctl diagnostics; only
`jemalloc-stats` is expected to exercise the live probe and effective-policy
telemetry:

```sh
cargo test -p chronoxide-ingester allocator_policy --no-default-features
cargo test -p chronoxide-ingester allocator_policy \
  --no-default-features --features jemalloc
cargo test -p chronoxide-ingester allocator_policy \
  --no-default-features --features jemalloc-stats
cargo test -p chronoxide-ingester --no-default-features \
  --test allocator_preflight
cargo test -p chronoxide-ingester --no-default-features --features jemalloc \
  --test allocator_preflight
cargo test -p chronoxide-ingester --no-default-features \
  --features jemalloc-stats --test allocator_preflight
python3 docs/experiments/storage_vnext/test_phase5_allocator_screen_gate.py
python3 -m py_compile \
  docs/experiments/storage_vnext/phase5_allocator_screen_gate.py
bash -n docs/experiments/storage_vnext/phase5_allocator_screen_run.sh
shellcheck docs/experiments/storage_vnext/phase5_allocator_screen_run.sh
bash -n docs/experiments/storage_vnext/phase5_allocator_profile_run.sh
shellcheck docs/experiments/storage_vnext/phase5_allocator_profile_run.sh
```

The Python tests mutate the frozen plan, helper hashes, effective allocator
values, all five jemalloc configuration sources, J0's unset/comparator-only
proof, J1-J3 confirmation output, build/source/archive/binary hashes, ignored
and untracked build inputs, arbitrary-extension ignored-file exclusion from the
exact read-only Git-tree extraction, hidden Git index flags, exact 250k counter
algebra,
checkpoint timing, CPU boundary uncertainty, RSS/HWM phase coverage, raw RSS
first-sample readiness, causal launch, middle and terminal cadence edges,
control roles/starttimes, marker mutation, dispersion, allocator
epochs/stats/nullability/alignment, continuous conflicts,
quiescence, exact unsampled storage schemas, raw-report hashes, pre-run 250k
calibration fabrication/mutation, canonical fingerprints, completion ordering,
completed-screen artifact and executable mutations, selected-profile policy
mutations, the separate untimed profile contract, and the hardened
QEMU/Android/Gradle quiet-host vocabulary, including exact rejection of
`btop`, `htop`, and `top`, lowercase Linux dead state `x`, and exact mode-0444
held-shell launch admission. The Rust tests cover strict policy parsing, bounded
diagnostic configuration, preflight argument isolation, requested/effective
mismatch rejection, explicit diagnostic unavailability in system and no-stats
builds, and the executable-level live global-allocation probe in the
`jemalloc-stats` build.
