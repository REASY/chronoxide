# Phase 5 allocator screen

**Status:** complete 250k stats-enabled screen; J1 is nominated for the
two-stage four-million-message confirmation gate. No allocator promotion is
authorized.

## Result authority

- Completed result root:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-phase5-allocator-screen-formal-20260723T041200Z`
- Source commit: `f0f8a5c4c10d880c81aafc443cc1d5b2f8c1834f`
- Schedule: `S,J0,J1,J2,J3,J3,J2,J1,J0,S`
- Complete observations: 10 of 10
- Completion marker: read-only `COMPLETE`
- Final raw revalidation: pass
- Final artifact seal: pass, 1,419 admitted files and 174 directories
- Screen summary SHA-256:
  `8e10357bbe35a96465813be09e2cdd2c4f973c13a3b608587e2fd4a357a89e87`
- Validation SHA-256:
  `dee41d232c4af79d4cca4a631f7274adf63fa67cb83ec73c27e97b4cacd068c7`

This is the only admitted completed allocator-screen root. Earlier roots that
failed finalization remain partial diagnostic artifacts and are not formal
replications.

## Policies

| Policy | Allocator configuration | Role |
| --- | --- | --- |
| S | System allocator | Baseline comparator |
| J0 | jemalloc defaults | Comparator only |
| J1 | `narenas:4` | Bounded candidate |
| J2 | J1 plus 1s dirty decay, zero muzzy decay, one background thread | Bounded candidate |
| J3 | J2 with `narenas:2` | Bounded candidate |

All jemalloc observations use the telemetry-bearing `jemalloc-stats` build.
That build is intentionally not treated as production-equivalent.

## Controlled medians

| Policy | Workload CPU | CPU improvement vs S | Wall | Peak RSS KiB | HWM KiB | Post-drop RSS KiB | Eligible |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| S | 48.055s | baseline | 47.991s | 5,291,158 | 5,241,350 | 3,987,022 | baseline |
| J0 | 44.130s | 8.168% | 44.043s | 5,329,770 | 5,315,382 | 1,376,766 | no; comparator and dispersion failure |
| J1 | 44.315s | 7.783% | 44.118s | 5,321,678 | 5,295,284 | 1,344,746 | yes; nominated |
| J2 | 45.115s | 6.118% | 44.592s | 5,296,080 | 5,239,376 | 147,874 | yes |
| J3 | 44.600s | 7.190% | 43.999s | 5,295,852 | 5,239,026 | 148,316 | yes |

Relative to S, J1 increases workload peak RSS by 0.577% and boundary HWM by
1.029%, while reducing post-drop RSS by 66.272%. J2 and J3 keep peak/HWM
essentially flat and reduce post-drop RSS by about 96.3%.

The frozen selection rule first chooses the greatest CPU improvement among
eligible J1-J3, then lower HWM regression, lower peak-RSS regression, and
finally policy order. J1 therefore wins the screen on CPU. Its advantage over
J3 is only 0.285 seconds of median workload CPU, or 0.593 percentage point
relative to S, while J1 retains about 1.14 GiB more post-drop RSS and has about
55 MiB higher HWM. This is a CPU-rule nomination, not evidence that J1 is the
universally best allocator policy.

J0 failed the mirrored-pair dispersion gate because its post-drop RSS spread
was 6.804%. Eligible maximum pair spreads were 2.518% for J1, 3.170% for J2,
and 0.135% for J3.

## Correctness and determinism

All ten replays produced the same 972,969,365-byte corpus: 34 files, four
segments, 4,450,272 chunks, and 9,634,809 recorded samples. Canonical
validation passed:

- exhaustive decode and segment-footer validation;
- 313,963 exact-postings lists and 89,285,049 decoded references;
- 40 of 40 independent readbacks executed;
- zero readback skips, isolation skips, or mismatches; and
- 14 canonical PromQL rows with one stable fingerprint.

Replay accounting also reconciles exactly: 250,000 messages contained
9,659,074 observed datapoints; 3,709 were too far in the future; 9,655,365
passed event-time policy; 20,510 missing number values and 46 invalid typed
values were not recorded; and 9,634,809 samples reached storage.

## Cross-attempt stability warning

The two closest partial roots show that the two-observation screen is sensitive
to noise and pair dispersion:

- `T030600Z` would have selected J1; candidate CPU improvements were roughly
  7.390%, 7.161%, and 7.171% for J1-J3.
- `T034000Z` would have selected J3; J1's CPU improvement was 8.188%, but its
  12.011% post-drop spread made it ineligible.
- The admitted `T041200Z` selected J1 at 7.783%, 6.118%, and 7.190%.

These partial roots are not pooled evidence. They are a warning that candidate
rank and even eligibility moved between attempts; any narrow 4M result must be
interpreted cautiously.

## Remaining gate

J1 must pass both production-shape confirmation stages before manual promotion
review:

1. Measure the preserved `jemalloc-stats` candidate against system at four
   million messages in `S,C,C,S` order.
2. Build plain no-stats `jemalloc` from the same sealed source and independently
   measure `S,N,N,S`.

Each stage requires at least 3% CPU improvement; no more than 5% peak RSS, HWM,
or post-drop regression; no more than 5% mirrored-pair spread; deterministic
corpus equality; and canonical storage/readback validation. Passing both stages
only yields eligibility for manual promotion review. Until then, retain the
system allocator default and keep J2/J3's much stronger release behavior in
the decision record.
