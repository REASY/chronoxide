# Post-compact replay CPU profile

- **Date:** 2026-07-16
- **Status:** The fresh profile does not support the earlier hypothesis that
  event-time-skew statistics consume roughly 7% of replay CPU. The strongest
  next A/B candidates are label hashing/equality and allocation/protobuf
  ownership.
- **Raw run:**
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/post-compact-profile-20260716-105305`

## Workload and provenance

The run replayed one million messages from the real
`kafka-capture-001/partition-1.capture` prefix. It accepted 38,747,141
datapoints and reached 5,214,871 interned series. The complete process lifetime
was sampled, including final head sealing and segment writes.

The release binary was built with optimization and debug information from Git
head `e5d642d1b282d88db29b661d27c4d1b0166cd5e8` plus working-tree diff SHA-256
`17428d9c6b6c814f66724bf70dc34e95467c667149ee0ba624f35d3ddbb468`.
The copied binary's SHA-256 is
`489ce5a059a9d4d6834b3409b4e2fdb766069ab8894a5c2f47ba28bfc1b9da54`;
its recorded and independently inspected ELF build ID is
`fdaab24920ae924757891100b2b340b7d11bc364`.

The relevant configuration was:

- Schema 8, 900-second segments, deterministic segment-ID seed 42;
- `flat_interned_contiguous` label-set storage;
- compact numeric head series enabled;
- one-hour head and out-of-order windows;
- `stop_after_messages = 1000000`.

Before the run, the harness issued `POSIX_FADV_DONTNEED` for the
20,589,025,986-byte capture. `fincore` then reported zero resident pages and
zero resident bytes. After the run it reported 155,488 pages and 636,878,848
resident bytes. This controls the capture's initial cache state, not unrelated
host activity.

## Profile quality

`perf record` sampled `cpu-clock` at 49 Hz with
`--call-graph dwarf,32768`. It captured 7,505 samples over 153.44 seconds and
reported zero lost samples. The top-level self-symbol report is therefore
suitable for choosing the next experiment.

Re-running `perf report` emitted four `cmd__addr2line ... could not read first
record` warnings for a stale entry in the user's build-ID cache. The copied
ELF itself has the expected build ID, and the saved report still resolves the
top-level Rust and libc symbols used below. The warnings make source-line and
deep ancestry attribution less trustworthy; they do not invalidate the
symbolized top-level self report.

The one-minute load average changed from 3.25 before the run to 2.48 after it,
and the process snapshots show unrelated CPU-heavy work. The run used 150.87
seconds of user CPU, 3.77 seconds of system CPU, and peaked at 8,673,320 KiB
RSS, but these are context only. DWARF stack collection and host noise make
the 154.44-second wall time unsuitable as a benchmark result.

## Self-symbol attribution

The following are selected non-overlapping sums of **self** percentages from
`perf report --no-children`. Each sampled instruction belongs to only one
listed self symbol, so the category sums do not double count each other.
They are not inclusive call-path costs.

| Explicit self-symbol family | Self CPU | Interpretation |
| --- | ---: | --- |
| Allocator-family routines | ~18.90% | Heap allocation, reallocation, and free machinery are the largest explicit family. |
| Label-family routines | ~17.69% | Explicit label interning, symbol interning, merge/normalization, lookup, and label-ordering work. |
| Two dominant SipHash `write` rows | 7.68% | Kept separate because hashing is shared infrastructure even though labels drive much of it. |
| `memcmp` | 5.50% | Kept separate because equality work is shared and the self symbol does not identify every caller. |
| Head-family routines | ~5.92% | Explicit head lookup, insertion, and sample-recording symbols. |
| TDigest-family routines | ~3.14% | All explicit TDigest insert/compress/sort symbols, across every statistics user. |
| `record_event_time_skew` directly | 0.09% | Direct self time only; descendant work is represented by its own self symbols. |

The explicit label-family sum deliberately excludes the 7.68% SipHash rows
and 5.50% `memcmp`. It also excludes allocator time. The logical label path is
therefore broader than 17.69%, but simply adding every shared hash, comparison,
and allocation symbol would incorrectly attribute all of those routines to
labels.

The largest individual self rows reinforce this direction:
`_int_malloc` was 6.74%, `Sip13Rounds::write` 6.16%,
`FlatInternedLabelSetStore::intern_encoded` 5.78%, `memcmp` 5.50%,
`HeadBuffer::record_samples` 3.93%, and `ArenaSymbolTable::intern` 3.61%.

## Conclusion and next experiments

The previous roughly 7% event-time-skew hypothesis is not supported. Its
wrapper has 0.09% direct self time, while all named TDigest work across all
statistics users totals about 3.14%. Broken or ambiguous ancestry prevents a
precise inclusive event-skew number, but this profile provides no reason to
make event-skew recording the next optimization target.

The next replay experiments should instead be:

1. Reduce label hashing and equality work, while retaining full equality
   checks and collision correctness.
2. Reduce allocator and protobuf-ownership work, especially transient decode
   and label-value allocations.

Neither profile percentage predicts the benefit of a particular change.
Each candidate needs a controlled A/B using the same real capture, identical
configuration and output checks, one release binary with a runtime comparator
where feasible, and repeated alternating runs on this noisy host.
