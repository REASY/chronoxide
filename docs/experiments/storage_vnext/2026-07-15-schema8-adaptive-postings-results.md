# Schema-8 adaptive-postings initial result

- **Date:** 2026-07-15
- **Status:** The four-million-message capacity and correctness screen passed.
  Warm cached postings-only latency was neutral. Schema 8 is not yet promoted:
  repeated deterministic same-revision replay and a broader controlled query
  A/B remain open.
- **Schema-7 corpus:**
  `/run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/storage-schema7-perf-4m-label-intern-20260714-234047`
- **Schema-8 corpus and raw results:**
  `/run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/storage-schema8-perf-4m-20260715-005902`
- **Complete Schema-7 postings inventory:**
  `/run/media/user/8c0c2e73-2c76-4cfb-bc59-36559b9bfb10/data/chronoxide/postings-inventory-20260715-005151/inventory.json`
- **Design:**
  [Storage schema 8: adaptive exact postings](../../superpowers/specs/archive/storage/2026-07-15-storage-schema8-adaptive-postings-design.md)

## Outcome

Schema 8 reduced exact-postings payloads by 1,029,594,922 bytes, or 72.90%,
on the complete eight-segment corpus. Because the rest of `indexes.puffin`
does not change, the same saving reduced that artifact by 57.71% and the whole
corpus by 15.60%. The actual Schema-8 `indexes.puffin` size exactly matched the
complete pre-replay model.

The real-corpus correctness screen passed: deterministic segment IDs and every
non-index data artifact matched; all eight Schema-8 footers validated; sampled
independent storage fingerprints matched Schema 7; the exhaustive authenticated
walk produced the same decoded-postings fingerprint for all 1,290,200 lists and
351,771,750 references; and 26 independent PromQL readbacks completed with zero
mismatches. The smoke oracle skipped 16 other cases because it could not isolate
them. Those skips are a coverage gap, not a pass.

The isolated postings-heavy query read 75.0% fewer encoded bytes. Its first
eight-warm run measured 1.3426 ms for Schema 7 and 1.3466 ms for Schema 8, a
0.30% Schema-8 regression. A current-source identical-binary rerun measured
1.4548 ms and 1.4087 ms, a 3.17% Schema-8 improvement. The ordering changed
under host noise, so cached latency is classified as neutral rather than a
speedup. A chunk-heavy query was also neutral and dominated by roughly 303 MB
of coalesced chunk reads. This result establishes a capacity win, not an
end-to-end latency win.

## Format and codec distribution

Schema 8 keeps Schema-7 symbols, series, chunk-index, chunk, and auxiliary
index bytes. Only the exact-postings payload changes. Each list independently
uses raw little-endian `u32` references or canonical delta unsigned LEB128;
raw wins ties. The public query facade still receives the same decoded,
strictly increasing `u32` references.

The complete authenticated Schema-7 inventory covered 1,290,200 postings lists
and 351,771,750 references. Its measurement-input fingerprint is
`09c6b525426b1c40b6b4d42e300498c8119483baabb0b2951f0248e65a77698a`.

| Measure                               |                  Value |
|---------------------------------------|-----------------------:|
| Schema-7 raw postings bytes           |          1,412,247,800 |
| Schema-8 selected postings bytes      |            382,652,878 |
| Saved postings bytes                  | 1,029,594,922 (72.90%) |
| Delta-ULEB128 lists                   |     1,123,119 (87.05%) |
| RAW32 lists                           |       167,081 (12.95%) |
| Reference-count p50 / p90 / p95 / p99 |   6 / 54 / 133 / 1,964 |
| Maximum references in one list        |              3,778,291 |

The 167,081 RAW32 selections are not a fallback or sampling artifact. They are
the canonical result when delta encoding is not strictly smaller, principally
for singleton lists whose first absolute reference needs four or five varint
bytes.

## Encoded size

| Artifact               |    Schema 7 bytes |    Schema 8 bytes |                           Change |
|------------------------|------------------:|------------------:|---------------------------------:|
| `indexes.puffin`       |     1,783,826,206 |       754,231,284 |         -1,029,594,922 (-57.71%) |
| `chunks.bin`           |     3,578,303,589 |     3,578,303,589 |                                0 |
| `series.bin`           |     1,154,153,445 |     1,154,153,445 |                                0 |
| `symbols.bin`          |        82,618,420 |        82,618,420 |                                0 |
| `chunk_index.bin`      |               512 |               512 |                                0 |
| `meta.json`            |             5,518 |             5,518 |                                0 |
| `ooo_chunks.bin`       |                 0 |                 0 |                                0 |
| `footer.bin`           |             1,312 |             1,312 | byte content changed as expected |
| Manifest and `CURRENT` |               816 |               816 |                   byte-identical |
| **Total**              | **6,598,909,818** | **5,569,314,896** |     **-1,029,594,922 (-15.60%)** |

All eight segment IDs matched. `chunks.bin`, `series.bin`, `symbols.bin`,
`chunk_index.bin`, `meta.json`, and `ooo_chunks.bin` matched byte for byte in
each corresponding segment. The manifest files also matched. Footer content
changed because the schema and authenticated index fingerprint changed.

## Correctness evidence

The format-independent authenticated inventory walked every exact posting in
both actual corpora. It verified the complete v8/v9 index authentication chain,
decoded each RAW32 or canonical delta-ULEB128 body, and hashed ordered segment
names, list counts, keys, declared counts, and decoded little-endian references.
Both layouts produced logical fingerprint
`0c670e17e36992ae829c118bfa6f71b94b1b15bbb9373deea881652bad78cf89`
over exactly 1,290,200 lists and 351,771,750 references. The Schema-8 walk also
confirmed 1,123,119 delta lists, 167,081 raw lists, and 382,652,878 actual
encoded bytes, exactly matching the pre-replay model. Raw reports are
`adaptive-postings-authenticated-v8.json` and
`adaptive-postings-authenticated-v9.json` in the Schema-8 result directory.

The independent storage verifier sampled 512 series per segment from both
layouts. Both runs covered 4,096 series and chunks, all four kinds present in
the corpus, 19,028 samples, and 456,483 logical chunk bytes. Both produced
verified-selection fingerprint
`32d98366000f0a6afbbfc93a5988a0ceeddb62c9f9dc513c79dd93a1955bccac`.
Their logical counts and metadata-read counters matched. This sampled
series/chunk readback is independent of the exhaustive postings proof above.

The Schema-8 smoke run validated every footer and scanned eight segments with
154,902,724 datapoints, 17,286,077 series, and 17,286,077 chunks. It exercised
Float, Histogram, ExponentialHistogram, and Summary data. The independent
readback oracle executed 26 of 42 expected queries with zero mismatches; the
other 16 were explicitly reported as isolation-check skips.

A real equality query produced identical exact and portable semantic
fingerprints, result shape, logical chunk counters, and every `QueryStats`
field except the intended encoded-postings byte counter. It read 10,397,652
postings bytes on Schema 7 and 2,599,659 on Schema 8, a 75.0% reduction.

The current source also routes direct public store/reader queries, PromQL,
native Histogram and ExponentialHistogram paths, and label/metric discovery
through the schema-neutral facade. Equality matcher ordering uses the protected
decoded reference count rather than compressed payload length. This is
necessary because encoded length is not a cardinality proxy and otherwise
Schema 7 and Schema 8 could perform intersections in different orders even
when their logical postings are identical.

## Read evidence

The postings-isolating query was:

```promql
count({
  telemetry_sdk_language_x8a2a2326c6c57c55="java",
  telemetry_sdk_language_x8a2a2326c6c57c55="dotnet"
})
```

The contradictory equalities force two long postings reads and an intersection
but no series materialization, chunk reads, or payload decoding. Each layout
ran once cold in a fresh session and eight more times warm. Exact and portable
semantic fingerprints, the zero-result shape, and all `QueryStats` matched
except the explicitly expected encoded-byte counter.

| Measure                        |  Schema 7 |  Schema 8 |          Change |
|--------------------------------|----------:|----------:|----------------:|
| Exact-postings reads per run   |         2 |         2 |               0 |
| Encoded postings bytes per run | 4,308,056 | 1,077,125 |          -75.0% |
| First-session latency          |   4.63 ms |   9.06 ms | noisy; no claim |
| Warm median latency            | 1.4548 ms | 1.4087 ms |          -3.17% |
| Chunk and payload bytes        |         0 |         0 |               0 |

The first-session result is only one sample and does not imply a cold operating-
system page cache. The warm result is cached decode CPU plus query bookkeeping;
the current-source run favors Schema 8, but the preceding identical schedule
measured 1.3426 ms versus 1.3466 ms and favored Schema 7 by 0.30%. The changing
ordering shows that canonical varint decoding is operationally bounded for
this case, not that Schema 8 improves latency.

The separate equality query which reached chunk evaluation reduced postings
bytes from 10.40 MB to 2.60 MB, but still read 253.02 MB of logical chunk data
through 303.41 MB of coalesced spans. Its one-run latency was 46.51 seconds on
Schema 7 and 47.85 seconds on Schema 8. Those values are noisy and the postings
work is too small a fraction of the query to support an end-to-end claim.

The final public-reader and matcher-order integration was rerun against both
layouts with one release binary, SHA-256
`bd30adfc45c6d2423367275cce6060fad4563c9bc6bdee878e1a280e4277850d`.
Its raw files are `query-postings-intersection-current-schema7.json` and
`query-postings-intersection-current-schema8.json` in the Schema-8 result
directory. Every fingerprint and `QueryStats` field matched except the intended
encoded-postings byte field.

## Replay observation

The Schema-8 replay consumed exactly four million source messages and emitted
the same eight segment IDs as the existing Schema-7 corpus.

| Measure    | Existing Schema-7 run |   Schema-8 run |
|------------|----------------------:|---------------:|
| Wall time  |              13:17.71 |       13:15.33 |
| User CPU   |              789.19 s |       787.17 s |
| System CPU |                9.04 s |         8.39 s |
| Peak RSS   |        12,207,400 KiB | 12,194,228 KiB |

These are not a controlled write-latency A/B. The Schema-7 run used an earlier
revision and profiling conditions, while the machine was running unrelated
workloads. The close values are evidence against an obvious large regression,
but they do not establish a sealing-time improvement or a precise overhead.

## Gate disposition and next evidence

The initial capacity gate passes: exact postings are 72.90% smaller, exceeding
the 65% threshold, and total corpus size improves by 15.60%. The available
readbacks and fingerprints make the initial correctness screen pass. Across
the two postings-only schedules, warm cached latency is neutral: one run was a
0.30% regression and the current-source rerun was a 3.17% improvement.

Schema 8 should remain an explicit experiment until the promotion gate adds:

1. a repeated Schema-8 replay at one revision with byte-identical output;
2. alternating Schema-7/Schema-8 replays and queries built from the same source
   and binary, with explicit cache budgets and host-pressure metadata;
3. the full controlled query matrix: long equality, multi-equality, sparse and
   broad regex, negative matcher, no-result, and zero-postings control, with
   cold and warm measurements; and
4. closure or an explicit retained waiver for the 16 isolation-check skips.

Until those are complete, the supported conclusion is deliberately narrow:
adaptive exact-postings compression is a strong capacity improvement with
neutral measured cached decode latency, but it has not yet demonstrated an
end-to-end query-latency advantage or completed the default-format promotion
gate.
