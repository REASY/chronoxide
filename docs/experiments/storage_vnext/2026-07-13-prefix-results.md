# Paged-symbol format prefix replay

## Scope

This is the two-million-message prefix gate for storage schema 6 and
`symbols.bin` version 3. The source capture and both release binaries were
pinned by SHA-256. The four-run order was:

```text
v7-a -> vnext-a -> vnext-b -> v7-b
```

Raw output is in:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-prefix-20260713-173713
```

The host was running unrelated workloads. Replay and query timings are
therefore exploratory only and must not be used as performance evidence.
Exact bytes, hashes, validation results, semantic fingerprints, and the broad
peak-RSS envelope remain useful.

## Result

The correctness and deterministic-replay prefix gate passed:

- both v7 runs produced identical complete file manifests;
- both vNext runs produced identical complete file manifests;
- all six segment IDs matched across formats;
- every cross-format artifact except `symbols.bin` and its `footer.bin`
  inventory was byte-identical;
- all four semantic and portable fingerprints matched and returned 10 series
  and 10 samples;
- full footer validation passed; and
- the independent readback oracle executed 26 queries with zero mismatches in
  every run.

The readback oracle skipped 16 of 42 expected queries because its isolation
check could not prove those cases on this corpus. Those skips match across all
four runs, but they are a coverage gap rather than a pass.

## Physical layout

| Artifact | v7 bytes | vNext bytes | Delta |
| --- | ---: | ---: | ---: |
| Whole six-segment corpus | 4,422,288,892 | 4,419,977,722 | -2,311,170 (-0.052%) |
| `symbols.bin` | 51,929,007 | 49,617,837 | -2,311,170 (-4.45%) |
| String payload | 46,104,927 | 46,104,927 | 0 |
| Offset tables | 5,824,008 | 2,918,000 | -2,906,008 |
| v3 root, descriptors, and fences | 0 | 546,750 | +546,750 |
| v3 page headers | 0 | 48,160 | +48,160 |

Version 3 encoded 727,995 symbols in 1,505 pages. The result demonstrates the
intended physical change: the string payload and every non-symbol artifact are
unchanged, while page-local `u32` offsets more than pay for the page directory,
fences, and checksums.

## Replay envelope

| Run | Elapsed | Peak RSS KiB | Corpus bytes |
| --- | ---: | ---: | ---: |
| v7-a | 460.41 s | 10,779,232 | 4,422,288,892 |
| vNext-a | 464.27 s | 10,783,540 | 4,419,977,722 |
| vNext-b | 549.31 s | 10,723,888 | 4,419,977,722 |
| v7-b | 446.68 s | 10,778,860 | 4,422,288,892 |

The 85-second difference between the two vNext runs came mainly from one
large segment seal while the machine was noisy. The symbol writer itself was
only a small part of that seal. Peak RSS stayed within roughly 60 MiB across
all four runs, so the prefix found no runaway v3 allocation, but this is not a
precise memory benchmark.

## Same-binary query access-pattern check

A benchmark-only dual-format reader subsequently ran the same release binary
against `v7-a` and `vnext-a`. The expression was:

```promql
sum by (service_name_x55e50a58f9befba7)(rate(http_client_duration_xf5f33b0f6bbd8257_count[15m]))
```

Every cold and warm comparison matched the exact and portable semantic
fingerprints, 10-series/10-sample result shape, complete `QueryStats`, chunk
payload accounting, and logical symbol calls/bytes. The exact fingerprint was
`e77cb5f6a9ae45281071fb44a8525a2e2dd1877b0fc8ec666c286f17b8ed3bb7`;
the portable fingerprint was
`9cb2dd6ca796e83f1756777b9aa1f4f96726c3a13d50e151194610cdb4961004`.

The count-bounded page visitor was tuned using process-issued byte and read
counts, which do not depend on host scheduling noise:

| Layout / reference cap | Logical values | Logical bytes | Root bytes | Page reads | Page bytes | Total issued symbol bytes | Retained charge after cold |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| v7 eager | 71,004 | 1,610,121 | 0 | 0 | 0 | 51,891,711 | 51,891,823 |
| v3 / 4,096 | 71,004 | 1,610,121 | 546,462 | 805 | 26,347,491 | 26,893,953 | 1,350,687 |
| v3 / 32,768 | 71,004 | 1,610,121 | 546,462 | 280 | 9,160,304 | 9,706,766 | 1,350,573 |
| v3 / 65,536 | 71,004 | 1,610,121 | 546,462 | 220 | 7,196,098 | 7,742,560 | 1,350,530 |

The selected 65,536-reference cap reduced cold issued symbol bytes by 85.1%
versus the eager v7 dictionary while reducing retained symbol charge by 97.4%.
Its transient request bookkeeping is count-bounded at roughly 3 MiB before
allocator/hash-table overhead; final materialized label strings are output
memory and are not covered by that bound. The warm v3 run issued six page
reads / 196,385 bytes for matcher lookups after the cold label materialization;
v7 issued no additional eager dictionary bytes. Warm logical work still
matched at 16 values / 248 bytes.

These were correctness/access-pattern checks on a noisy host without the
runner's complete cache-eviction and alternating schedule, so their latency
and RSS samples are not performance evidence. Raw runs and exact binaries are
preserved under:

```text
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-query-bounded-correctness-20260713-190456
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-query-bounded32k-correctness-20260713-190659
/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/storage-vnext-query-bounded64k-correctness-20260713-190849
```

The same-binary SHA-256 values for the 4,096-, 32,768-, and
65,536-reference iterations respectively were
`a2059b91841982facf53633d4ffef870db018dd9459d96efb94975a629c65788`,
`dcb4b196a3be827ba3be94a50c9d11fa60a6904f59f1b77347c419628f8ebb8b`,
and `a49dedaf860ba3e7d282c353ffcc2cdcc1f59bd081906769febc0d6b69ebffda`.
The hardened alternating query runner also completed a no-query dry run at
`storage-vnext-query-ab-dry-bounded64k-20260713-191000`; it inventoried and
hashed both corpora, verified the allowed cross-format differences, copied the
exact binary/provenance, and wrote the balanced schedule.

## Baseline disposition

Schema 6 is retained as the stable, isolated paged-symbol A/B baseline. It is
not being promoted through an expensive four-run full-capture replay before the
next format experiment. The next format may retain `symbols.bin` v3 under a new
explicit segment-schema boundary, but it must not reuse the schema-6 identity
for changed series or chunk-index bytes.

The following were production-promotion gates for schema 6. They are preserved
as historical acceptance criteria, not as an active instruction to run a full
capture:

Before promotion or a full performance claim:

1. Run the full-capture correctness gate with the hardened replay harness.
   It now archives exact binaries/source, enforces deterministic byte gates,
   and requires an explicit named waiver for any readback coverage skip.
2. Add an aggregate open-segment resource governor. The prototype currently
   retains one open symbol file, its root, and up to 256 KiB of decoded pages
   per touched segment.
3. Run the hardened same-binary query A/B during a quiet or isolated host
   window. The runner alternates an even number of repetitions, evicts and
   verifies every segment artifact, and compares cold/warm cohorts separately.
4. Exercise the format in a long-lived API process under an explicit aggregate
   metadata budget.
