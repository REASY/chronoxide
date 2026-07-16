# Source payload-reuse results

Date: 2026-07-16

## Decision

Do not keep either experiment.

- Reusing the source-message payload `Vec` applies to both capture replay and
  live Kafka, but reduced retired instructions by only 0.056% and branches by
  only 0.142%. It did not reduce peak RSS.
- Reusing a persistent Zstd decompression context applies only to capture-file
  replay. Kafka broker compression is decoded by librdkafka before Chronoxide
  receives the borrowed message payload. The persistent context increased
  instructions, branches, task CPU, and cycles in the isolated comparison.

Both candidates were removed. The ownership plumbing and retained-capacity
policy are not justified by these measurements.

## Experiments

The shared-buffer candidate passed a caller-owned allocation through
`Source::next_message`, the capture reader, Kafka source, capturing source, and
the ingester loop. Existing source errors and paths that do not return a
message deliberately dropped the buffer rather than hiding errors or changing
source semantics.

The second candidate additionally retained one `zstd::bulk::Decompressor` and
one compressed-input buffer in `OtlpCaptureReader`. It was measured separately
against the shared-buffer candidate so that replay-only Zstd behavior could
not be mistaken for a Kafka-ingest improvement.

## Method

- Capture:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/kafka-capture-001/partition-1.capture`
- Raw result root:
  `/run/media/user/8e0a3aed-ff44-4990-b8d9-6c4dc5efdb01/data/chronoxide/source-payload-reuse-ab-20260716-204928`
- Message limit: 250,000 per run.
- Original-control binary SHA-256:
  `74e169c36cebf161352808fb3ad8dfa50ba2b04fbd30aee0709b9535845d1cc3`.
- Shared-buffer binary SHA-256:
  `05c365ec60729e667dbedf464a9314bae73d2891634bf32ff16059c2ddd2f87f`.
- Shared-buffer plus persistent-context binary SHA-256:
  `ec09c9b15fa2fea44cc38dbc9e15f5fe1c90e68b6b761b05292e4ecb739a3854`.
- Shared-buffer schedule: control, candidate, candidate, control.
- Persistent-context schedule: buffer-only, context, context, buffer-only.

The host was noisy, including a much higher system load during the first
shared-buffer candidate run. Elapsed time is therefore not used to accept or
reject that candidate. Retired instructions and branches were stable and show
that the maximum available gain is immaterial. The isolated context comparison
used adjacent alternating runs and was consistently flat to worse.

## Results

### Shared payload buffer versus original

Values are means of two runs per variant.

| Metric | Original | Shared buffer | Difference |
| --- | ---: | ---: | ---: |
| Instructions | 789,959,900,971 | 789,514,014,646 | -0.056% |
| Branches | 142,502,856,426 | 142,300,797,720 | -0.142% |
| Peak RSS | 5,476,400 KiB | 5,479,306 KiB | +2,906 KiB (+0.053%) |

The small work-counter reduction is insufficient to justify changing ownership
across every source implementation. Reusing the largest seen payload could
also retain an unbounded one-off allocation unless a capacity policy were
added, increasing code and test burden further.

### Persistent capture Zstd context versus shared buffer only

Values are means of two runs per variant.

| Metric | Buffer only | Persistent context | Difference |
| --- | ---: | ---: | ---: |
| Task clock | 62,801.58 ms | 63,668.51 ms | +1.380% |
| Cycles | 347,569,430,537 | 351,636,728,459 | +1.170% |
| Instructions | 789,397,679,470 | 790,078,265,798 | +0.086% |
| Branches | 142,284,337,733 | 142,416,524,425 | +0.093% |
| Peak RSS | 5,479,692 KiB | 5,479,478 KiB | -214 KiB (-0.004%) |

This does not support keeping a replay-only context. The small instruction
increase could include code-layout effects, but it provides no reason to run a
more expensive confirmation trial.

## Correctness evidence

- All manifests from both A/B schedules were byte-identical, with digest
  `3094a23de602ff94e7e5898556c4935600b349b2933b31209d111fb2c1581423`.
- While the experiment existed, focused capture-reader, source, ingester, and
  source-level end-to-end tests passed.
- The experimental code was then removed, and every touched production file
  was restored byte-for-byte to the committed implementation.

The next replay optimization should target shared OTLP decode, label
canonicalization, or head-series lookup work. Source-buffer allocation is not
a material bottleneck on this corpus.
