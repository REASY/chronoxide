# Spec Chunk IO Benchmark

This benchmark exercises the storage-spec chunk read shape without requiring the
full PromQL query path to be implemented.

It generates immutable segment-like directories containing:

- `chunks.bin`
- `ooo_chunks.bin`
- `chunk_index.bin`

Then it plans reads as if selector evaluation had already produced candidate
`series_ref`s, and compares serial `pread` with batched `io_uring` reads over
the selected chunk byte ranges.

The benchmark intentionally keeps metadata simple. It focuses on the part of
the spec where `io_uring` should help: many independent reads from large chunk
files, with request sizes in the tens or hundreds of KiB.

## Build

Run from the repository root.

```bash
cargo build -p chronoxide-core --release --features io_uring --example spec_chunk_io_bench
```

`io_uring` mode requires Linux and the `io_uring` feature. `pread` mode works on
Unix-like development machines and is useful for smoke tests.

## Smoke Test

Use sparse files for a quick functional check only.

```bash
cargo run -p chronoxide-core --example spec_chunk_io_bench -- \
  --segments 1 \
  --total-series 16 \
  --candidate-series 4 \
  --chunks-per-series 2 \
  --chunk-size-kb 4 \
  --iterations 1 \
  --warmup-iters 0 \
  --mode pread \
  --sparse
```

Do not use `--sparse` for real performance numbers. Sparse files can avoid
physical device IO and make storage results misleading.

## Linux NVMe Run

Pick a benchmark directory on the SSD/NVMe device you want to measure.
Generate the dataset once:

```bash
cargo run -p chronoxide-core --release --features io_uring \
  --example spec_chunk_io_bench -- \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 4 \
  --total-series 8192 \
  --candidate-series 2048 \
  --chunks-per-series 4 \
  --chunk-size-kb 64 \
  --pattern strided \
  --iterations 1 \
  --warmup-iters 0 \
  --mode pread \
  --keep-files
```

Then run the cold-cache comparison. The helper drops caches before `pread` and
before each individual `io_uring` queue depth, so the rows are comparable:

```bash
DATASET_DIR=/mnt/nvme/chronoxide-spec-chunk-io \
QUEUE_DEPTHS=8,32,128,256 \
docs/experiments/iouring/spec_chunk_io_bench_run.sh
```

The script writes one stdout, stderr, and `/usr/bin/time -v` file per measured
case, plus `summary.csv`, under
`docs/experiments/iouring/spec_chunk_io_bench_results/<timestamp>/`.

## Parameters

- `--segments`: number of immutable segment directories.
- `--total-series`: number of series per segment.
- `--candidate-series`: number of selected series after hypothetical selector
  evaluation.
- `--chunks-per-series`: chunks available per series in each segment.
- `--chunk-size-kb`: logical chunk bytes read per request.
- `--ooo-percent`: percentage of chunks routed to `ooo_chunks.bin`.
- `--pattern`: selected series locality: `contiguous`, `strided`, or `random`.
- `--mode`: `pread`, `io-uring`, or `both`.
- `--queue-depths`: comma-separated io_uring queue depths.
- `--keep-files`: keep generated files after the run.
- `--reuse-existing`: open an existing generated dataset instead of replacing
  it. The shape arguments must match the existing files.
- `--sparse`: create sparse data files for smoke tests only.

## Output

The benchmark prints one CSV row per reader mode and queue depth.

```text
mode,queue_depth,iterations,requests,logical_mib,total_ms,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,throughput_mib_s
```

Interpretation:

- `requests`: chunk byte ranges read per iteration.
- `logical_mib`: logical chunk bytes read per iteration.
- `total_ms`: elapsed time across measured iterations.
- `avg_ms`, `min_ms`, `p50_ms`, `p95_ms`, `p99_ms`: per-iteration latency.
- `throughput_mib_s`: aggregate logical read throughput across measured
  iterations.

The benchmark also writes a short stderr summary with dataset shape, selected
request counts, and total generated artifact entries.

## Measurement Notes

Use a Linux host with an SSD/NVMe device. `io_uring` gains are easiest to see
when the workload has enough independent reads to keep the device busy:

- increase `--candidate-series`
- increase `--segments`
- increase `--chunks-per-series`
- test queue depths from `32` through `256`

Avoid measuring only warm page-cache reads unless that is the goal. For cold
or closer-to-device measurements, use `--reuse-existing` and drop caches before
each measured case. Do not use `--mode both` for a cold-cache comparison: the
first mode warms the page cache for the modes that follow.

## Recent Result

Cold-cache run on 2026-06-29 against `/media/android_dev_disk/temp` on ext4 over
NVMe, using:

```bash
GENERATE_DATASET=0 \
DATASET_DIR=/media/android_dev_disk/temp \
QUEUE_DEPTHS=8,32,128,256 \
docs/experiments/iouring/spec_chunk_io_bench_run.sh
```

Dataset shape:

```text
segments=4 total_series=8192 candidate_series=2048 chunks_per_series=4 chunk_size_kb=64
requests=32768 logical_mib=2048.00 pattern=Strided
```

Summary:

```text
mode,queue_depth,total_ms,throughput_mib_s,speedup_vs_pread
pread,-,3719.519,550.61,1.00x
io_uring,8,2043.805,1002.05,1.82x
io_uring,32,1385.824,1477.82,2.68x
io_uring,128,1213.998,1686.99,3.06x
io_uring,256,1115.908,1835.28,3.33x
```

For this cold, high-fanout workload, batched `io_uring` substantially outpaced
the serial `pread` baseline. Warm-cache runs remain much closer because the
bottleneck shifts toward page-cache traversal, kernel copy, and allocation
overhead rather than NVMe queueing.

Useful supporting tools:

```bash
perf stat -d -- \
  target/release/examples/spec_chunk_io_bench \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 4 \
  --total-series 8192 \
  --candidate-series 2048 \
  --chunks-per-series 4 \
  --chunk-size-kb 64 \
  --pattern strided \
  --iterations 1 \
  --warmup-iters 0 \
  --mode io-uring \
  --queue-depths 128 \
  --reuse-existing
```

For syscall-level checks:

```bash
strace -c target/release/examples/spec_chunk_io_bench \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 4 \
  --total-series 8192 \
  --candidate-series 2048 \
  --chunks-per-series 4 \
  --chunk-size-kb 64 \
  --iterations 1 \
  --warmup-iters 0 \
  --mode io-uring \
  --queue-depths 128 \
  --reuse-existing
```

## Manual Cold Runs

The helper script is preferred, but the equivalent manual sequence is:

```bash
sync
echo 3 | sudo tee /proc/sys/vm/drop_caches

/usr/bin/time -v target/release/examples/spec_chunk_io_bench \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 4 \
  --total-series 8192 \
  --candidate-series 2048 \
  --chunks-per-series 4 \
  --chunk-size-kb 64 \
  --iterations 1 \
  --warmup-iters 0 \
  --mode pread \
  --reuse-existing

sync
echo 3 | sudo tee /proc/sys/vm/drop_caches

/usr/bin/time -v target/release/examples/spec_chunk_io_bench \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 4 \
  --total-series 8192 \
  --candidate-series 2048 \
  --chunks-per-series 4 \
  --chunk-size-kb 64 \
  --iterations 1 \
  --warmup-iters 0 \
  --mode io-uring \
  --queue-depths 128 \
  --reuse-existing
```

Repeat the `io_uring` command with one queue depth at a time.

## Caveats

This is not an end-to-end query benchmark. It deliberately bypasses selector
evaluation, label materialization, chunk decoding, and PromQL execution. Use it
to evaluate the explicit chunk IO strategy described in the storage spec.

Serial `pread` is a deliberately simple baseline. For cold high-fanout reads,
it can underfeed NVMe devices compared with batched `io_uring`. A stricter
baseline would add parallel `pread` workers.
