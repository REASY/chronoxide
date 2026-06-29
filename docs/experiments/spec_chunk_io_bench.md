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

```bash
cargo run -p chronoxide-core --release --features io_uring \
  --example spec_chunk_io_bench -- \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 4 \
  --total-series 8192 \
  --candidate-series 2048 \
  --chunks-per-series 2 \
  --chunk-size-kb 64 \
  --ooo-percent 10 \
  --pattern strided \
  --iterations 5 \
  --warmup-iters 1 \
  --mode both \
  --queue-depths 8,32,128 \
  --keep-files
```

For a heavier high-fanout query shape:

```bash
cargo run -p chronoxide-core --release --features io_uring \
  --example spec_chunk_io_bench -- \
  --dir /mnt/nvme/chronoxide-spec-chunk-io-heavy \
  --segments 6 \
  --total-series 65536 \
  --candidate-series 16384 \
  --chunks-per-series 2 \
  --chunk-size-kb 128 \
  --ooo-percent 20 \
  --pattern random \
  --iterations 5 \
  --warmup-iters 1 \
  --mode both \
  --queue-depths 32,128,256 \
  --keep-files
```

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
or closer-to-device measurements, run against files larger than RAM, recreate
the dataset between runs, or drop caches between runs when acceptable for the
test host.

Useful supporting tools:

```bash
perf stat -d -- \
  target/release/examples/spec_chunk_io_bench \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 4 \
  --total-series 8192 \
  --candidate-series 2048 \
  --chunks-per-series 2 \
  --chunk-size-kb 64 \
  --ooo-percent 10 \
  --pattern strided \
  --iterations 5 \
  --warmup-iters 1 \
  --mode both \
  --queue-depths 8,32,128 \
  --keep-files
```

For syscall-level checks:

```bash
strace -c target/release/examples/spec_chunk_io_bench \
  --dir /mnt/nvme/chronoxide-spec-chunk-io \
  --segments 2 \
  --total-series 4096 \
  --candidate-series 1024 \
  --chunks-per-series 2 \
  --chunk-size-kb 64 \
  --mode both \
  --queue-depths 32 \
  --keep-files
```

## Caveats

This is not an end-to-end query benchmark. It deliberately bypasses selector
evaluation, label materialization, chunk decoding, and PromQL execution. Use it
to evaluate the explicit chunk IO strategy described in the storage spec.

For fair comparisons, keep the dataset and candidate plan identical between
`pread` and `io_uring`; `--mode both` does this in one run.
