#!/usr/bin/env bash

set -euo pipefail

BENCH_BIN="target/release/examples/spec_chunk_io_bench"
DATASET_DIR="${DATASET_DIR:-/media/android_dev_disk/temp}"
RESULT_DIR="${RESULT_DIR:-docs/experiments/spec_chunk_io_bench_results/$(date +%Y%m%d-%H%M%S)}"
SEGMENTS="${SEGMENTS:-4}"
TOTAL_SERIES="${TOTAL_SERIES:-8192}"
CANDIDATE_SERIES="${CANDIDATE_SERIES:-2048}"
CHUNKS_PER_SERIES="${CHUNKS_PER_SERIES:-4}"
CHUNK_SIZE_KB="${CHUNK_SIZE_KB:-64}"
OOO_PERCENT="${OOO_PERCENT:-0}"
PATTERN="${PATTERN:-strided}"
SEED="${SEED:-4263414480906202897}"
QUEUE_DEPTHS="${QUEUE_DEPTHS:-8,32,128,256}"
GENERATE_DATASET="${GENERATE_DATASET:-0}"
DROP_CACHES="${DROP_CACHES:-1}"
TIME_BIN="${TIME_BIN:-/usr/bin/time}"

common_args=(
  --dir "$DATASET_DIR"
  --segments "$SEGMENTS"
  --total-series "$TOTAL_SERIES"
  --candidate-series "$CANDIDATE_SERIES"
  --chunks-per-series "$CHUNKS_PER_SERIES"
  --chunk-size-kb "$CHUNK_SIZE_KB"
  --ooo-percent "$OOO_PERCENT"
  --pattern "$PATTERN"
  --seed "$SEED"
)

mkdir -p "$RESULT_DIR"

echo "result_dir=$RESULT_DIR"
echo "dataset_dir=$DATASET_DIR"
echo "queue_depths=$QUEUE_DEPTHS"

cargo build --release --features io_uring -p chronoxide-core --example spec_chunk_io_bench

if [[ "$GENERATE_DATASET" == "1" ]]; then
  echo "generating dataset"
  "$BENCH_BIN" \
    "${common_args[@]}" \
    --iterations 1 \
    --warmup-iters 0 \
    --mode pread \
    --keep-files \
    >"$RESULT_DIR/generate.stdout" \
    2>"$RESULT_DIR/generate.stderr"
else
  echo "using existing dataset"
fi

drop_caches() {
  sync
  if [[ "$DROP_CACHES" == "1" ]]; then
    echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null
  fi
}

run_case() {
  local label="$1"
  shift
  local stdout_file="$RESULT_DIR/${label}.stdout"
  local stderr_file="$RESULT_DIR/${label}.stderr"
  local time_file="$RESULT_DIR/${label}.time"

  echo "running $label"
  drop_caches

  "$TIME_BIN" -v -o "$time_file" "$BENCH_BIN" \
    "${common_args[@]}" \
    --iterations 1 \
    --warmup-iters 0 \
    --reuse-existing \
    "$@" \
    >"$stdout_file" \
    2>"$stderr_file"

  tail -n +2 "$stdout_file" >>"$RESULT_DIR/summary.csv"
}

printf "mode,queue_depth,iterations,requests,logical_mib,total_ms,avg_ms,min_ms,p50_ms,p95_ms,p99_ms,throughput_mib_s\n" >"$RESULT_DIR/summary.csv"

run_case "pread" --mode pread

IFS=',' read -r -a qds <<<"$QUEUE_DEPTHS"
for qd in "${qds[@]}"; do
  run_case "io_uring_qd${qd}" --mode io-uring --queue-depths "$qd"
done

echo "summary:"
cat "$RESULT_DIR/summary.csv"
