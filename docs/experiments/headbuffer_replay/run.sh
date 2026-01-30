#!/usr/bin/env bash

set -euox pipefail

# 1. Build the root Rust cargo project in release mode
echo "Building chronoxide and examples in release mode..."
cargo build --release && cargo build --release --examples

# 2. & 3. Sequentially spawn headbuffer_replay and track memory
FLOAT_ENCODINGS=("raw" "gorilla" "chimp128_baseline" "chimp128_duckdb" "alp_rd_spiraldb" "alp_rd" "alp" "elf")

for ENCODING in "${FLOAT_ENCODINGS[@]}"; do
    echo "Processing encoding: $ENCODING"
    
    LOG_FILE="${ENCODING}.md"
    CSV_FILE="${ENCODING}.csv"
    PLOT_FILE="${ENCODING}.png"
    
    # Spawn chronoxide-ingester
    # We use taskset if available, as seen in the original script's comment
    /usr/bin/time -pv taskset -c 10-16 target/release/examples/headbuffer_replay --capture-path /tmp/new_capture --partition 1 --labelset-store flat_interned --float-encoding "$ENCODING" --int-encoding delta_zigzag --mode sample --output-format markdown > "$LOG_FILE" 2>&1 &
    INGESTER_PID=$(pidof headbuffer_replay)
    
    echo "Spawned headbuffer_replay (PID: $INGESTER_PID) for $ENCODING"
    
    # Track memory
    # memory_monitor_tool.py will wait until the process finishes
    uv run --with psutil --with matplotlib --python 3.14+gil docs/tools/memory_monitor_tool.py --interval 1 --csv "$CSV_FILE" --plot "$PLOT_FILE" $INGESTER_PID

    echo "Finished processing $ENCODING"
    echo "-----------------------------------"
done

echo "All experiments completed."