#!/usr/bin/env bash

set -euo pipefail

# 1. Build the root Rust cargo project in release mode
echo "Building chronoxide in release mode..."
cargo build --release

# 2. & 3. Sequentially spawn chronoxide-ingester and track memory
STORES=("naive" "flat_interned" "key_set_dict_encoded")
CONFIG_FILE="chronoxide-ingester/config/dc/sg/metric.toml"

for STORE in "${STORES[@]}"; do
    echo "Processing store: $STORE"
    
    LOG_FILE="${STORE}.log"
    CSV_FILE="${STORE}.csv"
    PLOT_FILE="${STORE}.png"
    
    # Spawn chronoxide-ingester
    # We use taskset if available, as seen in the original script's comment
    INGESTION_LABELSET_STORE=$STORE CONFIG_FILE=$CONFIG_FILE /usr/bin/time -pv taskset -c 10-16 ./target/release/chronoxide-ingester > "$LOG_FILE" 2>&1 &
    INGESTER_PID=$(pidof chronoxide-ingester)
    
    echo "Spawned chronoxide-ingester (PID: $INGESTER_PID) for $STORE"
    
    # Track memory
    # memory_monitor_tool.py will wait until the process finishes
    uv run --with psutil --with matplotlib --python 3.14+gil docs/experiments/labelset_store/memory_monitor_tool.py $INGESTER_PID --interval 1 --csv "$CSV_FILE" --plot "$PLOT_FILE"
    
    echo "Finished processing $STORE"
    echo "-----------------------------------"
done

echo "All experiments completed."