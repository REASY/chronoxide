#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RESULT_DIR="${RESULT_DIR:-}"
PROJECT_NAME="${PROJECT_NAME:-chronoxide-cross-tsdb}"

if [[ -z "$RESULT_DIR" || "$RESULT_DIR" != /* ]]; then
    echo "RESULT_DIR must be a new absolute path" >&2
    exit 2
fi
if [[ -e "$RESULT_DIR" ]]; then
    echo "RESULT_DIR already exists: $RESULT_DIR" >&2
    exit 2
fi
for command in curl docker sha256sum; do
    if ! command -v "$command" >/dev/null 2>&1; then
        echo "required command is missing: $command" >&2
        exit 2
    fi
done

mkdir -p "$RESULT_DIR/prometheus-data" "$RESULT_DIR/greptime-data" "$RESULT_DIR/metadata"
# The pinned Prometheus image runs as an unprivileged container user. These are
# disposable, run-specific benchmark directories whose contents must also stay
# directly inspectable from the host.
chmod 0777 "$RESULT_DIR/prometheus-data" "$RESULT_DIR/greptime-data"
cp "$SCRIPT_DIR/compose.yaml" "$SCRIPT_DIR/prometheus.yml" "$RESULT_DIR/metadata/"
sha256sum "$RESULT_DIR/metadata/compose.yaml" "$RESULT_DIR/metadata/prometheus.yml" \
    >"$RESULT_DIR/metadata/config.sha256"

export RESULT_DIR
docker compose --project-name "$PROJECT_NAME" --file "$SCRIPT_DIR/compose.yaml" up --detach
docker compose --project-name "$PROJECT_NAME" --file "$SCRIPT_DIR/compose.yaml" config \
    >"$RESULT_DIR/metadata/resolved-compose.yaml"
docker image inspect prom/prometheus:v3.13.1 greptime/greptimedb:v1.1.2 \
    >"$RESULT_DIR/metadata/images.json"

wait_ready() {
    local name="$1"
    local url="$2"
    for _ in {1..120}; do
        if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
            echo "$name is ready"
            return
        fi
        sleep 1
    done
    echo "$name did not become ready: $url" >&2
    exit 1
}

wait_ready prometheus "http://127.0.0.1:${PROMETHEUS_PORT:-9090}/-/ready"
wait_ready greptime "http://127.0.0.1:${GREPTIME_PORT:-4000}/health"

cat <<EOF
stack is ready; keep this run root:
  export RESULT_DIR='$RESULT_DIR'

stop it later with:
  RESULT_DIR='$RESULT_DIR' docker compose --project-name '$PROJECT_NAME' --file '$SCRIPT_DIR/compose.yaml' down
EOF
