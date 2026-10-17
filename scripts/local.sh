#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
root="${GFS_DATA_ROOT:-$PWD/.data/local}"
mkdir -p "$root"
if [[ ! -f "$root/token" ]]; then (umask 077; head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$root/token"); fi
export GFS_TOKEN_FILE="$root/token"
export MASTER_URL="http://127.0.0.1:${GFS_MASTER_PORT:-8080}"
pids=()
cleanup() { for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done; wait || true; }
trap cleanup EXIT
trap 'exit 130' INT TERM
./bin/gfs master -listen "127.0.0.1:${GFS_MASTER_PORT:-8080}" -data "$root/master" &
pids+=("$!")
for i in 0 1 2 3; do
  port=$(( ${GFS_CHUNK_PORT_BASE:-9001} + i ))
  ./bin/gfs chunk -id "chunk-$i" -listen "127.0.0.1:$port" -advertise "http://127.0.0.1:$port" -data "$root/chunk-$i" &
  pids+=("$!")
done
printf 'Local GFS started. Token file: %s\nMaster: %s\nCtrl-C stops these processes and retains data.\n' "$GFS_TOKEN_FILE" "$MASTER_URL"
# Fail together if a child exits, instead of silently leaving a partial lab running.
wait -n "${pids[@]}"
