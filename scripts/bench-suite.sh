#!/usr/bin/env bash
#
# Runs the published benchmark suite and writes the reports under
# docs/benchmarks/.
#
# Every configuration is measured with the same harness, the same workload and
# the same machine, and a real redis-server is measured alongside as a baseline.
# A throughput number with nothing beside it says almost nothing; the point of
# this script is that every number here can be compared to another one that was
# produced under identical conditions.
#
# Usage: scripts/bench-suite.sh [duration] [output-dir]

set -euo pipefail

DURATION="${1:-10s}"
OUT="${2:-docs/benchmarks}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d)"
SERVER_PORT=7501
ENGINE_PORT=7502
REDIS_PORT=7503

BENCH=/tmp/rfs-bench-suite
SERVER=/tmp/rfs-server-suite
ENGINE="$ROOT/engine/target/release/rfs-engine"

pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m!! \033[0m %s\n' "$*" >&2; }

# wait_ready polls a port until it answers PING, so the harness never starts
# measuring a server that is still opening its data directory.
wait_ready() {
  local port="$1" name="$2" deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if [[ "$(redis-cli -p "$port" PING 2>/dev/null)" == "PONG" ]]; then
      return 0
    fi
    sleep 0.2
  done
  echo "$name on port $port never became ready" >&2
  return 1
}

log "building"
go build -o "$BENCH" ./cmd/rfs-bench
go build -o "$SERVER" ./cmd/server
if [[ ! -x "$ENGINE" ]]; then
  log "building the storage engine in release mode"
  (cd engine && cargo build --release --bins >/dev/null 2>&1)
fi

mkdir -p "$OUT"

# start_ours brings up a node with the given flags and returns once it answers.
start_ours() {
  local dir="$1"; shift
  rm -rf "$dir"
  "$SERVER" -addr "127.0.0.1:$SERVER_PORT" -dir "$dir" -metrics-addr "" "$@" \
    >"$WORK/server.log" 2>&1 &
  pids+=($!)
  wait_ready "$SERVER_PORT" "the server"
}

stop_ours() {
  redis-cli -p "$SERVER_PORT" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
  sleep 0.5
}

# run_suite measures one configuration across the standard sweep.
run_suite() {
  local label="$1" port="$2" file="$3" title="$4"
  log "measuring $label"
  "$BENCH" -addr "127.0.0.1:$port" \
    -suite standard -d "$DURATION" -warmup 3s \
    -keys 200000 -value 64 -c 50 \
    -markdown "$OUT/$file.md" -json "$OUT/$file.json" \
    -title "$title"
}

# ---------------------------------------------------------------------------
# 1. This server, in-process engine, write-ahead log with the default policy.
# ---------------------------------------------------------------------------
start_ours "$WORK/data-everysec" -appendfsync everysec
run_suite "memory engine, WAL everysec" "$SERVER_PORT" "memory-everysec" \
  "redis-from-scratch: in-process engine, WAL fsync everysec"
stop_ours

# ---------------------------------------------------------------------------
# 2. Same, with an fsync before every acknowledgement. This is the price of
#    the strongest single-node durability guarantee, and the gap between this
#    and the run above is the only honest way to quote that price.
# ---------------------------------------------------------------------------
start_ours "$WORK/data-always" -appendfsync always
"$BENCH" -addr "127.0.0.1:$SERVER_PORT" -workload set -c 50 -P 1 \
  -d "$DURATION" -warmup 3s -keys 200000 -preload=false \
  -label "set P=1 fsync=always" \
  -markdown "$OUT/durability-cost.md" -json "$OUT/durability-cost.json" \
  -title "redis-from-scratch: the cost of fsync-before-acknowledge"
stop_ours

# ---------------------------------------------------------------------------
# 3. Same, with persistence off entirely: a pure cache. This is the ceiling
#    the durable configurations are measured against.
# ---------------------------------------------------------------------------
start_ours "$WORK/data-cache" -appendonly no -no-save
run_suite "memory engine, no persistence" "$SERVER_PORT" "memory-cache" \
  "redis-from-scratch: in-process engine, persistence disabled"
stop_ours

# ---------------------------------------------------------------------------
# 4. The Rust LSM engine over gRPC. Slower per operation by construction: every
#    command crosses a process boundary. What it buys is a dataset that is not
#    bounded by RAM.
# ---------------------------------------------------------------------------
if [[ -x "$ENGINE" ]]; then
  rm -rf "$WORK/engine-data"
  "$ENGINE" --addr "127.0.0.1:$ENGINE_PORT" --dir "$WORK/engine-data" \
    --fsync everysec --memtable-bytes $((32 * 1024 * 1024)) \
    >"$WORK/engine.log" 2>&1 &
  pids+=($!)
  sleep 2
  start_ours "$WORK/data-lsm" -engine lsm -engine-addr "127.0.0.1:$ENGINE_PORT"
  "$BENCH" -addr "127.0.0.1:$SERVER_PORT" \
    -suite standard -d "$DURATION" -warmup 3s \
    -keys 50000 -value 64 -c 50 \
    -markdown "$OUT/lsm-engine.md" -json "$OUT/lsm-engine.json" \
    -title "redis-from-scratch: Rust LSM engine over gRPC"
  stop_ours
else
  warn "the storage engine binary is missing; skipping the LSM measurements"
fi

# ---------------------------------------------------------------------------
# 5. A real redis-server, measured by the same harness on the same machine.
# ---------------------------------------------------------------------------
if command -v redis-server >/dev/null 2>&1; then
  redis-server --port "$REDIS_PORT" --bind 127.0.0.1 --save '' --appendonly no \
    --dir "$WORK" >"$WORK/redis.log" 2>&1 &
  pids+=($!)
  wait_ready "$REDIS_PORT" "redis-server"
  run_suite "redis-server baseline" "$REDIS_PORT" "redis-baseline" \
    "redis-server baseline, persistence disabled, same harness and machine"
  redis-cli -p "$REDIS_PORT" SHUTDOWN NOSAVE >/dev/null 2>&1 || true
else
  warn "redis-server is not installed; skipping the baseline"
fi

log "reports written to $OUT"
ls -1 "$OUT"
