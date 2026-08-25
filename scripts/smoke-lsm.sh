#!/usr/bin/env bash
#
# Drives the Go server against the Rust storage engine and kills the engine to
# check that recovery works across the process boundary.
#
# The two processes are only ever wired together at run time, so a broken
# contract between them - a protobuf field that moved, a status code the client
# stopped handling - would pass every other test in the repository.

set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

WORK="$(mktemp -d)"
SERVER_PORT=7601
ENGINE_PORT=7602
pids=()

cleanup() {
  for pid in "${pids[@]:-}"; do kill "$pid" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

log() { printf '\033[1;34m==>\033[0m %s\n' "$*"; }
fail() { printf '\033[1;31mFAIL\033[0m %s\n' "$*" >&2; exit 1; }

SERVER="$ROOT/bin/rfs-server"
ENGINE="$ROOT/engine/target/release/rfs-engine"
[[ -x "$SERVER" ]] || fail "missing $SERVER; run make build"
[[ -x "$ENGINE" ]] || fail "missing $ENGINE; run make build-engine"

start_engine() {
  "$ENGINE" --addr "127.0.0.1:$ENGINE_PORT" --dir "$WORK/engine" \
    --fsync always --memtable-bytes $((4 * 1024 * 1024)) >>"$WORK/engine.log" 2>&1 &
  pids+=($!)
}

wait_for() {
  local port="$1" what="$2" deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    [[ "$(redis-cli -p "$port" PING 2>/dev/null)" == "PONG" ]] && return 0
    sleep 0.2
  done
  cat "$WORK"/*.log >&2 || true
  fail "$what never became ready"
}

# wait_keyspace waits until a command that actually reaches the storage engine
# succeeds.
#
# PING is answered by the server alone and says nothing about the engine, so
# using it as the readiness check would start verifying while the gRPC client is
# still reconnecting and report every read as lost data. The server correctly
# returns an error rather than serving stale values in that window, which is why
# the difference matters.
wait_keyspace() {
  local port="$1" deadline=$((SECONDS + 30))
  while (( SECONDS < deadline )); do
    if redis-cli -p "$port" DBSIZE 2>/dev/null | grep -qE '^[0-9]+$'; then
      return 0
    fi
    sleep 0.2
  done
  cat "$WORK"/*.log >&2 || true
  fail "the storage engine never became reachable through the server"
}

log "starting the storage engine"
start_engine
sleep 2

log "starting the server against it"
"$SERVER" -addr "127.0.0.1:$SERVER_PORT" -engine lsm \
  -engine-addr "127.0.0.1:$ENGINE_PORT" -metrics-addr "" >>"$WORK/server.log" 2>&1 &
pids+=($!)
wait_for "$SERVER_PORT" "the server"
wait_keyspace "$SERVER_PORT"

R() { redis-cli -p "$SERVER_PORT" "$@"; }

log "exercising the command surface through the engine"
[[ "$(R SET foo bar)" == "OK" ]]            || fail "SET"
[[ "$(R GET foo)" == "bar" ]]               || fail "GET"
[[ "$(R APPEND foo '!')" == "4" ]]          || fail "APPEND"
[[ "$(R GET foo)" == "bar!" ]]              || fail "APPEND result"
[[ "$(R INCR n)" == "1" ]]                  || fail "INCR"
[[ "$(R INCRBY n 41)" == "42" ]]            || fail "INCRBY"
[[ "$(R SETNX fresh v)" == "1" ]]           || fail "SETNX on a new key"
[[ "$(R SETNX fresh other)" == "0" ]]       || fail "SETNX on an existing key"
[[ "$(R TTL foo)" == "-1" ]]                || fail "TTL with no expiry"
R SET withttl v EX 100 >/dev/null
[[ "$(R TTL withttl)" == "100" ]]           || fail "TTL"
[[ "$(R EXISTS foo n fresh)" == "3" ]]      || fail "EXISTS"
[[ "$(R DEL fresh)" == "1" ]]               || fail "DEL"

log "FLUSHALL must remove keys that carry a TTL"
R FLUSHALL >/dev/null
remaining="$(R --scan 2>/dev/null | wc -l | tr -d ' ')"
[[ "$remaining" == "0" ]] || fail "FLUSHALL left $remaining keys behind"

log "writing 500 keys"
for i in $(seq 1 500); do R SET "k:$i" "v:$i" >/dev/null; done
[[ "$(R GET k:500)" == "v:500" ]] || fail "the last write is missing"

log "killing the engine with SIGKILL"
pkill -9 -f "rfs-engine --addr 127.0.0.1:$ENGINE_PORT" || true
sleep 1

log "restarting the engine and checking what survived"
start_engine
wait_keyspace "$SERVER_PORT"

lost=0
for i in $(seq 1 500); do
  [[ "$(R GET "k:$i")" == "v:$i" ]] || lost=$((lost + 1))
done
[[ "$lost" == "0" ]] || fail "$lost of 500 acknowledged writes were lost to SIGKILL"

grep -q "recovered" "$WORK/engine.log" || fail "the engine did not log a recovery"

log "500 of 500 acknowledged writes survived SIGKILL of the storage engine"
