#!/usr/bin/env bash
#
# Compare go-tron storage/sync behaviour across Erigon-style prune modes.
#
# The script is intentionally a harness, not a pass/fail test: it emits one
# JSON object per run so repeated samples can be graphed by mode.
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/../.." && pwd)"
GTRON="${GTRON:-$BASEDIR/build/bin/gtron}"
MODES="full,minimal,archive"
PROFILE="producer"
TARGET_BLOCKS=30
TIMEOUT=180
WORKDIR=""
OUTPUT=""
KEEP=0
BUILD=1
BASE_PORT=0
FREEZER_MARGIN=3
FREEZER_INTERVAL="1s"
FREEZER_BATCH=256
BUILD_COLD_FREEZER=0
SYNC_MAX_DIFF=2

# Fixed dev witness key also used by scripts/system_test.sh.
WITNESS_KEY="c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4"

PIDS=()

usage() {
  cat <<'EOF'
Usage: scripts/dev/storage_benchmark.sh [options]

Profiles:
  producer    Start one dev witness per mode and measure time to target block.
  sync        Start a dev witness and a fresh follower per mode; measure catch-up.

Options:
  --profile producer|sync        Benchmark profile (default: producer)
  --modes full,minimal,archive   Comma-separated prune modes
  --target-blocks N              Block height target (default: 30)
  --timeout SECONDS              Per-wait timeout (default: 180)
  --workdir DIR                  Working directory (default: mktemp)
  --output FILE                  JSONL output path (default: WORKDIR/results.jsonl)
  --gtron PATH                   gtron binary (default: build/bin/gtron)
  --no-build                     Do not build gtron if missing
  --keep                         Keep workdir after exit
  --base-port N                  First port to allocate (default: pid-derived)
  --freezer-margin N             Blocks kept hot behind solid line (default: 3)
  --freezer-interval DURATION    Freezer pass interval (default: 1s)
  --freezer-batch N              Max blocks frozen per pass (default: 256)
  --build-cold-freezer           After producer run, build chain-freezer snapshot
  --sync-max-diff N              Sync profile success threshold (default: 2)

Examples:
  scripts/dev/storage_benchmark.sh --modes full,minimal,archive --target-blocks 80
  scripts/dev/storage_benchmark.sh --profile sync --modes full,minimal --target-blocks 100
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --profile) PROFILE="${2:?}"; shift 2 ;;
    --modes) MODES="${2:?}"; shift 2 ;;
    --target-blocks) TARGET_BLOCKS="${2:?}"; shift 2 ;;
    --timeout) TIMEOUT="${2:?}"; shift 2 ;;
    --workdir) WORKDIR="${2:?}"; shift 2 ;;
    --output) OUTPUT="${2:?}"; shift 2 ;;
    --gtron) GTRON="${2:?}"; shift 2 ;;
    --no-build) BUILD=0; shift ;;
    --keep) KEEP=1; shift ;;
    --base-port) BASE_PORT="${2:?}"; shift 2 ;;
    --freezer-margin) FREEZER_MARGIN="${2:?}"; shift 2 ;;
    --freezer-interval) FREEZER_INTERVAL="${2:?}"; shift 2 ;;
    --freezer-batch) FREEZER_BATCH="${2:?}"; shift 2 ;;
    --build-cold-freezer) BUILD_COLD_FREEZER=1; shift ;;
    --sync-max-diff) SYNC_MAX_DIFF="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

case "$PROFILE" in
  producer|sync) ;;
  *) die "unknown profile $PROFILE" ;;
esac

if [ -z "$WORKDIR" ]; then
  WORKDIR="$(mktemp -d)"
else
  mkdir -p "$WORKDIR"
fi
if [ -z "$OUTPUT" ]; then
  OUTPUT="$WORKDIR/results.jsonl"
fi
if [ "$BASE_PORT" -eq 0 ]; then
  BASE_PORT=$((20000 + ($$ % 20000)))
fi

cleanup() {
  for pid in "${PIDS[@]:-}"; do
    if kill "$pid" >/dev/null 2>&1; then
      wait "$pid" >/dev/null 2>&1 || true
    fi
  done
  if [ "$KEEP" -eq 0 ]; then
    rm -rf "$WORKDIR"
  else
    echo "kept workdir: $WORKDIR"
  fi
}
trap cleanup EXIT

build_gtron() {
  if [ "$BUILD" -eq 1 ] && [ ! -x "$GTRON" ]; then
    echo "building gtron -> $GTRON"
    (cd "$BASEDIR" && go build -o "$GTRON" ./cmd/gtron/)
  fi
  [ -x "$GTRON" ] || die "gtron binary not executable: $GTRON"
}

http_get() {
  local port="$1"
  local path="$2"
  curl -sf --max-time 5 "http://127.0.0.1:$port$path"
}

json_field() {
  python3 -c "import sys,json; d=json.load(sys.stdin); print($1)" 2>/dev/null
}

block_num() {
  local port="$1"
  local out
  out="$(http_get "$port" /wallet/getnowblock 2>/dev/null || true)"
  if [ -z "$out" ]; then
    echo 0
    return
  fi
  json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" <<<"$out" || echo 0
}

wait_for_http() {
  local port="$1"
  local name="$2"
  local deadline=$((SECONDS + TIMEOUT))
  until http_get "$port" /wallet/getnodeinfo >/dev/null 2>&1; do
    if [ "$SECONDS" -ge "$deadline" ]; then
      die "$name HTTP not ready on port $port after ${TIMEOUT}s"
    fi
    sleep 1
  done
}

wait_for_block() {
  local port="$1"
  local target="$2"
  local name="$3"
  local deadline=$((SECONDS + TIMEOUT))
  local num=0
  while true; do
    num="$(block_num "$port")"
    if [ "$num" -ge "$target" ] 2>/dev/null; then
      echo "$num"
      return
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      die "$name reached block $num, want >= $target after ${TIMEOUT}s"
    fi
    sleep 1
  done
}

wait_for_sync_close() {
  local sr_port="$1"
  local node_port="$2"
  local deadline=$((SECONDS + TIMEOUT))
  local sr_num=0 node_num=0 diff=0
  while true; do
    sr_num="$(block_num "$sr_port")"
    node_num="$(block_num "$node_port")"
    diff=$((sr_num - node_num))
    if [ "$diff" -lt 0 ]; then diff=$((-diff)); fi
    if [ "$sr_num" -gt 0 ] && [ "$node_num" -gt 0 ] && [ "$diff" -le "$SYNC_MAX_DIFF" ]; then
      echo "$node_num"
      return
    fi
    if [ "$SECONDS" -ge "$deadline" ]; then
      die "follower sync diff $diff (sr=$sr_num follower=$node_num), want <= $SYNC_MAX_DIFF"
    fi
    sleep 1
  done
}

size_bytes() {
  local path="$1"
  if [ ! -e "$path" ]; then
    echo 0
    return
  fi
  du -sk "$path" | awk '{print $1 * 1024}'
}

file_count() {
  local path="$1"
  if [ ! -e "$path" ]; then
    echo 0
    return
  fi
  find "$path" -type f | wc -l | tr -d ' '
}

start_node() {
  local name="$1"
  local mode="$2"
  local datadir="$3"
  local http="$4"
  local p2p="$5"
  local jrpc="$6"
  local witness="$7"
  local seednode="$8"
  local log_path="$9"
  local args=(
    --dev
    --witness.key "$WITNESS_KEY"
    --datadir "$datadir"
    --p2p.port "$p2p"
    --http.port "$http"
    --jsonrpc.port "$jrpc"
    --grpc.port 0
    --discover.port 0
    --prune.mode "$mode"
    --freezer.interval "$FREEZER_INTERVAL"
    --freezer.margin "$FREEZER_MARGIN"
    --freezer.batch "$FREEZER_BATCH"
  )
  if [ "$witness" = "1" ]; then
    args+=(--witness)
  fi
  if [ -n "$seednode" ]; then
    args+=(--seednode "$seednode")
  fi
  "$GTRON" "${args[@]}" >"$log_path" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  echo "$pid"
  wait_for_http "$http" "$name"
}

stop_pid() {
  local pid="$1"
  if kill "$pid" >/dev/null 2>&1; then
    wait "$pid" >/dev/null 2>&1 || true
  fi
}

maybe_build_cold_freezer() {
  local datadir="$1"
  local height="$2"
  local log_path="$3"
  if [ "$BUILD_COLD_FREEZER" -ne 1 ]; then
    return
  fi
  if [ "$height" -le "$FREEZER_MARGIN" ]; then
    echo "skip cold freezer build: height $height <= freezer margin $FREEZER_MARGIN" >>"$log_path"
    return
  fi
  local to_block=$((height - FREEZER_MARGIN - 1))
  if [ "$to_block" -lt 0 ]; then
    return
  fi
  echo "building cold chain-freezer snapshot through block $to_block" >>"$log_path"
  "$GTRON" snapshot build-freezer \
    --datadir "$datadir" \
    --snapshot.from-block 0 \
    --snapshot.to-block "$to_block" \
    >>"$log_path" 2>&1 || echo "warning: snapshot build-freezer failed; see $log_path" >&2
}

emit_result() {
  local profile="$1"
  local mode="$2"
  local role="$3"
  local status="$4"
  local target="$5"
  local height="$6"
  local elapsed="$7"
  local datadir="$8"
  local log_path="$9"
  local total chain ancient snapshots ancient_files snapshot_files
  total="$(size_bytes "$datadir")"
  chain="$(size_bytes "$datadir/gtron/chaindata")"
  ancient="$(size_bytes "$datadir/gtron/ancient")"
  snapshots="$(size_bytes "$datadir/gtron/state-snapshots")"
  ancient_files="$(file_count "$datadir/gtron/ancient")"
  snapshot_files="$(file_count "$datadir/gtron/state-snapshots")"
  python3 - "$OUTPUT" "$profile" "$mode" "$role" "$status" "$target" "$height" "$elapsed" \
    "$total" "$chain" "$ancient" "$snapshots" "$ancient_files" "$snapshot_files" "$datadir" "$log_path" <<'PY'
import json, sys, time
out = sys.argv[1]
keys = [
    "profile", "mode", "role", "status", "targetBlock", "height", "elapsedSeconds",
    "datadirBytes", "chaindataBytes", "ancientBytes", "snapshotBytes",
    "ancientFiles", "snapshotFiles", "datadir", "log",
]
values = sys.argv[2:]
ints = {"targetBlock", "height", "elapsedSeconds", "datadirBytes", "chaindataBytes", "ancientBytes", "snapshotBytes", "ancientFiles", "snapshotFiles"}
row = {"unix": int(time.time())}
for key, value in zip(keys, values):
    row[key] = int(value) if key in ints else value
line = json.dumps(row, sort_keys=True)
with open(out, "a", encoding="utf-8") as fh:
    fh.write(line + "\n")
print(line)
PY
}

run_producer_mode() {
  local mode="$1"
  local idx="$2"
  local port_base=$((BASE_PORT + idx * 20))
  local datadir="$WORKDIR/$mode-producer"
  local log_path="$WORKDIR/$mode-producer.log"
  mkdir -p "$datadir"
  local start=$SECONDS
  local pid
  pid="$(start_node "$mode producer" "$mode" "$datadir" "$((port_base + 1))" "$((port_base + 2))" "$((port_base + 3))" 1 "" "$log_path")"
  local height
  height="$(wait_for_block "$((port_base + 1))" "$TARGET_BLOCKS" "$mode producer")"
  local elapsed=$((SECONDS - start))
  stop_pid "$pid"
  maybe_build_cold_freezer "$datadir" "$height" "$log_path"
  emit_result "$PROFILE" "$mode" "producer" "ok" "$TARGET_BLOCKS" "$height" "$elapsed" "$datadir" "$log_path"
}

run_sync_mode() {
  local mode="$1"
  local idx="$2"
  local port_base=$((BASE_PORT + idx * 30))
  local sr_dir="$WORKDIR/$mode-sync-sr"
  local node_dir="$WORKDIR/$mode-sync-follower"
  local sr_log="$WORKDIR/$mode-sync-sr.log"
  local node_log="$WORKDIR/$mode-sync-follower.log"
  mkdir -p "$sr_dir" "$node_dir"

  local sr_pid
  sr_pid="$(start_node "$mode sync sr" "full" "$sr_dir" "$((port_base + 1))" "$((port_base + 2))" "$((port_base + 3))" 1 "" "$sr_log")"
  wait_for_block "$((port_base + 1))" "$TARGET_BLOCKS" "$mode sync sr" >/dev/null

  local start=$SECONDS
  local node_pid
  node_pid="$(start_node "$mode sync follower" "$mode" "$node_dir" "$((port_base + 11))" "$((port_base + 12))" "$((port_base + 13))" 0 "127.0.0.1:$((port_base + 2))" "$node_log")"
  local height
  height="$(wait_for_sync_close "$((port_base + 1))" "$((port_base + 11))")"
  local elapsed=$((SECONDS - start))
  stop_pid "$node_pid"
  stop_pid "$sr_pid"
  emit_result "$PROFILE" "$mode" "sync-follower" "ok" "$TARGET_BLOCKS" "$height" "$elapsed" "$node_dir" "$node_log"
}

build_gtron
: >"$OUTPUT"
echo "workdir: $WORKDIR"
echo "output:  $OUTPUT"

IFS=',' read -r -a MODE_ARRAY <<<"$MODES"
idx=0
for mode in "${MODE_ARRAY[@]}"; do
  mode="$(echo "$mode" | tr '[:upper:]' '[:lower:]' | xargs)"
  case "$mode" in
    full|blocks|minimal|snap|archive) ;;
    *) die "unsupported mode $mode" ;;
  esac
  echo "=== $PROFILE benchmark: mode=$mode ==="
  if [ "$PROFILE" = "producer" ]; then
    run_producer_mode "$mode" "$idx"
  else
    run_sync_mode "$mode" "$idx"
  fi
  idx=$((idx + 1))
done

echo "benchmark complete: $OUTPUT"
