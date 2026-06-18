#!/usr/bin/env bash
#
# Compare go-tron storage/sync behaviour across Erigon-style prune modes.
#
# The script is intentionally a harness, not a pass/fail test: it emits one
# JSON object per run so repeated samples can be graphed by mode.
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/../.." && pwd)"
GTRON="${GTRON:-$BASEDIR/build/bin/gtron}"
MODES="full,blocks,minimal,archive"
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
BUILD_DERIVED_INDEXES=0
SIGNED_COLD_PRUNE=0
SNAPSHOT_SIGNING_SEED="1111111111111111111111111111111111111111111111111111111111111111"
SYNC_MAX_DIFF=2
HISTORY_WINDOW=0

# Fixed dev witness key also used by scripts/system_test.sh.
WITNESS_KEY="c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4"

PIDS=()
STARTED_PID=""
RUN_COLD_FREEZER_TO_BLOCK=-1
RUN_DERIVED_INDEX_TO_BLOCK=-1
RUN_DERIVED_INDEX_SEGMENTS=0
RUN_DERIVED_INDEX_BUILD_SECONDS=0
RUN_EVENT_LOG_INDEX_SEGMENTS=0
RUN_EVENT_LOG_INDEX_ADDRESS_KEYS=0
RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS=0
RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS=0
RUN_EVENT_LOG_INDEX_TOPIC_KEYS=0
RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS=0
RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS=0
RUN_BALANCE_TRACE_PRUNE_TO_BLOCK=-1
RUN_BALANCE_TRACE_BLOCK_ROWS=0
RUN_BALANCE_TRACE_ACCOUNT_ROWS=0
RUN_SECTION_BLOOM_PRUNE_TO_SECTION=-1
RUN_SECTION_BLOOM_ROWS=0
RUN_SIGNED_COLD_PRUNE=0
RUN_CHAIN_LOOKUP_PRUNE_TO_BLOCK=-1
RUN_CHAIN_LOOKUP_BLOCK_INDEXES=0
RUN_CHAIN_LOOKUP_TX_INDEXES=0
RUN_RETIRED_PRUNE_SEGMENTS=0
RUN_RETIRED_PRUNE_DELETED=0
RUN_RETIRED_PRUNE_MISSING=0
RUN_RETIRED_PRUNE_SKIPPED_ACTIVE=0
RUN_RETIRED_PRUNE_BYTES_DELETED=0
RUN_TAIL_PRUNED_THROUGH_BLOCK=-1
RUN_TAIL_PRUNED_FILES=0
RUN_FREEZER_ALERT_STATUS="not-run"
RUN_FREEZER_ALERT_ISSUES=-1
RUN_FREEZER_ALERT_HIDDEN_BYTES=-1
RUN_FREEZER_ALERT_DETAILS="[]"
RUN_STAGE_VERIFY_STATUS="not-run"
RUN_STAGE_VERIFY_ISSUES=-1
RUN_STAGE_VERIFY_DETAILS="[]"
RUN_SNAPSHOT_ALERT_STATUS="not-run"
RUN_SNAPSHOT_ALERT_ISSUES=-1
RUN_SNAPSHOT_ALERT_DETAILS="[]"
RUN_SNAPSHOT_RETIRED_SEGMENTS=-1
RUN_SNAPSHOT_RETIRED_FILES=-1
RUN_SNAPSHOT_RETIRED_MISSING=-1
RUN_SNAPSHOT_RETIRED_SKIPPED_ACTIVE=-1
RUN_SNAPSHOT_RETIRED_BYTES=-1
RUN_STORAGE_ALERT_FAILED=0

usage() {
  cat <<'EOF'
Usage: scripts/dev/storage_benchmark.sh [options]

Profiles:
  producer    Start one dev witness per mode and measure time to target block.
  sync        Start a dev witness and a fresh follower per mode; measure catch-up.

Options:
  --profile producer|sync        Benchmark profile (default: producer)
  --modes full,blocks,minimal,archive
                                  Comma-separated prune modes
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
  --build-derived-indexes        After producer run, build cold trace/bloom/log sidecars
  --signed-cold-prune            Build freezer snapshot, sign catalog, prune hot lookups
  --snapshot-signing-seed HEX    Ed25519 seed/private key for signed-cold-prune
  --sync-max-diff N              Sync profile success threshold (default: 2)
  --history-window N             Inject [history] prune_window for short prune drills

Examples:
  scripts/dev/storage_benchmark.sh --modes full,blocks,minimal,archive --target-blocks 80
  scripts/dev/storage_benchmark.sh --profile sync --modes full,blocks,minimal --target-blocks 100
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
    --build-derived-indexes) BUILD_DERIVED_INDEXES=1; shift ;;
    --signed-cold-prune) SIGNED_COLD_PRUNE=1; BUILD_COLD_FREEZER=1; shift ;;
    --snapshot-signing-seed) SNAPSHOT_SIGNING_SEED="${2:?}"; shift 2 ;;
    --sync-max-diff) SYNC_MAX_DIFF="${2:?}"; shift 2 ;;
    --history-window) HISTORY_WINDOW="${2:?}"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

case "$PROFILE" in
  producer|sync) ;;
  *) die "unknown profile $PROFILE" ;;
esac
if [ "$SIGNED_COLD_PRUNE" -eq 1 ] && [ "$PROFILE" != "producer" ]; then
  die "--signed-cold-prune is only supported with --profile producer"
fi
if [ "$BUILD_DERIVED_INDEXES" -eq 1 ] && [ "$PROFILE" != "producer" ]; then
  die "--build-derived-indexes is only supported with --profile producer"
fi

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

reset_run_metrics() {
  RUN_COLD_FREEZER_TO_BLOCK=-1
  RUN_DERIVED_INDEX_TO_BLOCK=-1
  RUN_DERIVED_INDEX_SEGMENTS=0
  RUN_DERIVED_INDEX_BUILD_SECONDS=0
  RUN_EVENT_LOG_INDEX_SEGMENTS=0
  RUN_EVENT_LOG_INDEX_ADDRESS_KEYS=0
  RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS=0
  RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS=0
  RUN_EVENT_LOG_INDEX_TOPIC_KEYS=0
  RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS=0
  RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS=0
  RUN_BALANCE_TRACE_PRUNE_TO_BLOCK=-1
  RUN_BALANCE_TRACE_BLOCK_ROWS=0
  RUN_BALANCE_TRACE_ACCOUNT_ROWS=0
  RUN_SECTION_BLOOM_PRUNE_TO_SECTION=-1
  RUN_SECTION_BLOOM_ROWS=0
  RUN_SIGNED_COLD_PRUNE=0
  RUN_CHAIN_LOOKUP_PRUNE_TO_BLOCK=-1
  RUN_CHAIN_LOOKUP_BLOCK_INDEXES=0
  RUN_CHAIN_LOOKUP_TX_INDEXES=0
  RUN_RETIRED_PRUNE_SEGMENTS=0
  RUN_RETIRED_PRUNE_DELETED=0
  RUN_RETIRED_PRUNE_MISSING=0
  RUN_RETIRED_PRUNE_SKIPPED_ACTIVE=0
  RUN_RETIRED_PRUNE_BYTES_DELETED=0
  RUN_TAIL_PRUNED_THROUGH_BLOCK=-1
  RUN_TAIL_PRUNED_FILES=0
  RUN_FREEZER_ALERT_STATUS="not-run"
  RUN_FREEZER_ALERT_ISSUES=-1
  RUN_FREEZER_ALERT_HIDDEN_BYTES=-1
  RUN_FREEZER_ALERT_DETAILS="[]"
  RUN_STAGE_VERIFY_STATUS="not-run"
  RUN_STAGE_VERIFY_ISSUES=-1
  RUN_STAGE_VERIFY_DETAILS="[]"
  RUN_SNAPSHOT_ALERT_STATUS="not-run"
  RUN_SNAPSHOT_ALERT_ISSUES=-1
  RUN_SNAPSHOT_ALERT_DETAILS="[]"
  RUN_SNAPSHOT_RETIRED_SEGMENTS=-1
  RUN_SNAPSHOT_RETIRED_FILES=-1
  RUN_SNAPSHOT_RETIRED_MISSING=-1
  RUN_SNAPSHOT_RETIRED_SKIPPED_ACTIVE=-1
  RUN_SNAPSHOT_RETIRED_BYTES=-1
  RUN_STORAGE_ALERT_FAILED=0
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

history_config_arg() {
  local datadir="$1"
  if [ "$HISTORY_WINDOW" -le 0 ]; then
    return
  fi
  local cfg="$datadir/history-benchmark.toml"
  mkdir -p "$datadir"
  cat >"$cfg" <<EOF
[history]
prune_window = $HISTORY_WINDOW
EOF
  printf '%s\n' "$cfg"
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
  local history_cfg
  history_cfg="$(history_config_arg "$datadir")"
  if [ -n "$history_cfg" ]; then
    args+=(--config "$history_cfg")
  fi
  if [ "$BUILD_DERIVED_INDEXES" -eq 1 ]; then
    args+=(--history.enabled)
  fi
  if [ "$witness" = "1" ]; then
    args+=(--witness)
  fi
  if [ -n "$seednode" ]; then
    args+=(--seednode "$seednode")
  fi
  "$GTRON" "${args[@]}" >"$log_path" 2>&1 &
  local pid=$!
  PIDS+=("$pid")
  STARTED_PID="$pid"
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
    if [ "$SIGNED_COLD_PRUNE" -eq 1 ]; then
      die "signed cold prune requires height > freezer margin"
    fi
    return
  fi
  local to_block=$((height - FREEZER_MARGIN - 1))
  if [ "$to_block" -lt 0 ]; then
    return
  fi
  RUN_COLD_FREEZER_TO_BLOCK="$to_block"
  echo "building cold chain-freezer snapshot through block $to_block" >>"$log_path"
  "$GTRON" snapshot build-freezer \
    --dev \
    --witness.key "$WITNESS_KEY" \
    --datadir "$datadir" \
    --snapshot.from-block 0 \
    --snapshot.to-block "$to_block" \
    >>"$log_path" 2>&1 || {
      if [ "$SIGNED_COLD_PRUNE" -eq 1 ]; then
        die "snapshot build-freezer failed; see $log_path"
      fi
      echo "warning: snapshot build-freezer failed; see $log_path" >&2
    }
}

maybe_build_derived_indexes() {
  local datadir="$1"
  local height="$2"
  local log_path="$3"
  if [ "$BUILD_DERIVED_INDEXES" -ne 1 ]; then
    return
  fi
  if [ "$height" -le "$FREEZER_MARGIN" ]; then
    die "derived index snapshot build requires height > freezer margin"
  fi
  local to_block=$((height - FREEZER_MARGIN - 1))
  if [ "$to_block" -lt 1 ]; then
    die "derived index snapshot build requires at least one post-genesis block before the freezer margin"
  fi
  RUN_DERIVED_INDEX_TO_BLOCK="$to_block"
  echo "building cold derived-index snapshots through block $to_block" >>"$log_path"
  local build_start=$SECONDS
  local derived_out="$WORKDIR/derived-indexes-$(basename "$datadir").out"
  if ! run_logged "$derived_out" "$GTRON" snapshot build-derived-indexes \
    --dev \
    --witness.key "$WITNESS_KEY" \
    --datadir "$datadir" \
    --snapshot.from-block 1 \
    --snapshot.to-block "$to_block" >>"$log_path"; then
    die "snapshot build-derived-indexes failed; see $log_path"
  fi
  RUN_DERIVED_INDEX_BUILD_SECONDS=$((SECONDS - build_start))
  local active_segments
  active_segments="$(sed -n 's/.*activeSegments=\([0-9][0-9]*\).*/\1/p' "$derived_out" | tail -1)"
  RUN_DERIVED_INDEX_SEGMENTS="${active_segments:-0}"
}

collect_event_log_index_stats() {
  local datadir="$1"
  local log_path="$2"
  if [ "$BUILD_DERIVED_INDEXES" -ne 1 ]; then
    return
  fi
  local stats_out="$WORKDIR/event-log-index-stats-$(basename "$datadir").out"
  echo "collecting event-log index stats" >>"$log_path"
  if ! run_logged "$stats_out" "$GTRON" snapshot event-log-index-stats --datadir "$datadir" >>"$log_path"; then
    die "snapshot event-log-index-stats failed; see $log_path"
  fi
  local segments address_keys address_postings address_max topic_keys topic_postings topic_max
  segments="$(sed -n 's/^Event log index stats:.* segments=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_keys="$(sed -n 's/^Event log index stats:.* addressKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_postings="$(sed -n 's/^Event log index stats:.* addressPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_max="$(sed -n 's/^Event log index stats:.* addressMaxPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_keys="$(sed -n 's/^Event log index stats:.* topicKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_postings="$(sed -n 's/^Event log index stats:.* topicPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_max="$(sed -n 's/^Event log index stats:.* topicMaxPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  RUN_EVENT_LOG_INDEX_SEGMENTS="${segments:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_KEYS="${address_keys:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS="${address_postings:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS="${address_max:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_KEYS="${topic_keys:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS="${topic_postings:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS="${topic_max:-0}"
}

run_logged() {
  local out="$1"
  shift
  if "$@" >"$out" 2>&1; then
    cat "$out"
    return 0
  fi
  cat "$out"
  return 1
}

storage_alert_detail_json() {
  local alert_out="$1"
  python3 - "$alert_out" <<'PY'
import json
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
freezer = []
stage = []
snapshot = []
for line in text.splitlines():
    m = re.match(r"Storage freezer alert: severity=([^ ]+) kind=([^ ]+) detail=(.*)$", line)
    if m:
        freezer.append({"severity": m.group(1), "kind": m.group(2), "detail": m.group(3)})
        continue
    m = re.match(r"Storage stage alert: severity=([^ ]+) detail=(.*)$", line)
    if m:
        stage.append({"severity": m.group(1), "detail": m.group(2)})
        continue
    m = re.match(r"Storage snapshot alert: severity=([^ ]+) kind=([^ ]+) detail=(.*)$", line)
    if m:
        snapshot.append({"severity": m.group(1), "kind": m.group(2), "detail": m.group(3)})
print(json.dumps(freezer, separators=(",", ":")))
print(json.dumps(stage, separators=(",", ":")))
print(json.dumps(snapshot, separators=(",", ":")))
PY
}

run_storage_alert_gate() {
  local mode="$1"
  local role="$2"
  local datadir="$3"
  local log_path="$4"
  local alert_out="$WORKDIR/$mode-$role-storage-alerts.out"
  echo "checking persisted storage alert conditions" >>"$log_path"
  local ok=1
  if ! run_logged "$alert_out" "$GTRON" db storage-alerts --datadir "$datadir" >>"$log_path"; then
    ok=0
  fi
  local freezer_status freezer_issues hidden stage_status stage_issues snapshot_status snapshot_issues retired_segments retired_files retired_missing retired_skipped retired_bytes
  freezer_status="$(sed -n 's/.* freezerStatus=\([^ ]*\).*/\1/p' "$alert_out" | tail -1)"
  freezer_issues="$(sed -n 's/.* freezerIssues=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  hidden="$(sed -n 's/.* hiddenSize=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  stage_status="$(sed -n 's/.* stageStatus=\([^ ]*\).*/\1/p' "$alert_out" | tail -1)"
  stage_issues="$(sed -n 's/.* stageIssues=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  snapshot_status="$(sed -n 's/.* snapshotStatus=\([^ ]*\).*/\1/p' "$alert_out" | tail -1)"
  snapshot_issues="$(sed -n 's/.* snapshotIssues=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  retired_segments="$(sed -n 's/.* retiredSegments=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  retired_files="$(sed -n 's/.* retiredFiles=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  retired_missing="$(sed -n 's/.* retiredMissing=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  retired_skipped="$(sed -n 's/.* retiredSkippedActive=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  retired_bytes="$(sed -n 's/.* retiredBytes=\([0-9][0-9]*\).*/\1/p' "$alert_out" | tail -1)"
  RUN_FREEZER_ALERT_STATUS="${freezer_status:-unknown}"
  RUN_FREEZER_ALERT_ISSUES="${freezer_issues:--1}"
  RUN_FREEZER_ALERT_HIDDEN_BYTES="${hidden:--1}"
  RUN_STAGE_VERIFY_STATUS="${stage_status:-unknown}"
  RUN_STAGE_VERIFY_ISSUES="${stage_issues:--1}"
  RUN_SNAPSHOT_ALERT_STATUS="${snapshot_status:-unknown}"
  RUN_SNAPSHOT_ALERT_ISSUES="${snapshot_issues:--1}"
  RUN_SNAPSHOT_RETIRED_SEGMENTS="${retired_segments:--1}"
  RUN_SNAPSHOT_RETIRED_FILES="${retired_files:--1}"
  RUN_SNAPSHOT_RETIRED_MISSING="${retired_missing:--1}"
  RUN_SNAPSHOT_RETIRED_SKIPPED_ACTIVE="${retired_skipped:--1}"
  RUN_SNAPSHOT_RETIRED_BYTES="${retired_bytes:--1}"
  local detail_json
  detail_json="$(storage_alert_detail_json "$alert_out")"
  RUN_FREEZER_ALERT_DETAILS="$(printf '%s\n' "$detail_json" | sed -n '1p')"
  RUN_STAGE_VERIFY_DETAILS="$(printf '%s\n' "$detail_json" | sed -n '2p')"
  RUN_SNAPSHOT_ALERT_DETAILS="$(printf '%s\n' "$detail_json" | sed -n '3p')"
  if [ "$ok" -ne 1 ]; then
    RUN_STORAGE_ALERT_FAILED=1
  fi
}

run_signed_cold_prune_drill() {
  local mode="$1"
  local idx="$2"
  local datadir="$3"
  local log_path="$4"
  if [ "$SIGNED_COLD_PRUNE" -ne 1 ]; then
    return
  fi
  if [ "$RUN_COLD_FREEZER_TO_BLOCK" -lt 0 ]; then
    die "signed cold prune requested but no cold freezer snapshot was built"
  fi
  RUN_SIGNED_COLD_PRUNE=1

  local publish_out="$WORKDIR/$mode-publish-catalog.out"
  echo "publishing signed snapshot catalog" >>"$log_path"
  if ! run_logged "$publish_out" "$GTRON" snapshot publish-catalog \
    --datadir "$datadir" \
    --snapshot.signing-key "$SNAPSHOT_SIGNING_SEED" >>"$log_path"; then
    die "snapshot publish-catalog failed; see $log_path"
  fi
  local signer
  signer="$(sed -n 's/.*signer=\([0-9a-fA-F]*\).*/\1/p' "$publish_out" | tail -1)"
  [ -n "$signer" ] || die "could not parse snapshot catalog signer from $publish_out"

  local prune_out="$WORKDIR/$mode-prune-chain-lookups.out"
  echo "pruning hot chain lookup rows using signed catalog signer $signer" >>"$log_path"
  if ! run_logged "$prune_out" "$GTRON" snapshot prune-chain-lookups \
    --dev \
    --witness.key "$WITNESS_KEY" \
    --datadir "$datadir" \
    --snapshot.trusted-key "$signer" >>"$log_path"; then
    die "snapshot prune-chain-lookups failed; see $log_path"
  fi
  local range_to block_indexes tx_indexes
  range_to="$(sed -n 's/.*range=\[[0-9][0-9]*,\([0-9][0-9]*\)\].*/\1/p' "$prune_out" | tail -1)"
  block_indexes="$(sed -n 's/.*blockIndexes=\([0-9][0-9]*\).*/\1/p' "$prune_out" | tail -1)"
  tx_indexes="$(sed -n 's/.*txIndexes=\([0-9][0-9]*\).*/\1/p' "$prune_out" | tail -1)"
  if [ -n "$range_to" ]; then
    RUN_CHAIN_LOOKUP_PRUNE_TO_BLOCK="$range_to"
  fi
  RUN_CHAIN_LOOKUP_BLOCK_INDEXES="${block_indexes:-0}"
  RUN_CHAIN_LOOKUP_TX_INDEXES="${tx_indexes:-0}"

  if [ "$BUILD_DERIVED_INDEXES" -eq 1 ]; then
    local balance_out="$WORKDIR/$mode-prune-balance-traces.out"
    echo "pruning hot balance trace rows using signed catalog signer $signer" >>"$log_path"
    if ! run_logged "$balance_out" "$GTRON" snapshot prune-balance-traces \
      --dev \
      --witness.key "$WITNESS_KEY" \
      --datadir "$datadir" \
      --snapshot.trusted-key "$signer" >>"$log_path"; then
      die "snapshot prune-balance-traces failed; see $log_path"
    fi
    local balance_range_to balance_block_rows balance_account_rows
    balance_range_to="$(sed -n 's/.*range=\[[0-9][0-9]*,\([0-9][0-9]*\)\].*/\1/p' "$balance_out" | tail -1)"
    balance_block_rows="$(sed -n 's/.*blockTraces=\([0-9][0-9]*\).*/\1/p' "$balance_out" | tail -1)"
    balance_account_rows="$(sed -n 's/.*accountTraces=\([0-9][0-9]*\).*/\1/p' "$balance_out" | tail -1)"
    if [ -n "$balance_range_to" ]; then
      RUN_BALANCE_TRACE_PRUNE_TO_BLOCK="$balance_range_to"
    fi
    RUN_BALANCE_TRACE_BLOCK_ROWS="${balance_block_rows:-0}"
    RUN_BALANCE_TRACE_ACCOUNT_ROWS="${balance_account_rows:-0}"

    local bloom_out="$WORKDIR/$mode-prune-section-blooms.out"
    echo "pruning hot section bloom rows using signed catalog signer $signer" >>"$log_path"
    if ! run_logged "$bloom_out" "$GTRON" snapshot prune-section-blooms \
      --dev \
      --witness.key "$WITNESS_KEY" \
      --datadir "$datadir" \
      --snapshot.trusted-key "$signer" >>"$log_path"; then
      die "snapshot prune-section-blooms failed; see $log_path"
    fi
    local bloom_range_to bloom_rows
    bloom_range_to="$(sed -n 's/.*sections=\[[0-9][0-9]*,\([0-9][0-9]*\)\].*/\1/p' "$bloom_out" | tail -1)"
    bloom_rows="$(sed -n 's/.*rows=\([0-9][0-9]*\).*/\1/p' "$bloom_out" | tail -1)"
    if [ -n "$bloom_range_to" ]; then
      RUN_SECTION_BLOOM_PRUNE_TO_SECTION="$bloom_range_to"
    fi
    RUN_SECTION_BLOOM_ROWS="${bloom_rows:-0}"
  fi

  local retired_out="$WORKDIR/$mode-prune-retired.out"
  echo "pruning retired snapshot segment files" >>"$log_path"
  if ! run_logged "$retired_out" "$GTRON" snapshot prune-retired --datadir "$datadir" >>"$log_path"; then
    die "snapshot prune-retired failed; see $log_path"
  fi
  local retired_segments retired_deleted retired_missing retired_skipped retired_bytes_deleted
  retired_segments="$(sed -n 's/.*retired=\([0-9][0-9]*\).*/\1/p' "$retired_out" | tail -1)"
  retired_deleted="$(sed -n 's/.*deleted=\([0-9][0-9]*\).*/\1/p' "$retired_out" | tail -1)"
  retired_missing="$(sed -n 's/.*missing=\([0-9][0-9]*\).*/\1/p' "$retired_out" | tail -1)"
  retired_skipped="$(sed -n 's/.*skippedActive=\([0-9][0-9]*\).*/\1/p' "$retired_out" | tail -1)"
  retired_bytes_deleted="$(sed -n 's/.*bytesDeleted=\([0-9][0-9]*\).*/\1/p' "$retired_out" | tail -1)"
  RUN_RETIRED_PRUNE_SEGMENTS="${retired_segments:-0}"
  RUN_RETIRED_PRUNE_DELETED="${retired_deleted:-0}"
  RUN_RETIRED_PRUNE_MISSING="${retired_missing:-0}"
  RUN_RETIRED_PRUNE_SKIPPED_ACTIVE="${retired_skipped:-0}"
  RUN_RETIRED_PRUNE_BYTES_DELETED="${retired_bytes_deleted:-0}"

  if [ "$mode" = "minimal" ]; then
    local port_base=$((BASE_PORT + idx * 20))
    local restart_log="$WORKDIR/$mode-producer-post-prune-restart.log"
    echo "restarting minimal node once so tail-prune lifecycle can run" >>"$log_path"
    local restart_pid
    start_node "$mode post-prune restart" "$mode" "$datadir" "$((port_base + 11))" "$((port_base + 12))" "$((port_base + 13))" 1 "" "$restart_log"
    restart_pid="$STARTED_PID"
    sleep 3
    stop_pid "$restart_pid"
    cat "$restart_log" >>"$log_path"
    local pruned_through pruned_files
    pruned_through="$(sed -n 's/.*prunedThroughBlock[= ]\([0-9][0-9]*\).*/\1/p' "$restart_log" | tail -1)"
    pruned_files="$(sed -n 's/.*prunedTailFiles[= ]\([0-9][0-9]*\).*/\1/p' "$restart_log" | tail -1)"
    RUN_TAIL_PRUNED_THROUGH_BLOCK="${pruned_through:--1}"
    RUN_TAIL_PRUNED_FILES="${pruned_files:-0}"
  fi
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
    "$total" "$chain" "$ancient" "$snapshots" "$ancient_files" "$snapshot_files" \
    "$RUN_COLD_FREEZER_TO_BLOCK" "$RUN_DERIVED_INDEX_TO_BLOCK" "$RUN_DERIVED_INDEX_SEGMENTS" \
    "$RUN_DERIVED_INDEX_BUILD_SECONDS" "$RUN_EVENT_LOG_INDEX_SEGMENTS" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_KEYS" "$RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS" "$RUN_EVENT_LOG_INDEX_TOPIC_KEYS" \
    "$RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS" "$RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS" \
    "$RUN_BALANCE_TRACE_PRUNE_TO_BLOCK" \
    "$RUN_BALANCE_TRACE_BLOCK_ROWS" "$RUN_BALANCE_TRACE_ACCOUNT_ROWS" \
    "$RUN_SECTION_BLOOM_PRUNE_TO_SECTION" "$RUN_SECTION_BLOOM_ROWS" \
    "$RUN_SIGNED_COLD_PRUNE" "$RUN_CHAIN_LOOKUP_PRUNE_TO_BLOCK" \
    "$RUN_CHAIN_LOOKUP_BLOCK_INDEXES" "$RUN_CHAIN_LOOKUP_TX_INDEXES" \
    "$RUN_RETIRED_PRUNE_SEGMENTS" "$RUN_RETIRED_PRUNE_DELETED" \
    "$RUN_RETIRED_PRUNE_MISSING" "$RUN_RETIRED_PRUNE_SKIPPED_ACTIVE" \
    "$RUN_RETIRED_PRUNE_BYTES_DELETED" \
    "$RUN_TAIL_PRUNED_THROUGH_BLOCK" "$RUN_TAIL_PRUNED_FILES" "$HISTORY_WINDOW" \
    "$RUN_FREEZER_ALERT_STATUS" "$RUN_FREEZER_ALERT_ISSUES" "$RUN_FREEZER_ALERT_HIDDEN_BYTES" \
    "$RUN_FREEZER_ALERT_DETAILS" \
    "$RUN_STAGE_VERIFY_STATUS" "$RUN_STAGE_VERIFY_ISSUES" "$RUN_STAGE_VERIFY_DETAILS" \
    "$RUN_SNAPSHOT_ALERT_STATUS" "$RUN_SNAPSHOT_ALERT_ISSUES" "$RUN_SNAPSHOT_ALERT_DETAILS" \
    "$RUN_SNAPSHOT_RETIRED_SEGMENTS" "$RUN_SNAPSHOT_RETIRED_FILES" \
    "$RUN_SNAPSHOT_RETIRED_MISSING" "$RUN_SNAPSHOT_RETIRED_SKIPPED_ACTIVE" \
    "$RUN_SNAPSHOT_RETIRED_BYTES" \
    "$datadir" "$log_path" <<'PY'
import json, sys, time
out = sys.argv[1]
keys = [
    "profile", "mode", "role", "status", "targetBlock", "height", "elapsedSeconds",
    "datadirBytes", "chaindataBytes", "ancientBytes", "snapshotBytes",
    "ancientFiles", "snapshotFiles",
    "coldFreezerToBlock", "derivedIndexToBlock", "derivedIndexSegments",
    "derivedIndexBuildSeconds", "eventLogIndexSegments", "eventLogIndexAddressKeys",
    "eventLogIndexAddressPostings", "eventLogIndexAddressMaxPostings",
    "eventLogIndexTopicKeys", "eventLogIndexTopicPostings",
    "eventLogIndexTopicMaxPostings", "balanceTracePruneToBlock",
    "balanceTraceBlockRowsPruned", "balanceTraceAccountRowsPruned",
    "sectionBloomPruneToSection", "sectionBloomRowsPruned",
    "signedColdPrune", "chainLookupPruneToBlock",
    "chainLookupBlockIndexes", "chainLookupTxIndexes",
    "retiredPruneSegments", "retiredPruneDeleted", "retiredPruneMissing",
    "retiredPruneSkippedActive", "retiredPruneBytesDeleted",
    "tailPrunedThroughBlock", "tailPrunedFiles", "historyWindow",
    "freezerAlertStatus", "freezerAlertIssues", "freezerAlertHiddenBytes",
    "freezerAlertDetails", "stageVerifyStatus", "stageVerifyIssues",
    "stageVerifyDetails", "snapshotAlertStatus", "snapshotAlertIssues",
    "snapshotAlertDetails", "snapshotRetiredSegments",
    "snapshotRetiredFiles", "snapshotRetiredMissing", "snapshotRetiredSkippedActive",
    "snapshotRetiredBytes",
    "datadir", "log",
]
values = sys.argv[2:]
ints = {
    "targetBlock", "height", "elapsedSeconds",
    "datadirBytes", "chaindataBytes", "ancientBytes", "snapshotBytes",
    "ancientFiles", "snapshotFiles", "coldFreezerToBlock", "derivedIndexToBlock",
    "derivedIndexSegments", "derivedIndexBuildSeconds", "balanceTracePruneToBlock",
    "eventLogIndexSegments", "eventLogIndexAddressKeys", "eventLogIndexAddressPostings",
    "eventLogIndexAddressMaxPostings", "eventLogIndexTopicKeys",
    "eventLogIndexTopicPostings", "eventLogIndexTopicMaxPostings",
    "balanceTraceBlockRowsPruned", "balanceTraceAccountRowsPruned",
    "sectionBloomPruneToSection", "sectionBloomRowsPruned", "signedColdPrune",
    "chainLookupPruneToBlock", "chainLookupBlockIndexes", "chainLookupTxIndexes",
    "retiredPruneSegments", "retiredPruneDeleted", "retiredPruneMissing",
    "retiredPruneSkippedActive", "retiredPruneBytesDeleted",
    "tailPrunedThroughBlock", "tailPrunedFiles", "historyWindow",
    "freezerAlertIssues", "freezerAlertHiddenBytes",
    "stageVerifyIssues",
    "snapshotAlertIssues", "snapshotRetiredSegments", "snapshotRetiredFiles",
    "snapshotRetiredMissing", "snapshotRetiredSkippedActive", "snapshotRetiredBytes",
}
row = {"unix": int(time.time())}
for key, value in zip(keys, values):
    row[key] = int(value) if key in ints else value
for key in ("freezerAlertDetails", "stageVerifyDetails", "snapshotAlertDetails"):
    try:
        parsed = json.loads(row.get(key, "[]"))
    except Exception:
        parsed = []
    row[key] = parsed if isinstance(parsed, list) else []
line = json.dumps(row, sort_keys=True)
with open(out, "a", encoding="utf-8") as fh:
    fh.write(line + "\n")
print(line)
PY
}

storage_alert_result_status() {
  if [ "$RUN_STORAGE_ALERT_FAILED" -ne 0 ]; then
    echo "storage-alerts-critical"
    return
  fi
  echo "ok"
}

fail_after_storage_alert_result_if_needed() {
  local mode="$1"
  local role="$2"
  local log_path="$3"
  if [ "$RUN_STORAGE_ALERT_FAILED" -ne 0 ]; then
    die "storage-alerts reported critical storage state for $mode/$role; result row was emitted; see $log_path"
  fi
}

run_producer_mode() {
  local mode="$1"
  local idx="$2"
  local port_base=$((BASE_PORT + idx * 20))
  local datadir="$WORKDIR/$mode-producer"
  local log_path="$WORKDIR/$mode-producer.log"
  reset_run_metrics
  mkdir -p "$datadir"
  local start=$SECONDS
  local pid
  start_node "$mode producer" "$mode" "$datadir" "$((port_base + 1))" "$((port_base + 2))" "$((port_base + 3))" 1 "" "$log_path"
  pid="$STARTED_PID"
  local height
  height="$(wait_for_block "$((port_base + 1))" "$TARGET_BLOCKS" "$mode producer")"
  local elapsed=$((SECONDS - start))
  stop_pid "$pid"
  maybe_build_cold_freezer "$datadir" "$height" "$log_path"
  maybe_build_derived_indexes "$datadir" "$height" "$log_path"
  collect_event_log_index_stats "$datadir" "$log_path"
  run_signed_cold_prune_drill "$mode" "$idx" "$datadir" "$log_path"
  run_storage_alert_gate "$mode" "producer" "$datadir" "$log_path"
  local result_status
  result_status="$(storage_alert_result_status)"
  emit_result "$PROFILE" "$mode" "producer" "$result_status" "$TARGET_BLOCKS" "$height" "$elapsed" "$datadir" "$log_path"
  fail_after_storage_alert_result_if_needed "$mode" "producer" "$log_path"
}

run_sync_mode() {
  local mode="$1"
  local idx="$2"
  local port_base=$((BASE_PORT + idx * 30))
  local sr_dir="$WORKDIR/$mode-sync-sr"
  local node_dir="$WORKDIR/$mode-sync-follower"
  local sr_log="$WORKDIR/$mode-sync-sr.log"
  local node_log="$WORKDIR/$mode-sync-follower.log"
  reset_run_metrics
  mkdir -p "$sr_dir" "$node_dir"

  local sr_pid
  start_node "$mode sync sr" "full" "$sr_dir" "$((port_base + 1))" "$((port_base + 2))" "$((port_base + 3))" 1 "" "$sr_log"
  sr_pid="$STARTED_PID"
  wait_for_block "$((port_base + 1))" "$TARGET_BLOCKS" "$mode sync sr" >/dev/null

  local start=$SECONDS
  local node_pid
  start_node "$mode sync follower" "$mode" "$node_dir" "$((port_base + 11))" "$((port_base + 12))" "$((port_base + 13))" 0 "127.0.0.1:$((port_base + 2))" "$node_log"
  node_pid="$STARTED_PID"
  local height
  height="$(wait_for_sync_close "$((port_base + 1))" "$((port_base + 11))")"
  local elapsed=$((SECONDS - start))
  stop_pid "$node_pid"
  stop_pid "$sr_pid"
  run_storage_alert_gate "$mode" "sync-follower" "$node_dir" "$node_log"
  local result_status
  result_status="$(storage_alert_result_status)"
  emit_result "$PROFILE" "$mode" "sync-follower" "$result_status" "$TARGET_BLOCKS" "$height" "$elapsed" "$node_dir" "$node_log"
  fail_after_storage_alert_result_if_needed "$mode" "sync-follower" "$node_log"
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
