#!/usr/bin/env bash
#
# Compare go-tron storage/sync behaviour across Erigon-style prune modes.
#
# The script is intentionally a harness, not a pass/fail test: it emits one
# JSON object per run so repeated samples can be graphed by mode.
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/../.." && pwd)"
GTRON="${GTRON:-$BASEDIR/build/bin/gtron}"
MODES="full,blocks,minimal,snap,archive"
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
ARCHIVE_API_PROBE=0
ARCHIVE_API_BLOCK=-1
ARCHIVE_API_ADDRESS="0x410000000000000000000000000000000000000000"
ARCHIVE_API_STORAGE_SLOT="0x0"
ARCHIVE_API_CALL_DATA=""
ARCHIVE_API_TRACE_TRANSACTION=0
ARCHIVE_API_TRACE_BLOCK=0

# Fixed dev witness key also used by scripts/system_test.sh.
WITNESS_KEY="c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4"

PIDS=()
STARTED_PID=""
RUN_COLD_FREEZER_TO_BLOCK=-1
RUN_DERIVED_INDEX_TO_BLOCK=-1
RUN_DERIVED_INDEX_SEGMENTS=0
RUN_DERIVED_INDEX_BUILD_SECONDS=0
RUN_EVENT_LOG_INDEX_SEGMENTS=0
RUN_EVENT_LOG_INDEX_FROM_BLOCK=-1
RUN_EVENT_LOG_INDEX_TO_BLOCK=-1
RUN_EVENT_LOG_INDEX_ADDRESS_KEYS=0
RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS=0
RUN_EVENT_LOG_INDEX_ADDRESS_AVG_POSTINGS_MILLI=0
RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS=0
RUN_EVENT_LOG_INDEX_ADDRESS_SINGLETON_KEYS=0
RUN_EVENT_LOG_INDEX_ADDRESS_MULTI_POSTING_KEYS=0
RUN_EVENT_LOG_INDEX_TOPIC_KEYS=0
RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS=0
RUN_EVENT_LOG_INDEX_TOPIC_AVG_POSTINGS_MILLI=0
RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS=0
RUN_EVENT_LOG_INDEX_TOPIC_SINGLETON_KEYS=0
RUN_EVENT_LOG_INDEX_TOPIC_MULTI_POSTING_KEYS=0
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
RUN_STORAGE_ALERT_STATUS="not-run"
RUN_STAGE_VERIFY_STATUS="not-run"
RUN_STAGE_VERIFY_ISSUES=-1
RUN_STAGE_VERIFY_DETAILS="[]"
RUN_STAGE_ALERT_PIPELINE_COMPLETE="false"
RUN_STAGE_ALERT_PIPELINE_PENDING=-1
RUN_STAGE_ALERT_PIPELINE_ISSUES=-1
RUN_STAGE_ALERT_PIPELINE_NEXT=""
RUN_STAGE_ALERT_PIPELINE_NEXT_STATUS=""
RUN_STAGE_ALERT_PIPELINE_NEXT_TARGET=-1
RUN_STAGE_ALERT_PIPELINE_NEXT_UPSTREAM=""
RUN_STAGE_ALERT_PIPELINE_NEXT_CURRENT=-1
RUN_STAGE_ALERT_PIPELINE_TASKS="[]"
RUN_MODE_ALERT_STATUS="not-run"
RUN_MODE_ALERT_ISSUES=-1
RUN_MODE_ALERT_DETAILS="[]"
RUN_PRUNE_MODE="unknown"
RUN_PRUNE_MODE_PERSISTED="false"
RUN_SNAPSHOT_ALERT_STATUS="not-run"
RUN_SNAPSHOT_ALERT_ISSUES=-1
RUN_SNAPSHOT_ALERT_DETAILS="[]"
RUN_SNAPSHOT_RETIRED_SEGMENTS=-1
RUN_SNAPSHOT_RETIRED_FILES=-1
RUN_SNAPSHOT_RETIRED_MISSING=-1
RUN_SNAPSHOT_RETIRED_SKIPPED_ACTIVE=-1
RUN_SNAPSHOT_RETIRED_BYTES=-1
RUN_STORAGE_ALERT_FAILED=0
RUN_STORAGE_ALERT_PROMETHEUS=""
RUN_ARCHIVE_API_STATUS="not-run"
RUN_ARCHIVE_API_CHECKS=0
RUN_ARCHIVE_API_FAILURES=0
RUN_ARCHIVE_API_BLOCK=-1
RUN_ARCHIVE_API_DEPTH_BLOCKS=-1
RUN_ARCHIVE_API_CALL_PROBE="false"
RUN_ARCHIVE_API_TRACE_TRANSACTION_PROBE="false"
RUN_ARCHIVE_API_TRACE_BLOCK_PROBE="false"
RUN_ARCHIVE_API_METHODS="[]"
RUN_ARCHIVE_API_TX_PROBE="false"
RUN_ARCHIVE_API_TX_HASH=""
RUN_ARCHIVE_API_TX_METHODS="[]"
SNAPSHOT_PROFILE_SCRIPT="$BASEDIR/scripts/dev/snapshot_manifest_profile.py"

usage() {
  cat <<'EOF'
Usage: scripts/dev/storage_benchmark.sh [options]

Profiles:
  producer    Start one dev witness per mode and measure time to target block.
  sync        Start a dev witness and a fresh follower per mode; measure catch-up.

Options:
  --profile producer|sync        Benchmark profile (default: producer)
  --modes full,blocks,minimal,snap,archive
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
  --snapshot-signing-seed HEX    Ed25519 seed/private key for signed-cold-prune;
                                  written to a temp key file before invoking gtron
  --sync-max-diff N              Sync profile success threshold (default: 2)
  --history-window N             Inject [history] prune_window for short prune drills
  --archive-api-probe            Probe historical JSON-RPC archive APIs and emit archiveApi*/archiveApiTx* fields
  --archive-api-block N          Historical block for archive API probe (default: height-1)
  --archive-api-address HEX      0x41-prefixed TRON address for account/contract probes
  --archive-api-storage-slot HEX Storage slot for eth_getStorageAt probe (default: 0x0)
  --archive-api-call-data HEX    Include eth_call, debug_traceCall, and eth_estimateGas with this calldata against archive-api-address
  --archive-api-trace-transaction
                                  Include debug_traceTransaction when archive-api-block has a transaction
  --archive-api-trace-block
                                  Include debug_traceBlockByNumber/Hash for archive-api-block

Examples:
  scripts/dev/storage_benchmark.sh --modes full,blocks,minimal,snap,archive --target-blocks 80
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
    --archive-api-probe) ARCHIVE_API_PROBE=1; shift ;;
    --archive-api-block) ARCHIVE_API_BLOCK="${2:?}"; shift 2 ;;
    --archive-api-address) ARCHIVE_API_ADDRESS="${2:?}"; shift 2 ;;
    --archive-api-storage-slot) ARCHIVE_API_STORAGE_SLOT="${2:?}"; shift 2 ;;
    --archive-api-call-data) ARCHIVE_API_CALL_DATA="${2:?}"; shift 2 ;;
    --archive-api-trace-transaction) ARCHIVE_API_TRACE_TRANSACTION=1; shift ;;
    --archive-api-trace-block) ARCHIVE_API_TRACE_BLOCK=1; shift ;;
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
mkdir -p "$(dirname "$OUTPUT")"
ARTIFACT_DIR="$(cd "$(dirname "$OUTPUT")" && pwd)"
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
  RUN_EVENT_LOG_INDEX_FROM_BLOCK=-1
  RUN_EVENT_LOG_INDEX_TO_BLOCK=-1
  RUN_EVENT_LOG_INDEX_ADDRESS_KEYS=0
  RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS=0
  RUN_EVENT_LOG_INDEX_ADDRESS_AVG_POSTINGS_MILLI=0
  RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS=0
  RUN_EVENT_LOG_INDEX_ADDRESS_SINGLETON_KEYS=0
  RUN_EVENT_LOG_INDEX_ADDRESS_MULTI_POSTING_KEYS=0
  RUN_EVENT_LOG_INDEX_TOPIC_KEYS=0
  RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS=0
  RUN_EVENT_LOG_INDEX_TOPIC_AVG_POSTINGS_MILLI=0
  RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS=0
  RUN_EVENT_LOG_INDEX_TOPIC_SINGLETON_KEYS=0
  RUN_EVENT_LOG_INDEX_TOPIC_MULTI_POSTING_KEYS=0
  RUN_BALANCE_TRACE_PRUNE_TO_BLOCK=-1
  RUN_BALANCE_TRACE_BLOCK_ROWS=0
  RUN_BALANCE_TRACE_ACCOUNT_ROWS=0
  RUN_SECTION_BLOOM_PRUNE_TO_SECTION=-1
  RUN_SECTION_BLOOM_ROWS=0
  RUN_SIGNED_COLD_PRUNE=0
  RUN_CHAIN_LOOKUP_PRUNE_TO_BLOCK=-1
  RUN_CHAIN_LOOKUP_BLOCK_INDEXES=0
  RUN_CHAIN_LOOKUP_TX_INDEXES=0
  RUN_RETIRED_PRUNE_RAN="false"
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
  RUN_STORAGE_ALERT_STATUS="not-run"
  RUN_STAGE_VERIFY_STATUS="not-run"
  RUN_STAGE_VERIFY_ISSUES=-1
  RUN_STAGE_VERIFY_DETAILS="[]"
  RUN_STAGE_ALERT_PIPELINE_COMPLETE="false"
  RUN_STAGE_ALERT_PIPELINE_PENDING=-1
  RUN_STAGE_ALERT_PIPELINE_ISSUES=-1
  RUN_STAGE_ALERT_PIPELINE_NEXT=""
  RUN_STAGE_ALERT_PIPELINE_NEXT_STATUS=""
  RUN_STAGE_ALERT_PIPELINE_NEXT_TARGET=-1
  RUN_STAGE_ALERT_PIPELINE_NEXT_UPSTREAM=""
  RUN_STAGE_ALERT_PIPELINE_NEXT_CURRENT=-1
  RUN_STAGE_ALERT_PIPELINE_TASKS="[]"
  RUN_MODE_ALERT_STATUS="not-run"
  RUN_MODE_ALERT_ISSUES=-1
  RUN_MODE_ALERT_DETAILS="[]"
  RUN_PRUNE_MODE="unknown"
  RUN_PRUNE_MODE_PERSISTED="false"
  RUN_SNAPSHOT_ALERT_STATUS="not-run"
  RUN_SNAPSHOT_ALERT_ISSUES=-1
  RUN_SNAPSHOT_ALERT_DETAILS="[]"
  RUN_SNAPSHOT_RETIRED_SEGMENTS=-1
  RUN_SNAPSHOT_RETIRED_FILES=-1
  RUN_SNAPSHOT_RETIRED_MISSING=-1
  RUN_SNAPSHOT_RETIRED_SKIPPED_ACTIVE=-1
  RUN_SNAPSHOT_RETIRED_BYTES=-1
  RUN_STORAGE_ALERT_FAILED=0
  RUN_STORAGE_ALERT_PROMETHEUS=""
  RUN_ARCHIVE_API_STATUS="not-run"
  RUN_ARCHIVE_API_CHECKS=0
  RUN_ARCHIVE_API_FAILURES=0
  RUN_ARCHIVE_API_BLOCK=-1
  RUN_ARCHIVE_API_DEPTH_BLOCKS=-1
  if [ -n "$ARCHIVE_API_CALL_DATA" ]; then
    RUN_ARCHIVE_API_CALL_PROBE="true"
  else
    RUN_ARCHIVE_API_CALL_PROBE="false"
  fi
  if [ "$ARCHIVE_API_TRACE_TRANSACTION" -eq 1 ]; then
    RUN_ARCHIVE_API_TRACE_TRANSACTION_PROBE="true"
  else
    RUN_ARCHIVE_API_TRACE_TRANSACTION_PROBE="false"
  fi
  if [ "$ARCHIVE_API_TRACE_BLOCK" -eq 1 ]; then
    RUN_ARCHIVE_API_TRACE_BLOCK_PROBE="true"
  else
    RUN_ARCHIVE_API_TRACE_BLOCK_PROBE="false"
  fi
  RUN_ARCHIVE_API_METHODS="[]"
  RUN_ARCHIVE_API_TX_PROBE="false"
  RUN_ARCHIVE_API_TX_HASH=""
  RUN_ARCHIVE_API_TX_METHODS="[]"
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

derived_index_stats() {
  local path="$1"
  if [ ! -e "$path" ]; then
    printf '0\n0\n'
    return
  fi
  python3 - "$path" <<'PY'
import os
import sys

root = sys.argv[1]
markers = (
    "chain-freezer-accessor",
    "chain-index",
    "balance-trace",
    "section-bloom",
    "event-log",
)
total = 0
files = 0
try:
    for dirpath, _, names in os.walk(root):
        for name in names:
            path = os.path.join(dirpath, name)
            if not os.path.isfile(path):
                continue
            rel = os.path.relpath(path, root).replace(os.sep, "/")
            if not any(marker in rel for marker in markers):
                continue
            try:
                st = os.stat(path)
            except OSError:
                continue
            blocks = getattr(st, "st_blocks", 0)
            total += blocks * 512 if blocks else st.st_size
            files += 1
except Exception:
    total = 0
    files = 0
print(total)
print(files)
PY
}

write_storage_benchmark_prometheus() {
  local path="$1"
  local profile="$2"
  local mode="$3"
  local role="$4"
  local status="$5"
  local height="$6"
  local elapsed="$7"
  local datadir="$8"
  local total="$9"
  local chain="${10}"
  local ancient="${11}"
  local snapshots="${12}"
  local derived_index_bytes="${13}"
  local snapshot_sidecar_share_milli="${14}"
  local snapshot_point_tx_hash_lookup_segments="${15}"
  local snapshot_point_tx_hash_lookup_bytes="${16}"
  local snapshot_point_tx_hash_lookup_payload_bytes="${17}"
  local snapshot_point_tx_hash_lookup_sidecar_bytes="${18}"
  local snapshot_point_tx_hash_lookup_sidecar_share_milli="${19}"
  local snapshot_point_tx_hash_lookup_share_milli="${20}"
  local snapshot_point_event_log_index_segments="${21}"
  local snapshot_point_event_log_index_bytes="${22}"
  local snapshot_point_event_log_index_payload_bytes="${23}"
  local snapshot_point_event_log_index_sidecar_bytes="${24}"
  local snapshot_point_event_log_index_sidecar_share_milli="${25}"
  local snapshot_point_event_log_index_share_milli="${26}"
  local snapshot_point_state_history_accessor_segments="${27}"
  local snapshot_point_state_history_accessor_bytes="${28}"
  local snapshot_point_state_history_accessor_payload_bytes="${29}"
  local snapshot_point_state_history_accessor_sidecar_bytes="${30}"
  local snapshot_point_state_history_accessor_sidecar_share_milli="${31}"
  local snapshot_point_state_history_accessor_share_milli="${32}"
  local snapshot_point_latest_btree_segments="${33}"
  local snapshot_point_latest_btree_bytes="${34}"
  local snapshot_point_latest_btree_payload_bytes="${35}"
  local snapshot_point_latest_btree_sidecar_bytes="${36}"
  local snapshot_point_latest_btree_sidecar_share_milli="${37}"
  local snapshot_point_latest_btree_share_milli="${38}"
  local snapshot_point_chain_freezer_accessor_segments="${39}"
  local snapshot_point_chain_freezer_accessor_bytes="${40}"
  local snapshot_point_chain_freezer_accessor_payload_bytes="${41}"
  local snapshot_point_chain_freezer_accessor_sidecar_bytes="${42}"
  local snapshot_point_chain_freezer_accessor_sidecar_share_milli="${43}"
  local snapshot_point_chain_freezer_accessor_share_milli="${44}"
  local snapshot_point_code_domain_segments="${45}"
  local snapshot_point_code_domain_bytes="${46}"
  local snapshot_point_code_domain_payload_bytes="${47}"
  local snapshot_point_code_domain_sidecar_bytes="${48}"
  local snapshot_point_code_domain_sidecar_share_milli="${49}"
  local snapshot_point_code_domain_share_milli="${50}"
  local snapshot_point_commitment_snapshot_segments="${51}"
  local snapshot_point_commitment_snapshot_bytes="${52}"
  local snapshot_point_commitment_snapshot_payload_bytes="${53}"
  local snapshot_point_commitment_snapshot_sidecar_bytes="${54}"
  local snapshot_point_commitment_snapshot_sidecar_share_milli="${55}"
  local snapshot_point_commitment_snapshot_share_milli="${56}"
  local archive_api_checks="${57}"
  local archive_api_block="${58}"
  local archive_api_depth_blocks="${59}"
  local archive_api_failures="${60}"
  local archive_api_call_probe="${61}"
  local archive_api_trace_transaction_probe="${62}"
  local archive_api_trace_block_probe="${63}"
  local archive_api_methods="${64}"
  local archive_api_tx_probe="${65}"
  local archive_api_tx_methods="${66}"
  local cold_freezer_to_block="${67}"
  local derived_index_to_block="${68}"
  local chain_lookup_prune_to_block="${69}"
  local tail_pruned_through_block="${70}"
  local balance_trace_prune_to_block="${71}"
  local section_bloom_prune_to_section="${72}"
  local signed_cold_prune="${73}"
  local tail_pruned_files="${74}"
  local history_window="${75}"
  local event_log_index_segments="${76}"
  local event_log_index_address_keys="${77}"
  local event_log_index_address_postings="${78}"
  local event_log_index_address_avg_postings_milli="${79}"
  local event_log_index_address_max_postings="${80}"
  local event_log_index_address_singleton_keys="${81}"
  local event_log_index_address_multi_posting_keys="${82}"
  local event_log_index_topic_keys="${83}"
  local event_log_index_topic_postings="${84}"
  local event_log_index_topic_avg_postings_milli="${85}"
  local event_log_index_topic_max_postings="${86}"
  local event_log_index_topic_singleton_keys="${87}"
  local event_log_index_topic_multi_posting_keys="${88}"
  local event_log_index_from_block="${89}"
  local event_log_index_to_block="${90}"
  local snapshot_state_history_bytes="${91}"
  local snapshot_state_history_compressed_segments="${92}"
  local snapshot_state_history_compressed_bytes="${93}"
  local snapshot_state_history_compressed_share_milli="${94}"
  python3 - "$path" "$profile" "$mode" "$role" "$status" "$height" "$elapsed" "$datadir" \
    "$total" "$chain" "$ancient" "$snapshots" "$derived_index_bytes" \
    "$snapshot_sidecar_share_milli" \
    "$snapshot_point_tx_hash_lookup_segments" "$snapshot_point_tx_hash_lookup_bytes" \
    "$snapshot_point_tx_hash_lookup_payload_bytes" "$snapshot_point_tx_hash_lookup_sidecar_bytes" \
    "$snapshot_point_tx_hash_lookup_sidecar_share_milli" "$snapshot_point_tx_hash_lookup_share_milli" \
    "$snapshot_point_event_log_index_segments" "$snapshot_point_event_log_index_bytes" \
    "$snapshot_point_event_log_index_payload_bytes" "$snapshot_point_event_log_index_sidecar_bytes" \
    "$snapshot_point_event_log_index_sidecar_share_milli" "$snapshot_point_event_log_index_share_milli" \
    "$snapshot_point_state_history_accessor_segments" "$snapshot_point_state_history_accessor_bytes" \
    "$snapshot_point_state_history_accessor_payload_bytes" "$snapshot_point_state_history_accessor_sidecar_bytes" \
    "$snapshot_point_state_history_accessor_sidecar_share_milli" "$snapshot_point_state_history_accessor_share_milli" \
    "$snapshot_point_latest_btree_segments" "$snapshot_point_latest_btree_bytes" \
    "$snapshot_point_latest_btree_payload_bytes" "$snapshot_point_latest_btree_sidecar_bytes" \
    "$snapshot_point_latest_btree_sidecar_share_milli" "$snapshot_point_latest_btree_share_milli" \
    "$snapshot_point_chain_freezer_accessor_segments" "$snapshot_point_chain_freezer_accessor_bytes" \
    "$snapshot_point_chain_freezer_accessor_payload_bytes" "$snapshot_point_chain_freezer_accessor_sidecar_bytes" \
    "$snapshot_point_chain_freezer_accessor_sidecar_share_milli" "$snapshot_point_chain_freezer_accessor_share_milli" \
    "$snapshot_point_code_domain_segments" "$snapshot_point_code_domain_bytes" \
    "$snapshot_point_code_domain_payload_bytes" "$snapshot_point_code_domain_sidecar_bytes" \
    "$snapshot_point_code_domain_sidecar_share_milli" "$snapshot_point_code_domain_share_milli" \
    "$snapshot_point_commitment_snapshot_segments" "$snapshot_point_commitment_snapshot_bytes" \
    "$snapshot_point_commitment_snapshot_payload_bytes" "$snapshot_point_commitment_snapshot_sidecar_bytes" \
    "$snapshot_point_commitment_snapshot_sidecar_share_milli" "$snapshot_point_commitment_snapshot_share_milli" \
    "$archive_api_checks" "$archive_api_block" \
    "$archive_api_depth_blocks" "$archive_api_failures" "$archive_api_call_probe" "$archive_api_trace_transaction_probe" \
    "$archive_api_trace_block_probe" "$archive_api_methods" "$archive_api_tx_probe" "$archive_api_tx_methods" \
    "$cold_freezer_to_block" "$derived_index_to_block" "$chain_lookup_prune_to_block" \
    "$tail_pruned_through_block" "$balance_trace_prune_to_block" \
    "$section_bloom_prune_to_section" "$signed_cold_prune" "$tail_pruned_files" \
    "$history_window" "$event_log_index_segments" "$event_log_index_address_keys" \
    "$event_log_index_address_postings" "$event_log_index_address_avg_postings_milli" \
    "$event_log_index_address_max_postings" "$event_log_index_address_singleton_keys" \
    "$event_log_index_address_multi_posting_keys" "$event_log_index_topic_keys" \
    "$event_log_index_topic_postings" "$event_log_index_topic_avg_postings_milli" \
    "$event_log_index_topic_max_postings" "$event_log_index_topic_singleton_keys" \
    "$event_log_index_topic_multi_posting_keys" "$event_log_index_from_block" \
    "$event_log_index_to_block" \
    "$snapshot_state_history_bytes" "$snapshot_state_history_compressed_segments" \
    "$snapshot_state_history_compressed_bytes" "$snapshot_state_history_compressed_share_milli" <<'PY'
import json
import sys
from pathlib import Path

path = Path(sys.argv[1])
profile, mode, role, status = sys.argv[2:6]
height, elapsed = (int(sys.argv[6]), int(sys.argv[7]))
datadir = sys.argv[8]
total, chain, ancient, snapshots, derived_index = map(int, sys.argv[9:14])
snapshot_sidecar_share_milli = int(sys.argv[14])
snapshot_point_tx_hash_lookup_segments = int(sys.argv[15])
snapshot_point_tx_hash_lookup_bytes = int(sys.argv[16])
snapshot_point_tx_hash_lookup_payload_bytes = int(sys.argv[17])
snapshot_point_tx_hash_lookup_sidecar_bytes = int(sys.argv[18])
snapshot_point_tx_hash_lookup_sidecar_share_milli = int(sys.argv[19])
snapshot_point_tx_hash_lookup_share_milli = int(sys.argv[20])
snapshot_point_event_log_index_segments = int(sys.argv[21])
snapshot_point_event_log_index_bytes = int(sys.argv[22])
snapshot_point_event_log_index_payload_bytes = int(sys.argv[23])
snapshot_point_event_log_index_sidecar_bytes = int(sys.argv[24])
snapshot_point_event_log_index_sidecar_share_milli = int(sys.argv[25])
snapshot_point_event_log_index_share_milli = int(sys.argv[26])
snapshot_point_state_history_accessor_segments = int(sys.argv[27])
snapshot_point_state_history_accessor_bytes = int(sys.argv[28])
snapshot_point_state_history_accessor_payload_bytes = int(sys.argv[29])
snapshot_point_state_history_accessor_sidecar_bytes = int(sys.argv[30])
snapshot_point_state_history_accessor_sidecar_share_milli = int(sys.argv[31])
snapshot_point_state_history_accessor_share_milli = int(sys.argv[32])
snapshot_point_latest_btree_segments = int(sys.argv[33])
snapshot_point_latest_btree_bytes = int(sys.argv[34])
snapshot_point_latest_btree_payload_bytes = int(sys.argv[35])
snapshot_point_latest_btree_sidecar_bytes = int(sys.argv[36])
snapshot_point_latest_btree_sidecar_share_milli = int(sys.argv[37])
snapshot_point_latest_btree_share_milli = int(sys.argv[38])
snapshot_point_chain_freezer_accessor_segments = int(sys.argv[39])
snapshot_point_chain_freezer_accessor_bytes = int(sys.argv[40])
snapshot_point_chain_freezer_accessor_payload_bytes = int(sys.argv[41])
snapshot_point_chain_freezer_accessor_sidecar_bytes = int(sys.argv[42])
snapshot_point_chain_freezer_accessor_sidecar_share_milli = int(sys.argv[43])
snapshot_point_chain_freezer_accessor_share_milli = int(sys.argv[44])
snapshot_point_code_domain_segments = int(sys.argv[45])
snapshot_point_code_domain_bytes = int(sys.argv[46])
snapshot_point_code_domain_payload_bytes = int(sys.argv[47])
snapshot_point_code_domain_sidecar_bytes = int(sys.argv[48])
snapshot_point_code_domain_sidecar_share_milli = int(sys.argv[49])
snapshot_point_code_domain_share_milli = int(sys.argv[50])
snapshot_point_commitment_snapshot_segments = int(sys.argv[51])
snapshot_point_commitment_snapshot_bytes = int(sys.argv[52])
snapshot_point_commitment_snapshot_payload_bytes = int(sys.argv[53])
snapshot_point_commitment_snapshot_sidecar_bytes = int(sys.argv[54])
snapshot_point_commitment_snapshot_sidecar_share_milli = int(sys.argv[55])
snapshot_point_commitment_snapshot_share_milli = int(sys.argv[56])
archive_api_checks, archive_api_block, archive_api_depth_blocks, archive_api_failures = map(int, sys.argv[57:61])
archive_api_call_probe = sys.argv[61].lower() in {"1", "true", "yes"}
archive_api_trace_transaction_probe = sys.argv[62].lower() in {"1", "true", "yes"}
archive_api_trace_block_probe = sys.argv[63].lower() in {"1", "true", "yes"}
archive_api_methods_raw = sys.argv[64]
archive_api_tx_probe = sys.argv[65].lower() in {"1", "true", "yes"}
archive_api_tx_methods_raw = sys.argv[66]
cold_freezer_to_block = int(sys.argv[67])
derived_index_to_block = int(sys.argv[68])
chain_lookup_prune_to_block = int(sys.argv[69])
tail_pruned_through_block = int(sys.argv[70])
balance_trace_prune_to_block = int(sys.argv[71])
section_bloom_prune_to_section = int(sys.argv[72])
signed_cold_prune = int(sys.argv[73])
tail_pruned_files = int(sys.argv[74])
history_window = int(sys.argv[75])
event_log_index_segments = int(sys.argv[76])
event_log_index_address_keys = int(sys.argv[77])
event_log_index_address_postings = int(sys.argv[78])
event_log_index_address_avg_postings_milli = int(sys.argv[79])
event_log_index_address_max_postings = int(sys.argv[80])
event_log_index_address_singleton_keys = int(sys.argv[81])
event_log_index_address_multi_posting_keys = int(sys.argv[82])
event_log_index_topic_keys = int(sys.argv[83])
event_log_index_topic_postings = int(sys.argv[84])
event_log_index_topic_avg_postings_milli = int(sys.argv[85])
event_log_index_topic_max_postings = int(sys.argv[86])
event_log_index_topic_singleton_keys = int(sys.argv[87])
event_log_index_topic_multi_posting_keys = int(sys.argv[88])
event_log_index_from_block = int(sys.argv[89])
event_log_index_to_block = int(sys.argv[90])
snapshot_state_history_bytes = int(sys.argv[91])
snapshot_state_history_compressed_segments = int(sys.argv[92])
snapshot_state_history_compressed_bytes = int(sys.argv[93])
snapshot_state_history_compressed_share_milli = int(sys.argv[94])
datadir_per_block = float(total) / height if height > 0 else 0.0
hot_per_block = float(chain) / height if height > 0 else 0.0
cold_archive_per_block = float(ancient + snapshots) / height if height > 0 else 0.0
derived_index_per_block = float(derived_index) / height if height > 0 else 0.0

def esc(value):
    return str(value).replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')

labels = (
    f'datadir="{esc(datadir)}",'
    f'mode="{esc(mode)}",'
    f'profile="{esc(profile)}",'
    f'role="{esc(role)}",'
    f'status="{esc(status)}"'
)

ARCHIVE_API_BASE_METHODS = (
    "eth_getBlockByNumber",
    "eth_getBlockByHash",
    "eth_getBlockTransactionCountByNumber",
    "eth_getBlockTransactionCountByHash",
    "eth_getUncleCountByBlockNumber",
    "eth_getUncleCountByBlockHash",
    "eth_getUncleByBlockNumberAndIndex",
    "eth_getUncleByBlockHashAndIndex",
    "eth_getBlockReceipts",
    "eth_getBalance",
    "eth_getCode",
    "eth_getStorageAt",
    "eth_getLogs",
)
ARCHIVE_API_CALL_METHODS = ("eth_call", "debug_traceCall", "eth_estimateGas")
ARCHIVE_API_TX_METHODS = (
    "eth_getTransactionByHash",
    "eth_getTransactionReceipt",
    "eth_getTransactionByBlockNumberAndIndex",
    "eth_getTransactionByBlockHashAndIndex",
)
ARCHIVE_API_TRACE_TRANSACTION_METHODS = ("debug_traceTransaction",)
ARCHIVE_API_TRACE_BLOCK_METHODS = ("debug_traceBlockByNumber", "debug_traceBlockByHash")
BENCHMARK_STATUS_VALUES = {
    "ok": 0,
    "warning": 1,
    "storage-alerts-critical": 2,
    "unknown": 3,
}

def method_set(raw):
    try:
        parsed = json.loads(raw)
    except Exception:
        return set()
    if not isinstance(parsed, list):
        return set()
    return {str(value) for value in parsed}

def archive_api_expected_methods(successful_methods):
    expected = list(ARCHIVE_API_BASE_METHODS)
    if archive_api_call_probe or any(method in successful_methods for method in ARCHIVE_API_CALL_METHODS):
        expected.extend(ARCHIVE_API_CALL_METHODS)
    if archive_api_tx_probe or any(
        method in successful_methods
        for method in ARCHIVE_API_TX_METHODS + ARCHIVE_API_TRACE_TRANSACTION_METHODS
    ):
        expected.extend(ARCHIVE_API_TX_METHODS)
    if archive_api_trace_transaction_probe or any(
        method in successful_methods for method in ARCHIVE_API_TRACE_TRANSACTION_METHODS
    ):
        expected.extend(ARCHIVE_API_TRACE_TRANSACTION_METHODS)
    if archive_api_trace_block_probe or any(
        method in successful_methods for method in ARCHIVE_API_TRACE_BLOCK_METHODS
    ):
        expected.extend(ARCHIVE_API_TRACE_BLOCK_METHODS)
    return expected

def labels_with(extra):
    labels_map = {
        "datadir": datadir,
        "mode": mode,
        "profile": profile,
        "role": role,
        "status": status,
    }
    labels_map.update(extra)
    return "{" + ",".join(f'{key}="{esc(labels_map[key])}"' for key in sorted(labels_map)) + "}"

def snapshot_point_metrics(
    slug,
    display,
    segments,
    total_bytes,
    payload_bytes,
    sidecar_bytes,
    sidecar_share_milli,
    snapshot_share_milli,
):
    prefix = f"gtron_storage_benchmark_snapshot_point_{slug}"
    return (
        (f"{prefix}_segments", f"Benchmark snapshot segment count covered by the {display}.", segments),
        (f"{prefix}_bytes", f"Benchmark snapshot bytes covered by the {display}.", total_bytes),
        (f"{prefix}_payload_bytes", f"Benchmark snapshot payload bytes covered by the {display}.", payload_bytes),
        (f"{prefix}_sidecar_bytes", f"Benchmark snapshot sidecar bytes covered by the {display}.", sidecar_bytes),
        (
            f"{prefix}_sidecar_share_milli",
            f"Benchmark candidate-local sidecar share for the {display} in milli-units.",
            sidecar_share_milli,
        ),
        (
            f"{prefix}_snapshot_share_milli",
            f"Benchmark snapshot-wide share for the {display} in milli-units.",
            snapshot_share_milli,
        ),
    )

metrics = (
    (
        "gtron_storage_benchmark_status",
        "Benchmark status: 0=ok, 1=warning, 2=critical, 3=unknown/other.",
        BENCHMARK_STATUS_VALUES.get(status.lower(), BENCHMARK_STATUS_VALUES["unknown"]),
    ),
    ("gtron_storage_benchmark_height", "Benchmark run block height.", height),
    ("gtron_storage_benchmark_elapsed_seconds", "Benchmark run elapsed seconds.", elapsed),
    ("gtron_storage_benchmark_datadir_bytes", "Total benchmark datadir bytes.", total),
    ("gtron_storage_benchmark_chaindata_bytes", "Hot benchmark chaindata bytes.", chain),
    ("gtron_storage_benchmark_ancient_bytes", "Benchmark ancient freezer bytes.", ancient),
    ("gtron_storage_benchmark_snapshot_bytes", "Benchmark state snapshot bytes.", snapshots),
    ("gtron_storage_benchmark_cold_archive_bytes", "Benchmark ancient plus snapshot bytes.", ancient + snapshots),
    ("gtron_storage_benchmark_derived_index_bytes", "Benchmark derived cold index bytes.", derived_index),
    ("gtron_storage_benchmark_datadir_bytes_per_block", "Benchmark total datadir bytes per imported block.", datadir_per_block),
    ("gtron_storage_benchmark_hot_bytes_per_block", "Benchmark hot chaindata bytes per imported block.", hot_per_block),
    ("gtron_storage_benchmark_cold_archive_bytes_per_block", "Benchmark cold archive bytes per imported block.", cold_archive_per_block),
    ("gtron_storage_benchmark_derived_index_bytes_per_block", "Benchmark derived cold index bytes per imported block.", derived_index_per_block),
    ("gtron_storage_benchmark_snapshot_sidecar_share_milli", "Benchmark snapshot sidecar share in milli-units.", snapshot_sidecar_share_milli),
    ("gtron_storage_benchmark_snapshot_state_history_bytes", "Benchmark state-history snapshot bytes.", snapshot_state_history_bytes),
    (
        "gtron_storage_benchmark_snapshot_state_history_compressed_segments",
        "Benchmark block-compressed state-history snapshot segment count.",
        snapshot_state_history_compressed_segments,
    ),
    (
        "gtron_storage_benchmark_snapshot_state_history_compressed_bytes",
        "Benchmark block-compressed state-history snapshot bytes.",
        snapshot_state_history_compressed_bytes,
    ),
    (
        "gtron_storage_benchmark_snapshot_state_history_compressed_share_milli",
        "Benchmark state-history bytes stored in block-compressed segments in milli-units.",
        snapshot_state_history_compressed_share_milli,
    ),
    *snapshot_point_metrics(
        "tx_hash_lookup",
        "tx-hash point lookup candidate",
        snapshot_point_tx_hash_lookup_segments,
        snapshot_point_tx_hash_lookup_bytes,
        snapshot_point_tx_hash_lookup_payload_bytes,
        snapshot_point_tx_hash_lookup_sidecar_bytes,
        snapshot_point_tx_hash_lookup_sidecar_share_milli,
        snapshot_point_tx_hash_lookup_share_milli,
    ),
    *snapshot_point_metrics(
        "event_log_index",
        "event-log point lookup candidate",
        snapshot_point_event_log_index_segments,
        snapshot_point_event_log_index_bytes,
        snapshot_point_event_log_index_payload_bytes,
        snapshot_point_event_log_index_sidecar_bytes,
        snapshot_point_event_log_index_sidecar_share_milli,
        snapshot_point_event_log_index_share_milli,
    ),
    *snapshot_point_metrics(
        "state_history_accessor",
        "state-history accessor point lookup candidate",
        snapshot_point_state_history_accessor_segments,
        snapshot_point_state_history_accessor_bytes,
        snapshot_point_state_history_accessor_payload_bytes,
        snapshot_point_state_history_accessor_sidecar_bytes,
        snapshot_point_state_history_accessor_sidecar_share_milli,
        snapshot_point_state_history_accessor_share_milli,
    ),
    *snapshot_point_metrics(
        "latest_btree",
        "latest-BTree point lookup candidate",
        snapshot_point_latest_btree_segments,
        snapshot_point_latest_btree_bytes,
        snapshot_point_latest_btree_payload_bytes,
        snapshot_point_latest_btree_sidecar_bytes,
        snapshot_point_latest_btree_sidecar_share_milli,
        snapshot_point_latest_btree_share_milli,
    ),
    *snapshot_point_metrics(
        "chain_freezer_accessor",
        "chain-freezer accessor point lookup candidate",
        snapshot_point_chain_freezer_accessor_segments,
        snapshot_point_chain_freezer_accessor_bytes,
        snapshot_point_chain_freezer_accessor_payload_bytes,
        snapshot_point_chain_freezer_accessor_sidecar_bytes,
        snapshot_point_chain_freezer_accessor_sidecar_share_milli,
        snapshot_point_chain_freezer_accessor_share_milli,
    ),
    *snapshot_point_metrics(
        "code_domain",
        "CodeDomain point lookup candidate",
        snapshot_point_code_domain_segments,
        snapshot_point_code_domain_bytes,
        snapshot_point_code_domain_payload_bytes,
        snapshot_point_code_domain_sidecar_bytes,
        snapshot_point_code_domain_sidecar_share_milli,
        snapshot_point_code_domain_share_milli,
    ),
    *snapshot_point_metrics(
        "commitment_snapshot",
        "commitment-snapshot point lookup candidate",
        snapshot_point_commitment_snapshot_segments,
        snapshot_point_commitment_snapshot_bytes,
        snapshot_point_commitment_snapshot_payload_bytes,
        snapshot_point_commitment_snapshot_sidecar_bytes,
        snapshot_point_commitment_snapshot_sidecar_share_milli,
        snapshot_point_commitment_snapshot_share_milli,
    ),
    ("gtron_storage_benchmark_archive_api_checks", "Benchmark historical archive API probe check count.", archive_api_checks),
    ("gtron_storage_benchmark_archive_api_block", "Benchmark historical archive API probe block number.", archive_api_block),
    ("gtron_storage_benchmark_archive_api_depth_blocks", "Benchmark historical archive API probe depth below sampled head.", archive_api_depth_blocks),
    ("gtron_storage_benchmark_archive_api_failures", "Benchmark historical archive API probe failures.", archive_api_failures),
    ("gtron_storage_benchmark_cold_freezer_to_block", "Highest block covered by benchmark cold freezer segments.", cold_freezer_to_block),
    ("gtron_storage_benchmark_derived_index_to_block", "Highest block covered by benchmark derived indexes.", derived_index_to_block),
    ("gtron_storage_benchmark_chain_lookup_prune_to_block", "Highest block whose hot chain lookup rows were pruned.", chain_lookup_prune_to_block),
    ("gtron_storage_benchmark_tail_pruned_through_block", "Highest active ancient block physically tail-pruned.", tail_pruned_through_block),
    ("gtron_storage_benchmark_balance_trace_prune_to_block", "Highest block whose hot balance trace rows were pruned.", balance_trace_prune_to_block),
    ("gtron_storage_benchmark_section_bloom_prune_to_section", "Highest section whose hot bloom rows were pruned.", section_bloom_prune_to_section),
    ("gtron_storage_benchmark_signed_cold_prune", "Whether benchmark ran signed cold-prune operations.", signed_cold_prune),
    ("gtron_storage_benchmark_tail_pruned_files", "Benchmark active ancient tail files physically pruned.", tail_pruned_files),
    ("gtron_storage_benchmark_history_window", "Configured benchmark history prune window.", history_window),
    ("gtron_storage_benchmark_event_log_index_segments", "Benchmark cold event-log index segment count.", event_log_index_segments),
    ("gtron_storage_benchmark_event_log_index_from_block", "Lowest block covered by benchmark cold event-log indexes.", event_log_index_from_block),
    ("gtron_storage_benchmark_event_log_index_to_block", "Highest block covered by benchmark cold event-log indexes.", event_log_index_to_block),
    ("gtron_storage_benchmark_event_log_index_address_keys", "Benchmark event-log address index key count.", event_log_index_address_keys),
    ("gtron_storage_benchmark_event_log_index_address_postings", "Benchmark event-log address index posting count.", event_log_index_address_postings),
    ("gtron_storage_benchmark_event_log_index_address_avg_postings_milli", "Benchmark event-log address index average postings per key in milli-units.", event_log_index_address_avg_postings_milli),
    ("gtron_storage_benchmark_event_log_index_address_max_postings", "Benchmark event-log address index max postings for one key.", event_log_index_address_max_postings),
    ("gtron_storage_benchmark_event_log_index_address_singleton_keys", "Benchmark event-log address index singleton-key count.", event_log_index_address_singleton_keys),
    ("gtron_storage_benchmark_event_log_index_address_multi_posting_keys", "Benchmark event-log address index multi-posting key count.", event_log_index_address_multi_posting_keys),
    ("gtron_storage_benchmark_event_log_index_topic_keys", "Benchmark event-log topic index key count.", event_log_index_topic_keys),
    ("gtron_storage_benchmark_event_log_index_topic_postings", "Benchmark event-log topic index posting count.", event_log_index_topic_postings),
    ("gtron_storage_benchmark_event_log_index_topic_avg_postings_milli", "Benchmark event-log topic index average postings per key in milli-units.", event_log_index_topic_avg_postings_milli),
    ("gtron_storage_benchmark_event_log_index_topic_max_postings", "Benchmark event-log topic index max postings for one key.", event_log_index_topic_max_postings),
    ("gtron_storage_benchmark_event_log_index_topic_singleton_keys", "Benchmark event-log topic index singleton-key count.", event_log_index_topic_singleton_keys),
    ("gtron_storage_benchmark_event_log_index_topic_multi_posting_keys", "Benchmark event-log topic index multi-posting key count.", event_log_index_topic_multi_posting_keys),
)
lines = []
for name, help_text, value in metrics:
    lines.append(f"# HELP {name} {help_text}")
    lines.append(f"# TYPE {name} gauge")
    lines.append(f"{name}{{{labels}}} {value}")
successful_archive_api_methods = method_set(archive_api_methods_raw)
successful_archive_api_tx_methods = method_set(archive_api_tx_methods_raw)
if archive_api_checks > 0 or successful_archive_api_methods:
    method_metric = "gtron_storage_benchmark_archive_api_method_success"
    lines.append(
        f"# HELP {method_metric} Whether an expected benchmark historical archive API method probe succeeded."
    )
    lines.append(f"# TYPE {method_metric} gauge")
    for method in archive_api_expected_methods(successful_archive_api_methods):
        value = 1 if method in successful_archive_api_methods else 0
        lines.append(f'{method_metric}{labels_with({"method": method})} {value:g}')
if archive_api_tx_probe:
    tx_metric = "gtron_storage_benchmark_archive_api_tx_method_success"
    lines.append(
        f"# HELP {tx_metric} Whether an expected benchmark transaction-level historical archive API method probe succeeded."
    )
    lines.append(f"# TYPE {tx_metric} gauge")
    expected_tx_methods = list(ARCHIVE_API_TX_METHODS)
    if archive_api_trace_transaction_probe or any(
        method in successful_archive_api_tx_methods
        for method in ARCHIVE_API_TRACE_TRANSACTION_METHODS
    ):
        expected_tx_methods.extend(ARCHIVE_API_TRACE_TRANSACTION_METHODS)
    for method in expected_tx_methods:
        value = 1 if method in successful_archive_api_tx_methods else 0
        lines.append(f'{tx_metric}{labels_with({"method": method})} {value:g}')
path.write_text("\n".join(lines) + "\n", encoding="utf-8")
PY
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
  local segments from_block to_block address_keys address_postings address_avg address_max address_singleton address_multi
  local topic_keys topic_postings topic_avg topic_max topic_singleton topic_multi
  segments="$(sed -n 's/^Event log index stats:.* segments=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  from_block="$(sed -n 's/^Event log index stats:.* fromBlock=\(-\{0,1\}[0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  to_block="$(sed -n 's/^Event log index stats:.* toBlock=\(-\{0,1\}[0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_keys="$(sed -n 's/^Event log index stats:.* addressKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_postings="$(sed -n 's/^Event log index stats:.* addressPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_avg="$(sed -n 's/^Event log index stats:.* addressAvgPostingsMilli=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_max="$(sed -n 's/^Event log index stats:.* addressMaxPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_singleton="$(sed -n 's/^Event log index stats:.* addressSingletonKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  address_multi="$(sed -n 's/^Event log index stats:.* addressMultiPostingKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_keys="$(sed -n 's/^Event log index stats:.* topicKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_postings="$(sed -n 's/^Event log index stats:.* topicPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_avg="$(sed -n 's/^Event log index stats:.* topicAvgPostingsMilli=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_max="$(sed -n 's/^Event log index stats:.* topicMaxPostings=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_singleton="$(sed -n 's/^Event log index stats:.* topicSingletonKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  topic_multi="$(sed -n 's/^Event log index stats:.* topicMultiPostingKeys=\([0-9][0-9]*\).*/\1/p' "$stats_out" | tail -1)"
  RUN_EVENT_LOG_INDEX_SEGMENTS="${segments:-0}"
  RUN_EVENT_LOG_INDEX_FROM_BLOCK="${from_block:--1}"
  RUN_EVENT_LOG_INDEX_TO_BLOCK="${to_block:--1}"
  RUN_EVENT_LOG_INDEX_ADDRESS_KEYS="${address_keys:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS="${address_postings:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_AVG_POSTINGS_MILLI="${address_avg:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS="${address_max:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_SINGLETON_KEYS="${address_singleton:-0}"
  RUN_EVENT_LOG_INDEX_ADDRESS_MULTI_POSTING_KEYS="${address_multi:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_KEYS="${topic_keys:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS="${topic_postings:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_AVG_POSTINGS_MILLI="${topic_avg:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS="${topic_max:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_SINGLETON_KEYS="${topic_singleton:-0}"
  RUN_EVENT_LOG_INDEX_TOPIC_MULTI_POSTING_KEYS="${topic_multi:-0}"
}

snapshot_manifest_profile_default_values() {
  local status="$1"
  local missing_share=-1
  printf '%s\n' "$status" 0 0 0 0 "$missing_share"
  for _ in latest state-history chain-freezer event-log balance-trace section-bloom; do
    printf '%s\n' 0 "$missing_share"
  done
  for _ in txHashLookup eventLogIndex stateHistoryAccessor latestBTree chainFreezerAccessor codeDomain commitmentSnapshot; do
    printf '%s\n' 0 0 0 0 "$missing_share" "$missing_share"
  done
  printf '%s\n' 0 0 0 "$missing_share"
}

snapshot_manifest_profile_values() {
  local snapshot_dir="$1"
  local log_path="$2"
  if [ ! -f "$snapshot_dir/manifest.json" ]; then
    snapshot_manifest_profile_default_values "missing"
    return
  fi
  if [ ! -x "$SNAPSHOT_PROFILE_SCRIPT" ]; then
    echo "warning: snapshot manifest profiler not executable: $SNAPSHOT_PROFILE_SCRIPT" >>"$log_path"
    snapshot_manifest_profile_default_values "error"
    return
  fi
  local profile_out="$WORKDIR/snapshot-profile-$(basename "$(dirname "$(dirname "$snapshot_dir")")").json"
  if ! "$SNAPSHOT_PROFILE_SCRIPT" "$snapshot_dir" --json --verify-files >"$profile_out" 2>>"$log_path"; then
    echo "warning: snapshot manifest profile failed for $snapshot_dir; see $log_path" >>"$log_path"
    snapshot_manifest_profile_default_values "error"
    return
  fi
  python3 - "$profile_out" <<'PY'
import json
import sys
from pathlib import Path

profile = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
families = profile.get("byFamily", {})
point_candidates = profile.get("pointIndexCandidates", {})

def family_value(name, key, default=0):
    value = families.get(name, {}).get(key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

def point_value(name, key, default=0):
    stats = point_candidates.get(name, {})
    if not isinstance(stats, dict):
        stats = {}
    value = stats.get(key, default)
    try:
        return int(value)
    except (TypeError, ValueError):
        return default

print("ok")
print(int(profile.get("activeSegments", 0)))
print(int(profile.get("totalBytes", 0)))
print(int(profile.get("payloadBytes", 0)))
print(int(profile.get("sidecarBytes", 0)))
print(int(profile.get("sidecarShareMilli", 0)))
for family in (
    "latest",
    "state-history",
    "chain-freezer",
    "event-log",
    "balance-trace",
    "section-bloom",
):
    print(family_value(family, "sidecarBytes"))
    print(family_value(family, "sidecarShareMilli", -1))
for candidate in (
    "txHashLookup",
    "eventLogIndex",
    "stateHistoryAccessor",
    "latestBTree",
    "chainFreezerAccessor",
    "codeDomain",
    "commitmentSnapshot",
):
    print(point_value(candidate, "segments"))
    print(point_value(candidate, "totalBytes"))
    print(point_value(candidate, "payloadBytes"))
    print(point_value(candidate, "sidecarBytes"))
    print(point_value(candidate, "sidecarShareMilli", -1))
    print(point_value(candidate, "snapshotShareMilli", -1))
state_history = families.get("state-history", {})
if not isinstance(state_history, dict):
    state_history = {}
print(family_value("state-history", "totalBytes"))
print(family_value("state-history", "compressedSegments"))
print(family_value("state-history", "compressedBytes"))
print(family_value("state-history", "compressedShareMilli", -1))
PY
}

archive_api_probe_values() {
  local endpoint="$1"
  local block="$2"
  local address="$3"
  local slot="$4"
  local call_data="$5"
  local trace_transaction="$6"
  local trace_block="$7"
  python3 - "$endpoint" "$block" "$address" "$slot" "$call_data" "$trace_transaction" "$trace_block" <<'PY'
import json
import subprocess
import sys

endpoint = sys.argv[1]
block = int(sys.argv[2])
address = sys.argv[3]
slot = sys.argv[4]
call_data = sys.argv[5]
trace_transaction = sys.argv[6] == "1"
trace_block = sys.argv[7] == "1"
block_tag = hex(block)

def rpc_call(request_id, method, params):
    payload = json.dumps(
        {"jsonrpc": "2.0", "id": request_id, "method": method, "params": params},
        separators=(",", ":"),
    )
    try:
        proc = subprocess.run(
            [
                "curl",
                "-sf",
                "--max-time",
                "5",
                "-H",
                "Content-Type: application/json",
                "--data-binary",
                payload,
                endpoint,
            ],
            text=True,
            capture_output=True,
            check=False,
        )
    except OSError:
        return None, False
    if proc.returncode != 0:
        return None, False
    try:
        response = json.loads(proc.stdout)
    except json.JSONDecodeError:
        return None, False
    if response.get("error") is not None or "result" not in response:
        return None, False
    return response.get("result"), True

def normalize_hash(value):
    if not isinstance(value, str) or not value:
        return ""
    if not value.startswith("0x"):
        value = "0x" + value
    return value.lower()

def hex_quantity(value):
    try:
        return int(str(value), 16)
    except Exception:
        return None

def is_hex_string(value):
    return (
        isinstance(value, str)
        and value.startswith("0x")
        and all(ch in "0123456789abcdefABCDEF" for ch in value[2:])
    )

selected_block_hash = ""
selected_tx_hash = ""

def trace_result_ok(result):
    return (
        isinstance(result, dict)
        and any(key in result for key in ("structLogs", "returnValue", "type", "calls"))
    )

def trace_block_result_ok(result):
    if not isinstance(result, list):
        return False
    for entry in result:
        if not isinstance(entry, dict):
            return False
        tx_hash = entry.get("txHash") or entry.get("transactionHash")
        if tx_hash is not None and not normalize_hash(tx_hash):
            return False
        if "result" in entry:
            if not trace_result_ok(entry.get("result")):
                return False
            continue
        if "error" in entry:
            return isinstance(entry.get("error"), str) and bool(entry.get("error"))
        return False
    return True

def archive_result_ok(method, result, params):
    if method == "eth_getBlockByNumber":
        if not isinstance(result, dict):
            return False
        result_number = hex_quantity(result.get("number"))
        requested_number = hex_quantity(params[0] if params else None)
        return result_number is not None and result_number == requested_number
    if method == "eth_getBlockByHash":
        if not isinstance(result, dict):
            return False
        result_hash = normalize_hash(result.get("hash") or result.get("blockHash"))
        requested_hash = normalize_hash(params[0] if params else "")
        result_number = hex_quantity(result.get("number"))
        requested_number = hex_quantity(block_tag)
        return (
            result_hash == requested_hash
            and result_number is not None
            and result_number == requested_number
        )
    if method in {
        "eth_getBlockTransactionCountByNumber",
        "eth_getBlockTransactionCountByHash",
        "eth_getUncleCountByBlockNumber",
        "eth_getUncleCountByBlockHash",
    }:
        count = hex_quantity(result)
        return is_hex_string(result) and count is not None
    if method in {"eth_getUncleByBlockNumberAndIndex", "eth_getUncleByBlockHashAndIndex"}:
        return result is None
    if method == "eth_getBlockReceipts":
        if not isinstance(result, list):
            return False
        requested_number = hex_quantity(params[0] if params else None)
        selected_receipt_seen = False
        for receipt in result:
            if not isinstance(receipt, dict):
                return False
            result_number = hex_quantity(receipt.get("blockNumber"))
            if result_number is None or result_number != requested_number:
                return False
            if selected_block_hash and normalize_hash(receipt.get("blockHash")) != selected_block_hash:
                return False
            receipt_tx_hash = normalize_hash(receipt.get("transactionHash") or receipt.get("hash"))
            if selected_tx_hash and receipt_tx_hash == selected_tx_hash:
                selected_receipt_seen = True
        if selected_tx_hash and not selected_receipt_seen:
            return False
        return True
    if method in {"eth_getBalance", "eth_getCode", "eth_getStorageAt", "eth_call", "eth_estimateGas"}:
        return is_hex_string(result)
    if method in {"debug_traceCall", "debug_traceTransaction"}:
        return trace_result_ok(result)
    if method in {"debug_traceBlockByNumber", "debug_traceBlockByHash"}:
        return trace_block_result_ok(result)
    if method == "eth_getLogs":
        if not isinstance(result, list):
            return False
        requested_number = hex_quantity(params[0].get("fromBlock") if params else None)
        for log in result:
            if not isinstance(log, dict):
                return False
            if hex_quantity(log.get("blockNumber")) != requested_number:
                return False
            if selected_block_hash and normalize_hash(log.get("blockHash")) != selected_block_hash:
                return False
        return True
    if method == "eth_getTransactionByHash":
        return (
            isinstance(result, dict)
            and normalize_hash(result.get("hash") or result.get("transactionHash")) == normalize_hash(params[0])
            and hex_quantity(result.get("blockNumber")) == hex_quantity(block_tag)
            and (
                not selected_block_hash
                or normalize_hash(result.get("blockHash")) == selected_block_hash
            )
        )
    if method == "eth_getTransactionReceipt":
        return (
            isinstance(result, dict)
            and normalize_hash(result.get("transactionHash") or result.get("hash")) == normalize_hash(params[0])
            and hex_quantity(result.get("blockNumber")) == hex_quantity(block_tag)
            and (
                not selected_block_hash
                or normalize_hash(result.get("blockHash")) == selected_block_hash
            )
        )
    if method == "eth_getTransactionByBlockNumberAndIndex":
        if not isinstance(result, dict):
            return False
        result_hash = normalize_hash(result.get("hash") or result.get("transactionHash"))
        result_number = hex_quantity(result.get("blockNumber"))
        requested_number = hex_quantity(params[0] if params else None)
        result_index = hex_quantity(result.get("transactionIndex"))
        requested_index = hex_quantity(params[1] if len(params) > 1 else None)
        return (
            (not selected_tx_hash or result_hash == selected_tx_hash)
            and result_number is not None
            and result_number == requested_number
            and result_index is not None
            and result_index == requested_index
        )
    if method == "eth_getTransactionByBlockHashAndIndex":
        if not isinstance(result, dict):
            return False
        result_hash = normalize_hash(result.get("hash") or result.get("transactionHash"))
        result_block_hash = normalize_hash(result.get("blockHash"))
        result_index = hex_quantity(result.get("transactionIndex"))
        requested_index = hex_quantity(params[1] if len(params) > 1 else None)
        return (
            (not selected_tx_hash or result_hash == selected_tx_hash)
            and result_block_hash == normalize_hash(params[0] if params else "")
            and result_index is not None
            and result_index == requested_index
        )
    return result is not None

def block_hash(block_result):
    if not isinstance(block_result, dict):
        return ""
    return normalize_hash(block_result.get("hash") or block_result.get("blockHash"))

def first_tx_hash(block_result):
    if not isinstance(block_result, dict):
        return ""
    txs = block_result.get("transactions")
    if not isinstance(txs, list) or not txs:
        return ""
    tx = txs[0]
    if isinstance(tx, str):
        tx_hash = tx
    elif isinstance(tx, dict):
        tx_hash = tx.get("hash") or tx.get("transactionHash") or ""
    else:
        return ""
    if not isinstance(tx_hash, str) or not tx_hash:
        return ""
    if not tx_hash.startswith("0x"):
        tx_hash = "0x" + tx_hash
    return tx_hash

calls = [
    ("eth_getBlockByNumber", [block_tag, False]),
    ("eth_getBlockTransactionCountByNumber", [block_tag]),
    ("eth_getUncleCountByBlockNumber", [block_tag]),
    ("eth_getUncleByBlockNumberAndIndex", [block_tag, "0x0"]),
    ("eth_getBlockReceipts", [block_tag]),
    ("eth_getBalance", [address, block_tag]),
    ("eth_getCode", [address, block_tag]),
    ("eth_getStorageAt", [address, slot, block_tag]),
    ("eth_getLogs", [{"fromBlock": block_tag, "toBlock": block_tag}]),
]
if call_data:
    calls[7:7] = [
        ("eth_call", [{"to": address, "data": call_data}, block_tag]),
        ("debug_traceCall", [{"to": address, "data": call_data}, block_tag, {}]),
        ("eth_estimateGas", [{"to": address, "data": call_data}, block_tag]),
    ]

methods = []
tx_methods = []
tx_probe = False
tx_hash_value = ""
failures = 0
idx = 0
while idx < len(calls):
    method, params = calls[idx]
    result, ok = rpc_call(idx + 1, method, params)
    if not ok or not archive_result_ok(method, result, params):
        failures += 1
        idx += 1
        continue
    methods.append(method)
    if method == "eth_getBlockByNumber":
        selected_block_hash = block_hash(result)
        if selected_block_hash:
            calls.append(("eth_getBlockByHash", [selected_block_hash, False]))
            calls.append(("eth_getBlockTransactionCountByHash", [selected_block_hash]))
            calls.append(("eth_getUncleCountByBlockHash", [selected_block_hash]))
            calls.append(("eth_getUncleByBlockHashAndIndex", [selected_block_hash, "0x0"]))
        if trace_block:
            calls.append(("debug_traceBlockByNumber", [block_tag, {}]))
            calls.append(("debug_traceBlockByHash", [selected_block_hash, {}]))
        tx_hash = first_tx_hash(result)
        if tx_hash:
            tx_probe = True
            tx_hash_value = tx_hash
            selected_tx_hash = normalize_hash(tx_hash)
            calls.append(("eth_getTransactionByHash", [tx_hash]))
            calls.append(("eth_getTransactionReceipt", [tx_hash]))
            calls.append(("eth_getTransactionByBlockNumberAndIndex", [block_tag, "0x0"]))
            calls.append(("eth_getTransactionByBlockHashAndIndex", [selected_block_hash, "0x0"]))
            if trace_transaction:
                calls.append(("debug_traceTransaction", [tx_hash, {}]))
    elif method in {
        "eth_getTransactionByHash",
        "eth_getTransactionReceipt",
        "eth_getTransactionByBlockNumberAndIndex",
        "eth_getTransactionByBlockHashAndIndex",
        "debug_traceTransaction",
    }:
        tx_methods.append(method)
    idx += 1

print("ok" if failures == 0 else "failed")
print(len(calls))
print(failures)
print(block)
print(json.dumps(methods, separators=(",", ":")))
print("true" if tx_probe else "false")
print(tx_hash_value)
print(json.dumps(tx_methods, separators=(",", ":")))
PY
}

run_archive_api_probe() {
  local jrpc_port="$1"
  local height="$2"
  local log_path="$3"
  if [ "$ARCHIVE_API_PROBE" -ne 1 ]; then
    return
  fi
  local probe_block="$ARCHIVE_API_BLOCK"
  if [ "$probe_block" -lt 0 ]; then
    if [ "$height" -gt 0 ]; then
      probe_block=$((height - 1))
    else
      probe_block=0
    fi
  fi
  local values
  echo "probing archive JSON-RPC APIs at block $probe_block" >>"$log_path"
  values="$(archive_api_probe_values "http://127.0.0.1:$jrpc_port" "$probe_block" "$ARCHIVE_API_ADDRESS" "$ARCHIVE_API_STORAGE_SLOT" "$ARCHIVE_API_CALL_DATA" "$ARCHIVE_API_TRACE_TRANSACTION" "$ARCHIVE_API_TRACE_BLOCK")"
  RUN_ARCHIVE_API_STATUS="$(printf '%s\n' "$values" | sed -n '1p')"
  RUN_ARCHIVE_API_CHECKS="$(printf '%s\n' "$values" | sed -n '2p')"
  RUN_ARCHIVE_API_FAILURES="$(printf '%s\n' "$values" | sed -n '3p')"
  RUN_ARCHIVE_API_BLOCK="$(printf '%s\n' "$values" | sed -n '4p')"
  if [ "$height" -ge "$RUN_ARCHIVE_API_BLOCK" ] 2>/dev/null; then
    RUN_ARCHIVE_API_DEPTH_BLOCKS=$((height - RUN_ARCHIVE_API_BLOCK))
  else
    RUN_ARCHIVE_API_DEPTH_BLOCKS=-1
  fi
  RUN_ARCHIVE_API_METHODS="$(printf '%s\n' "$values" | sed -n '5p')"
  RUN_ARCHIVE_API_TX_PROBE="$(printf '%s\n' "$values" | sed -n '6p')"
  RUN_ARCHIVE_API_TX_HASH="$(printf '%s\n' "$values" | sed -n '7p')"
  RUN_ARCHIVE_API_TX_METHODS="$(printf '%s\n' "$values" | sed -n '8p')"
  echo "archive API probe status=$RUN_ARCHIVE_API_STATUS checks=$RUN_ARCHIVE_API_CHECKS failures=$RUN_ARCHIVE_API_FAILURES block=$RUN_ARCHIVE_API_BLOCK depth=$RUN_ARCHIVE_API_DEPTH_BLOCKS methods=$RUN_ARCHIVE_API_METHODS txProbe=$RUN_ARCHIVE_API_TX_PROBE txMethods=$RUN_ARCHIVE_API_TX_METHODS" >>"$log_path"
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
mode = []
snapshot = []
for line in text.splitlines():
    stripped = line.strip()
    if not stripped.startswith("{"):
        continue
    try:
        obj = json.loads(stripped)
    except Exception:
        continue
    if not isinstance(obj, dict):
        continue
    freezer = obj.get("freezerAlertDetails", [])
    stage = obj.get("stageVerifyDetails", [])
    mode = obj.get("modeAlertDetails", [])
    snapshot = obj.get("snapshotAlertDetails", [])
    print(json.dumps(freezer if isinstance(freezer, list) else [], separators=(",", ":")))
    print(json.dumps(stage if isinstance(stage, list) else [], separators=(",", ":")))
    print(json.dumps(mode if isinstance(mode, list) else [], separators=(",", ":")))
    print(json.dumps(snapshot if isinstance(snapshot, list) else [], separators=(",", ":")))
    raise SystemExit(0)
for line in text.splitlines():
    m = re.match(r"Storage freezer alert: severity=([^ ]+) kind=([^ ]+) detail=(.*)$", line)
    if m:
        freezer.append({"severity": m.group(1), "kind": m.group(2), "detail": m.group(3)})
        continue
    m = re.match(r"Storage stage alert: severity=([^ ]+) kind=([^ ]+) detail=(.*)$", line)
    if m:
        stage.append({"severity": m.group(1), "kind": m.group(2), "detail": m.group(3)})
        continue
    m = re.match(r"Storage stage alert: severity=([^ ]+) detail=(.*)$", line)
    if m:
        stage.append({"severity": m.group(1), "detail": m.group(2)})
        continue
    m = re.match(r"Storage mode alert: severity=([^ ]+) kind=([^ ]+) detail=(.*)$", line)
    if m:
        mode.append({"severity": m.group(1), "kind": m.group(2), "detail": m.group(3)})
        continue
    m = re.match(r"Storage snapshot alert: severity=([^ ]+) kind=([^ ]+) detail=(.*)$", line)
    if m:
        snapshot.append({"severity": m.group(1), "kind": m.group(2), "detail": m.group(3)})
print(json.dumps(freezer, separators=(",", ":")))
print(json.dumps(stage, separators=(",", ":")))
print(json.dumps(mode, separators=(",", ":")))
print(json.dumps(snapshot, separators=(",", ":")))
PY
}

storage_alert_field() {
  local alert_out="$1"
  local key="$2"
  local pattern="$3"
  python3 - "$alert_out" "$key" "$pattern" <<'PY'
import json
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")
key = sys.argv[2]
pattern = sys.argv[3]
for line in text.splitlines():
    stripped = line.strip()
    if not stripped.startswith("{"):
        continue
    try:
        obj = json.loads(stripped)
    except Exception:
        continue
    if isinstance(obj, dict) and key in obj:
        print(obj[key])
        raise SystemExit(0)
found = re.findall(pattern, text)
if found:
    print(found[-1])
PY
}

storage_alert_pipeline_values() {
  local alert_out="$1"
  python3 - "$alert_out" <<'PY'
import json
import re
import sys
from pathlib import Path

text = Path(sys.argv[1]).read_text(encoding="utf-8", errors="replace")


def as_int(value, default=-1):
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def as_bool_text(value):
    return "true" if str(value).lower() in {"1", "true", "yes"} else "false"


row = {
    "complete": "false",
    "pending": -1,
    "issues": -1,
    "next": "",
    "nextStatus": "",
    "nextTarget": -1,
    "nextUpstream": "",
    "nextCurrent": -1,
    "tasks": [],
}

for line in text.splitlines():
    stripped = line.strip()
    if not stripped.startswith("{"):
        continue
    try:
        obj = json.loads(stripped)
    except Exception:
        continue
    pipeline = obj.get("stagePipeline") if isinstance(obj, dict) else None
    if not isinstance(pipeline, dict):
        continue
    row["complete"] = as_bool_text(pipeline.get("complete", False))
    row["pending"] = as_int(pipeline.get("pending", -1))
    row["issues"] = as_int(pipeline.get("issues", -1))
    tasks = []
    for item in pipeline.get("tasks", []) or []:
        if not isinstance(item, dict):
            continue
        task = {
            "stage": str(item.get("stage", "")),
            "upstream": str(item.get("upstream", "")),
            "status": str(item.get("status", "")),
            "targetValue": as_int(item.get("targetValue", -1)),
            "targetHash": str(item.get("targetHash", "")),
            "currentValue": as_int(item.get("currentValue", -1)),
            "currentHash": str(item.get("currentHash", "")),
        }
        tasks.append(task)
    row["tasks"] = tasks
    if tasks:
        first = tasks[0]
        row["next"] = first.get("stage", "")
        row["nextStatus"] = first.get("status", "")
        row["nextTarget"] = first.get("targetValue", -1)
        row["nextUpstream"] = first.get("upstream", "")
        row["nextCurrent"] = first.get("currentValue", -1)
    break
else:
    patterns = {
        "complete": r"stagePipelineComplete=([^ ]+)",
        "pending": r"stagePipelinePending=([0-9]+)",
        "issues": r"stagePipelineIssues=([0-9]+)",
        "next": r"stagePipelineNext=([^ ]+)",
        "nextStatus": r"stagePipelineNextStatus=([^ ]+)",
        "nextTarget": r"stagePipelineNextTarget=([0-9]+)",
        "nextUpstream": r"stagePipelineNextUpstream=([^ ]+)",
        "nextCurrent": r"stagePipelineNextCurrent=([0-9]+)",
    }
    found = {}
    for key, pattern in patterns.items():
        matches = re.findall(pattern, text)
        if matches:
            found[key] = matches[-1]
    if "complete" in found:
        row["complete"] = as_bool_text(found["complete"])
    if "pending" in found:
        row["pending"] = as_int(found["pending"], -1)
    if "issues" in found:
        row["issues"] = as_int(found["issues"], -1)
    if "next" in found:
        row["next"] = found["next"]
    if "nextStatus" in found:
        row["nextStatus"] = found["nextStatus"]
    if "nextTarget" in found:
        row["nextTarget"] = as_int(found["nextTarget"], -1)
    if "nextUpstream" in found:
        row["nextUpstream"] = found["nextUpstream"]
    if "nextCurrent" in found:
        row["nextCurrent"] = as_int(found["nextCurrent"], -1)

print(row["complete"])
print(row["pending"])
print(row["issues"])
print(row["next"])
print(row["nextStatus"])
print(row["nextTarget"])
print(row["nextUpstream"])
print(row["nextCurrent"])
print(json.dumps(row["tasks"], separators=(",", ":")))
PY
}

run_storage_alert_gate() {
  local mode="$1"
  local role="$2"
  local datadir="$3"
  local log_path="$4"
  local alert_out="$WORKDIR/$mode-$role-storage-alerts.out"
  local alert_prometheus="$ARTIFACT_DIR/$mode-$role-storage-alerts.prom"
  echo "checking persisted storage alert conditions" >>"$log_path"
  local ok=1
  if ! run_logged "$alert_out" "$GTRON" db storage-alerts --json --datadir "$datadir" >>"$log_path"; then
    ok=0
  fi
  echo "writing persisted storage alert prometheus metrics: $alert_prometheus" >>"$log_path"
  if "$GTRON" db storage-alerts --prometheus --datadir "$datadir" >"$alert_prometheus" 2>&1; then
    RUN_STORAGE_ALERT_PROMETHEUS="$alert_prometheus"
  elif grep -q '^gtron_storage_alert_status{' "$alert_prometheus"; then
    # Critical storage states intentionally return non-zero after writing metrics.
    RUN_STORAGE_ALERT_PROMETHEUS="$alert_prometheus"
  else
    echo "warning: storage-alerts prometheus metrics failed; see $alert_prometheus" >>"$log_path"
  fi
  local alert_status freezer_status freezer_issues hidden stage_status stage_issues mode_status mode_issues prune_mode prune_mode_persisted snapshot_status snapshot_issues retired_segments retired_files retired_missing retired_skipped retired_bytes
  alert_status="$(storage_alert_field "$alert_out" status 'status=([^ ]+)')"
  freezer_status="$(storage_alert_field "$alert_out" freezerStatus 'freezerStatus=([^ ]+)')"
  freezer_issues="$(storage_alert_field "$alert_out" freezerIssues 'freezerIssues=([0-9]+)')"
  hidden="$(storage_alert_field "$alert_out" freezerAlertHiddenBytes 'hiddenSize=([0-9]+)')"
  stage_status="$(storage_alert_field "$alert_out" stageStatus 'stageStatus=([^ ]+)')"
  stage_issues="$(storage_alert_field "$alert_out" stageIssues 'stageIssues=([0-9]+)')"
  mode_status="$(storage_alert_field "$alert_out" modeStatus 'modeStatus=([^ ]+)')"
  mode_issues="$(storage_alert_field "$alert_out" modeIssues 'modeIssues=([0-9]+)')"
  prune_mode="$(storage_alert_field "$alert_out" pruneMode 'pruneMode=([^ ]+)')"
  prune_mode_persisted="$(storage_alert_field "$alert_out" pruneModePersisted 'pruneModePersisted=([^ ]+)')"
  snapshot_status="$(storage_alert_field "$alert_out" snapshotStatus 'snapshotStatus=([^ ]+)')"
  snapshot_issues="$(storage_alert_field "$alert_out" snapshotIssues 'snapshotIssues=([0-9]+)')"
  retired_segments="$(storage_alert_field "$alert_out" snapshotRetiredSegments 'retiredSegments=([0-9]+)')"
  retired_files="$(storage_alert_field "$alert_out" snapshotRetiredFiles 'retiredFiles=([0-9]+)')"
  retired_missing="$(storage_alert_field "$alert_out" snapshotRetiredMissing 'retiredMissing=([0-9]+)')"
  retired_skipped="$(storage_alert_field "$alert_out" snapshotRetiredSkippedActive 'retiredSkippedActive=([0-9]+)')"
  retired_bytes="$(storage_alert_field "$alert_out" snapshotRetiredBytes 'retiredBytes=([0-9]+)')"
  RUN_STORAGE_ALERT_STATUS="${alert_status:-unknown}"
  RUN_FREEZER_ALERT_STATUS="${freezer_status:-unknown}"
  RUN_FREEZER_ALERT_ISSUES="${freezer_issues:--1}"
  RUN_FREEZER_ALERT_HIDDEN_BYTES="${hidden:--1}"
  RUN_STAGE_VERIFY_STATUS="${stage_status:-unknown}"
  RUN_STAGE_VERIFY_ISSUES="${stage_issues:--1}"
  RUN_MODE_ALERT_STATUS="${mode_status:-unknown}"
  RUN_MODE_ALERT_ISSUES="${mode_issues:--1}"
  RUN_PRUNE_MODE="${prune_mode:-unknown}"
  RUN_PRUNE_MODE_PERSISTED="${prune_mode_persisted:-false}"
  RUN_PRUNE_MODE_PERSISTED="$(printf '%s' "$RUN_PRUNE_MODE_PERSISTED" | tr '[:upper:]' '[:lower:]')"
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
  RUN_MODE_ALERT_DETAILS="$(printf '%s\n' "$detail_json" | sed -n '3p')"
  RUN_SNAPSHOT_ALERT_DETAILS="$(printf '%s\n' "$detail_json" | sed -n '4p')"
  local pipeline_values
  pipeline_values="$(storage_alert_pipeline_values "$alert_out")"
  RUN_STAGE_ALERT_PIPELINE_COMPLETE="$(printf '%s\n' "$pipeline_values" | sed -n '1p')"
  RUN_STAGE_ALERT_PIPELINE_PENDING="$(printf '%s\n' "$pipeline_values" | sed -n '2p')"
  RUN_STAGE_ALERT_PIPELINE_ISSUES="$(printf '%s\n' "$pipeline_values" | sed -n '3p')"
  RUN_STAGE_ALERT_PIPELINE_NEXT="$(printf '%s\n' "$pipeline_values" | sed -n '4p')"
  RUN_STAGE_ALERT_PIPELINE_NEXT_STATUS="$(printf '%s\n' "$pipeline_values" | sed -n '5p')"
  RUN_STAGE_ALERT_PIPELINE_NEXT_TARGET="$(printf '%s\n' "$pipeline_values" | sed -n '6p')"
  RUN_STAGE_ALERT_PIPELINE_NEXT_UPSTREAM="$(printf '%s\n' "$pipeline_values" | sed -n '7p')"
  RUN_STAGE_ALERT_PIPELINE_NEXT_CURRENT="$(printf '%s\n' "$pipeline_values" | sed -n '8p')"
  RUN_STAGE_ALERT_PIPELINE_TASKS="$(printf '%s\n' "$pipeline_values" | sed -n '9p')"
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
  if [ "$mode" = "archive" ]; then
    echo "skipping signed cold prune for archive mode" >>"$log_path"
    return
  fi
  if [ "$RUN_COLD_FREEZER_TO_BLOCK" -lt 0 ]; then
    die "signed cold prune requested but no cold freezer snapshot was built"
  fi
  RUN_SIGNED_COLD_PRUNE=1

  local signing_key_file="$WORKDIR/$mode-snapshot-signing-key.hex"
  local old_umask
  old_umask="$(umask)"
  umask 077
  printf '%s\n' "$SNAPSHOT_SIGNING_SEED" >"$signing_key_file"
  umask "$old_umask"

  local publish_out="$WORKDIR/$mode-publish-catalog.out"
  echo "publishing signed snapshot catalog" >>"$log_path"
  if ! run_logged "$publish_out" "$GTRON" snapshot publish-catalog \
    --datadir "$datadir" \
    --snapshot.signing-key-file "$signing_key_file" >>"$log_path"; then
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
  RUN_RETIRED_PRUNE_RAN="true"
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
    run_archive_api_probe "$((port_base + 13))" "$TARGET_BLOCKS" "$restart_log"
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
  local derived_index_values derived_index_bytes derived_index_files
  total="$(size_bytes "$datadir")"
  chain="$(size_bytes "$datadir/gtron/chaindata")"
  ancient="$(size_bytes "$datadir/gtron/ancient")"
  snapshots="$(size_bytes "$datadir/gtron/state-snapshots")"
  ancient_files="$(file_count "$datadir/gtron/ancient")"
  snapshot_files="$(file_count "$datadir/gtron/state-snapshots")"
  derived_index_values="$(derived_index_stats "$datadir/gtron/state-snapshots")"
  derived_index_bytes="$(printf '%s\n' "$derived_index_values" | sed -n '1p')"
  derived_index_files="$(printf '%s\n' "$derived_index_values" | sed -n '2p')"
  local profile_values
  profile_values="$(snapshot_manifest_profile_values "$datadir/gtron/state-snapshots" "$log_path")"
  local snapshot_profile_status snapshot_profile_segments snapshot_profile_total_bytes snapshot_payload_bytes snapshot_sidecar_bytes snapshot_sidecar_share_milli
  local snapshot_latest_sidecar_bytes snapshot_latest_sidecar_share_milli
  local snapshot_state_history_sidecar_bytes snapshot_state_history_sidecar_share_milli
  local snapshot_state_history_bytes snapshot_state_history_compressed_segments snapshot_state_history_compressed_bytes snapshot_state_history_compressed_share_milli
  local snapshot_chain_freezer_sidecar_bytes snapshot_chain_freezer_sidecar_share_milli
  local snapshot_event_log_sidecar_bytes snapshot_event_log_sidecar_share_milli
  local snapshot_balance_trace_sidecar_bytes snapshot_balance_trace_sidecar_share_milli
  local snapshot_section_bloom_sidecar_bytes snapshot_section_bloom_sidecar_share_milli
  local snapshot_point_tx_hash_lookup_segments snapshot_point_tx_hash_lookup_bytes snapshot_point_tx_hash_lookup_payload_bytes snapshot_point_tx_hash_lookup_sidecar_bytes snapshot_point_tx_hash_lookup_sidecar_share_milli snapshot_point_tx_hash_lookup_share_milli
  local snapshot_point_event_log_index_segments snapshot_point_event_log_index_bytes snapshot_point_event_log_index_payload_bytes snapshot_point_event_log_index_sidecar_bytes snapshot_point_event_log_index_sidecar_share_milli snapshot_point_event_log_index_share_milli
  local snapshot_point_state_history_accessor_segments snapshot_point_state_history_accessor_bytes snapshot_point_state_history_accessor_payload_bytes snapshot_point_state_history_accessor_sidecar_bytes snapshot_point_state_history_accessor_sidecar_share_milli snapshot_point_state_history_accessor_share_milli
  local snapshot_point_latest_btree_segments snapshot_point_latest_btree_bytes snapshot_point_latest_btree_payload_bytes snapshot_point_latest_btree_sidecar_bytes snapshot_point_latest_btree_sidecar_share_milli snapshot_point_latest_btree_share_milli
  local snapshot_point_chain_freezer_accessor_segments snapshot_point_chain_freezer_accessor_bytes snapshot_point_chain_freezer_accessor_payload_bytes snapshot_point_chain_freezer_accessor_sidecar_bytes snapshot_point_chain_freezer_accessor_sidecar_share_milli snapshot_point_chain_freezer_accessor_share_milli
  local snapshot_point_code_domain_segments snapshot_point_code_domain_bytes snapshot_point_code_domain_payload_bytes snapshot_point_code_domain_sidecar_bytes snapshot_point_code_domain_sidecar_share_milli snapshot_point_code_domain_share_milli
  local snapshot_point_commitment_snapshot_segments snapshot_point_commitment_snapshot_bytes snapshot_point_commitment_snapshot_payload_bytes snapshot_point_commitment_snapshot_sidecar_bytes snapshot_point_commitment_snapshot_sidecar_share_milli snapshot_point_commitment_snapshot_share_milli
  snapshot_profile_status="$(printf '%s\n' "$profile_values" | sed -n '1p')"
  snapshot_profile_segments="$(printf '%s\n' "$profile_values" | sed -n '2p')"
  snapshot_profile_total_bytes="$(printf '%s\n' "$profile_values" | sed -n '3p')"
  snapshot_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '4p')"
  snapshot_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '5p')"
  snapshot_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '6p')"
  local snapshot_profile_verify_files="false" snapshot_profile_verified_segments=0
  if [ "$snapshot_profile_status" = "ok" ]; then
    snapshot_profile_verify_files="true"
    snapshot_profile_verified_segments="$snapshot_profile_segments"
  fi
  snapshot_latest_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '7p')"
  snapshot_latest_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '8p')"
  snapshot_state_history_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '9p')"
  snapshot_state_history_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '10p')"
  snapshot_chain_freezer_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '11p')"
  snapshot_chain_freezer_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '12p')"
  snapshot_event_log_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '13p')"
  snapshot_event_log_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '14p')"
  snapshot_balance_trace_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '15p')"
  snapshot_balance_trace_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '16p')"
  snapshot_section_bloom_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '17p')"
  snapshot_section_bloom_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '18p')"
  snapshot_point_tx_hash_lookup_segments="$(printf '%s\n' "$profile_values" | sed -n '19p')"
  snapshot_point_tx_hash_lookup_bytes="$(printf '%s\n' "$profile_values" | sed -n '20p')"
  snapshot_point_tx_hash_lookup_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '21p')"
  snapshot_point_tx_hash_lookup_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '22p')"
  snapshot_point_tx_hash_lookup_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '23p')"
  snapshot_point_tx_hash_lookup_share_milli="$(printf '%s\n' "$profile_values" | sed -n '24p')"
  snapshot_point_event_log_index_segments="$(printf '%s\n' "$profile_values" | sed -n '25p')"
  snapshot_point_event_log_index_bytes="$(printf '%s\n' "$profile_values" | sed -n '26p')"
  snapshot_point_event_log_index_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '27p')"
  snapshot_point_event_log_index_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '28p')"
  snapshot_point_event_log_index_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '29p')"
  snapshot_point_event_log_index_share_milli="$(printf '%s\n' "$profile_values" | sed -n '30p')"
  snapshot_point_state_history_accessor_segments="$(printf '%s\n' "$profile_values" | sed -n '31p')"
  snapshot_point_state_history_accessor_bytes="$(printf '%s\n' "$profile_values" | sed -n '32p')"
  snapshot_point_state_history_accessor_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '33p')"
  snapshot_point_state_history_accessor_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '34p')"
  snapshot_point_state_history_accessor_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '35p')"
  snapshot_point_state_history_accessor_share_milli="$(printf '%s\n' "$profile_values" | sed -n '36p')"
  snapshot_point_latest_btree_segments="$(printf '%s\n' "$profile_values" | sed -n '37p')"
  snapshot_point_latest_btree_bytes="$(printf '%s\n' "$profile_values" | sed -n '38p')"
  snapshot_point_latest_btree_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '39p')"
  snapshot_point_latest_btree_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '40p')"
  snapshot_point_latest_btree_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '41p')"
  snapshot_point_latest_btree_share_milli="$(printf '%s\n' "$profile_values" | sed -n '42p')"
  snapshot_point_chain_freezer_accessor_segments="$(printf '%s\n' "$profile_values" | sed -n '43p')"
  snapshot_point_chain_freezer_accessor_bytes="$(printf '%s\n' "$profile_values" | sed -n '44p')"
  snapshot_point_chain_freezer_accessor_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '45p')"
  snapshot_point_chain_freezer_accessor_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '46p')"
  snapshot_point_chain_freezer_accessor_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '47p')"
  snapshot_point_chain_freezer_accessor_share_milli="$(printf '%s\n' "$profile_values" | sed -n '48p')"
  snapshot_point_code_domain_segments="$(printf '%s\n' "$profile_values" | sed -n '49p')"
  snapshot_point_code_domain_bytes="$(printf '%s\n' "$profile_values" | sed -n '50p')"
  snapshot_point_code_domain_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '51p')"
  snapshot_point_code_domain_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '52p')"
  snapshot_point_code_domain_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '53p')"
  snapshot_point_code_domain_share_milli="$(printf '%s\n' "$profile_values" | sed -n '54p')"
  snapshot_point_commitment_snapshot_segments="$(printf '%s\n' "$profile_values" | sed -n '55p')"
  snapshot_point_commitment_snapshot_bytes="$(printf '%s\n' "$profile_values" | sed -n '56p')"
  snapshot_point_commitment_snapshot_payload_bytes="$(printf '%s\n' "$profile_values" | sed -n '57p')"
  snapshot_point_commitment_snapshot_sidecar_bytes="$(printf '%s\n' "$profile_values" | sed -n '58p')"
  snapshot_point_commitment_snapshot_sidecar_share_milli="$(printf '%s\n' "$profile_values" | sed -n '59p')"
  snapshot_point_commitment_snapshot_share_milli="$(printf '%s\n' "$profile_values" | sed -n '60p')"
  snapshot_state_history_bytes="$(printf '%s\n' "$profile_values" | sed -n '61p')"
  snapshot_state_history_compressed_segments="$(printf '%s\n' "$profile_values" | sed -n '62p')"
  snapshot_state_history_compressed_bytes="$(printf '%s\n' "$profile_values" | sed -n '63p')"
  snapshot_state_history_compressed_share_milli="$(printf '%s\n' "$profile_values" | sed -n '64p')"
  local benchmark_prometheus="$ARTIFACT_DIR/$mode-$role-storage-benchmark.prom"
  write_storage_benchmark_prometheus "$benchmark_prometheus" "$profile" "$mode" "$role" "$status" \
    "$height" "$elapsed" "$datadir" "$total" "$chain" "$ancient" "$snapshots" \
    "$derived_index_bytes" "$snapshot_sidecar_share_milli" \
    "$snapshot_point_tx_hash_lookup_segments" "$snapshot_point_tx_hash_lookup_bytes" \
    "$snapshot_point_tx_hash_lookup_payload_bytes" "$snapshot_point_tx_hash_lookup_sidecar_bytes" \
    "$snapshot_point_tx_hash_lookup_sidecar_share_milli" "$snapshot_point_tx_hash_lookup_share_milli" \
    "$snapshot_point_event_log_index_segments" "$snapshot_point_event_log_index_bytes" \
    "$snapshot_point_event_log_index_payload_bytes" "$snapshot_point_event_log_index_sidecar_bytes" \
    "$snapshot_point_event_log_index_sidecar_share_milli" "$snapshot_point_event_log_index_share_milli" \
    "$snapshot_point_state_history_accessor_segments" "$snapshot_point_state_history_accessor_bytes" \
    "$snapshot_point_state_history_accessor_payload_bytes" "$snapshot_point_state_history_accessor_sidecar_bytes" \
    "$snapshot_point_state_history_accessor_sidecar_share_milli" "$snapshot_point_state_history_accessor_share_milli" \
    "$snapshot_point_latest_btree_segments" "$snapshot_point_latest_btree_bytes" \
    "$snapshot_point_latest_btree_payload_bytes" "$snapshot_point_latest_btree_sidecar_bytes" \
    "$snapshot_point_latest_btree_sidecar_share_milli" "$snapshot_point_latest_btree_share_milli" \
    "$snapshot_point_chain_freezer_accessor_segments" "$snapshot_point_chain_freezer_accessor_bytes" \
    "$snapshot_point_chain_freezer_accessor_payload_bytes" "$snapshot_point_chain_freezer_accessor_sidecar_bytes" \
    "$snapshot_point_chain_freezer_accessor_sidecar_share_milli" "$snapshot_point_chain_freezer_accessor_share_milli" \
    "$snapshot_point_code_domain_segments" "$snapshot_point_code_domain_bytes" \
    "$snapshot_point_code_domain_payload_bytes" "$snapshot_point_code_domain_sidecar_bytes" \
    "$snapshot_point_code_domain_sidecar_share_milli" "$snapshot_point_code_domain_share_milli" \
    "$snapshot_point_commitment_snapshot_segments" "$snapshot_point_commitment_snapshot_bytes" \
    "$snapshot_point_commitment_snapshot_payload_bytes" "$snapshot_point_commitment_snapshot_sidecar_bytes" \
    "$snapshot_point_commitment_snapshot_sidecar_share_milli" "$snapshot_point_commitment_snapshot_share_milli" \
    "$RUN_ARCHIVE_API_CHECKS" "$RUN_ARCHIVE_API_BLOCK" "$RUN_ARCHIVE_API_DEPTH_BLOCKS" "$RUN_ARCHIVE_API_FAILURES" \
    "$RUN_ARCHIVE_API_CALL_PROBE" "$RUN_ARCHIVE_API_TRACE_TRANSACTION_PROBE" "$RUN_ARCHIVE_API_TRACE_BLOCK_PROBE" \
    "$RUN_ARCHIVE_API_METHODS" "$RUN_ARCHIVE_API_TX_PROBE" "$RUN_ARCHIVE_API_TX_METHODS" \
    "$RUN_COLD_FREEZER_TO_BLOCK" "$RUN_DERIVED_INDEX_TO_BLOCK" \
    "$RUN_CHAIN_LOOKUP_PRUNE_TO_BLOCK" "$RUN_TAIL_PRUNED_THROUGH_BLOCK" \
    "$RUN_BALANCE_TRACE_PRUNE_TO_BLOCK" "$RUN_SECTION_BLOOM_PRUNE_TO_SECTION" \
    "$RUN_SIGNED_COLD_PRUNE" "$RUN_TAIL_PRUNED_FILES" "$HISTORY_WINDOW" \
    "$RUN_EVENT_LOG_INDEX_SEGMENTS" "$RUN_EVENT_LOG_INDEX_ADDRESS_KEYS" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS" "$RUN_EVENT_LOG_INDEX_ADDRESS_AVG_POSTINGS_MILLI" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS" "$RUN_EVENT_LOG_INDEX_ADDRESS_SINGLETON_KEYS" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_MULTI_POSTING_KEYS" "$RUN_EVENT_LOG_INDEX_TOPIC_KEYS" \
    "$RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS" "$RUN_EVENT_LOG_INDEX_TOPIC_AVG_POSTINGS_MILLI" \
    "$RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS" "$RUN_EVENT_LOG_INDEX_TOPIC_SINGLETON_KEYS" \
    "$RUN_EVENT_LOG_INDEX_TOPIC_MULTI_POSTING_KEYS" "$RUN_EVENT_LOG_INDEX_FROM_BLOCK" \
    "$RUN_EVENT_LOG_INDEX_TO_BLOCK" \
    "$snapshot_state_history_bytes" "$snapshot_state_history_compressed_segments" \
    "$snapshot_state_history_compressed_bytes" "$snapshot_state_history_compressed_share_milli"
  python3 - "$OUTPUT" "$profile" "$mode" "$role" "$status" "$target" "$height" "$elapsed" \
    "$total" "$chain" "$ancient" "$snapshots" "$ancient_files" "$snapshot_files" \
    "$derived_index_bytes" "$derived_index_files" \
    "$snapshot_profile_status" "$snapshot_profile_segments" "$snapshot_profile_total_bytes" \
    "$snapshot_profile_verify_files" "$snapshot_profile_verified_segments" \
    "$snapshot_payload_bytes" "$snapshot_sidecar_bytes" "$snapshot_sidecar_share_milli" \
    "$snapshot_latest_sidecar_bytes" "$snapshot_latest_sidecar_share_milli" \
    "$snapshot_state_history_sidecar_bytes" "$snapshot_state_history_sidecar_share_milli" \
    "$snapshot_chain_freezer_sidecar_bytes" "$snapshot_chain_freezer_sidecar_share_milli" \
    "$snapshot_event_log_sidecar_bytes" "$snapshot_event_log_sidecar_share_milli" \
    "$snapshot_balance_trace_sidecar_bytes" "$snapshot_balance_trace_sidecar_share_milli" \
    "$snapshot_section_bloom_sidecar_bytes" "$snapshot_section_bloom_sidecar_share_milli" \
    "$snapshot_point_tx_hash_lookup_segments" "$snapshot_point_tx_hash_lookup_bytes" \
    "$snapshot_point_tx_hash_lookup_payload_bytes" "$snapshot_point_tx_hash_lookup_sidecar_bytes" \
    "$snapshot_point_tx_hash_lookup_sidecar_share_milli" "$snapshot_point_tx_hash_lookup_share_milli" \
    "$snapshot_point_event_log_index_segments" "$snapshot_point_event_log_index_bytes" \
    "$snapshot_point_event_log_index_payload_bytes" "$snapshot_point_event_log_index_sidecar_bytes" \
    "$snapshot_point_event_log_index_sidecar_share_milli" "$snapshot_point_event_log_index_share_milli" \
    "$snapshot_point_state_history_accessor_segments" "$snapshot_point_state_history_accessor_bytes" \
    "$snapshot_point_state_history_accessor_payload_bytes" "$snapshot_point_state_history_accessor_sidecar_bytes" \
    "$snapshot_point_state_history_accessor_sidecar_share_milli" "$snapshot_point_state_history_accessor_share_milli" \
    "$snapshot_point_latest_btree_segments" "$snapshot_point_latest_btree_bytes" \
    "$snapshot_point_latest_btree_payload_bytes" "$snapshot_point_latest_btree_sidecar_bytes" \
    "$snapshot_point_latest_btree_sidecar_share_milli" "$snapshot_point_latest_btree_share_milli" \
    "$snapshot_point_chain_freezer_accessor_segments" "$snapshot_point_chain_freezer_accessor_bytes" \
    "$snapshot_point_chain_freezer_accessor_payload_bytes" "$snapshot_point_chain_freezer_accessor_sidecar_bytes" \
    "$snapshot_point_chain_freezer_accessor_sidecar_share_milli" "$snapshot_point_chain_freezer_accessor_share_milli" \
    "$snapshot_point_code_domain_segments" "$snapshot_point_code_domain_bytes" \
    "$snapshot_point_code_domain_payload_bytes" "$snapshot_point_code_domain_sidecar_bytes" \
    "$snapshot_point_code_domain_sidecar_share_milli" "$snapshot_point_code_domain_share_milli" \
    "$snapshot_point_commitment_snapshot_segments" "$snapshot_point_commitment_snapshot_bytes" \
    "$snapshot_point_commitment_snapshot_payload_bytes" "$snapshot_point_commitment_snapshot_sidecar_bytes" \
    "$snapshot_point_commitment_snapshot_sidecar_share_milli" "$snapshot_point_commitment_snapshot_share_milli" \
    "$RUN_COLD_FREEZER_TO_BLOCK" "$RUN_DERIVED_INDEX_TO_BLOCK" "$RUN_DERIVED_INDEX_SEGMENTS" \
    "$RUN_DERIVED_INDEX_BUILD_SECONDS" "$RUN_EVENT_LOG_INDEX_SEGMENTS" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_KEYS" "$RUN_EVENT_LOG_INDEX_ADDRESS_POSTINGS" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_AVG_POSTINGS_MILLI" "$RUN_EVENT_LOG_INDEX_ADDRESS_MAX_POSTINGS" \
    "$RUN_EVENT_LOG_INDEX_ADDRESS_SINGLETON_KEYS" "$RUN_EVENT_LOG_INDEX_ADDRESS_MULTI_POSTING_KEYS" \
    "$RUN_EVENT_LOG_INDEX_TOPIC_KEYS" "$RUN_EVENT_LOG_INDEX_TOPIC_POSTINGS" \
    "$RUN_EVENT_LOG_INDEX_TOPIC_AVG_POSTINGS_MILLI" "$RUN_EVENT_LOG_INDEX_TOPIC_MAX_POSTINGS" \
    "$RUN_EVENT_LOG_INDEX_TOPIC_SINGLETON_KEYS" "$RUN_EVENT_LOG_INDEX_TOPIC_MULTI_POSTING_KEYS" \
    "$RUN_EVENT_LOG_INDEX_FROM_BLOCK" "$RUN_EVENT_LOG_INDEX_TO_BLOCK" \
    "$RUN_BALANCE_TRACE_PRUNE_TO_BLOCK" \
    "$RUN_BALANCE_TRACE_BLOCK_ROWS" "$RUN_BALANCE_TRACE_ACCOUNT_ROWS" \
    "$RUN_SECTION_BLOOM_PRUNE_TO_SECTION" "$RUN_SECTION_BLOOM_ROWS" \
    "$RUN_SIGNED_COLD_PRUNE" "$RUN_CHAIN_LOOKUP_PRUNE_TO_BLOCK" \
    "$RUN_CHAIN_LOOKUP_BLOCK_INDEXES" "$RUN_CHAIN_LOOKUP_TX_INDEXES" \
    "$RUN_RETIRED_PRUNE_RAN" "$RUN_RETIRED_PRUNE_SEGMENTS" "$RUN_RETIRED_PRUNE_DELETED" \
    "$RUN_RETIRED_PRUNE_MISSING" "$RUN_RETIRED_PRUNE_SKIPPED_ACTIVE" \
    "$RUN_RETIRED_PRUNE_BYTES_DELETED" \
    "$RUN_TAIL_PRUNED_THROUGH_BLOCK" "$RUN_TAIL_PRUNED_FILES" "$HISTORY_WINDOW" \
    "$RUN_STORAGE_ALERT_STATUS" \
    "$RUN_FREEZER_ALERT_STATUS" "$RUN_FREEZER_ALERT_ISSUES" "$RUN_FREEZER_ALERT_HIDDEN_BYTES" \
    "$RUN_FREEZER_ALERT_DETAILS" \
    "$RUN_STAGE_VERIFY_STATUS" "$RUN_STAGE_VERIFY_ISSUES" "$RUN_STAGE_VERIFY_DETAILS" \
    "$RUN_STAGE_ALERT_PIPELINE_COMPLETE" "$RUN_STAGE_ALERT_PIPELINE_PENDING" \
    "$RUN_STAGE_ALERT_PIPELINE_ISSUES" "$RUN_STAGE_ALERT_PIPELINE_NEXT" \
    "$RUN_STAGE_ALERT_PIPELINE_NEXT_STATUS" "$RUN_STAGE_ALERT_PIPELINE_NEXT_TARGET" \
    "$RUN_STAGE_ALERT_PIPELINE_NEXT_UPSTREAM" "$RUN_STAGE_ALERT_PIPELINE_NEXT_CURRENT" \
    "$RUN_STAGE_ALERT_PIPELINE_TASKS" \
    "$RUN_MODE_ALERT_STATUS" "$RUN_MODE_ALERT_ISSUES" "$RUN_MODE_ALERT_DETAILS" \
    "$RUN_PRUNE_MODE" "$RUN_PRUNE_MODE_PERSISTED" \
    "$RUN_SNAPSHOT_ALERT_STATUS" "$RUN_SNAPSHOT_ALERT_ISSUES" "$RUN_SNAPSHOT_ALERT_DETAILS" \
    "$RUN_SNAPSHOT_RETIRED_SEGMENTS" "$RUN_SNAPSHOT_RETIRED_FILES" \
    "$RUN_SNAPSHOT_RETIRED_MISSING" "$RUN_SNAPSHOT_RETIRED_SKIPPED_ACTIVE" \
    "$RUN_SNAPSHOT_RETIRED_BYTES" \
    "$RUN_ARCHIVE_API_STATUS" "$RUN_ARCHIVE_API_CHECKS" "$RUN_ARCHIVE_API_FAILURES" \
    "$RUN_ARCHIVE_API_BLOCK" "$RUN_ARCHIVE_API_DEPTH_BLOCKS" "$RUN_ARCHIVE_API_CALL_PROBE" \
    "$RUN_ARCHIVE_API_TRACE_TRANSACTION_PROBE" "$RUN_ARCHIVE_API_TRACE_BLOCK_PROBE" "$RUN_ARCHIVE_API_METHODS" \
    "$RUN_ARCHIVE_API_TX_PROBE" "$RUN_ARCHIVE_API_TX_HASH" "$RUN_ARCHIVE_API_TX_METHODS" \
    "$snapshot_state_history_bytes" "$snapshot_state_history_compressed_segments" \
    "$snapshot_state_history_compressed_bytes" "$snapshot_state_history_compressed_share_milli" \
    "$benchmark_prometheus" "$RUN_STORAGE_ALERT_PROMETHEUS" "$datadir" "$log_path" <<'PY'
import json, sys, time
out = sys.argv[1]
keys = [
    "profile", "mode", "role", "status", "targetBlock", "height", "elapsedSeconds",
    "datadirBytes", "chaindataBytes", "ancientBytes", "snapshotBytes",
    "ancientFiles", "snapshotFiles", "derivedIndexBytes", "derivedIndexFiles",
    "snapshotManifestProfileStatus", "snapshotProfileSegments", "snapshotProfileTotalBytes",
    "snapshotProfileVerifyFiles", "snapshotProfileVerifiedSegments",
    "snapshotPayloadBytes", "snapshotSidecarBytes", "snapshotSidecarShareMilli",
    "snapshotLatestSidecarBytes", "snapshotLatestSidecarShareMilli",
    "snapshotStateHistorySidecarBytes", "snapshotStateHistorySidecarShareMilli",
    "snapshotChainFreezerSidecarBytes", "snapshotChainFreezerSidecarShareMilli",
    "snapshotEventLogSidecarBytes", "snapshotEventLogSidecarShareMilli",
    "snapshotBalanceTraceSidecarBytes", "snapshotBalanceTraceSidecarShareMilli",
    "snapshotSectionBloomSidecarBytes", "snapshotSectionBloomSidecarShareMilli",
    "snapshotPointTxHashLookupSegments", "snapshotPointTxHashLookupBytes",
    "snapshotPointTxHashLookupPayloadBytes", "snapshotPointTxHashLookupSidecarBytes",
    "snapshotPointTxHashLookupSidecarShareMilli", "snapshotPointTxHashLookupSnapshotShareMilli",
    "snapshotPointEventLogIndexSegments", "snapshotPointEventLogIndexBytes",
    "snapshotPointEventLogIndexPayloadBytes", "snapshotPointEventLogIndexSidecarBytes",
    "snapshotPointEventLogIndexSidecarShareMilli", "snapshotPointEventLogIndexSnapshotShareMilli",
    "snapshotPointStateHistoryAccessorSegments", "snapshotPointStateHistoryAccessorBytes",
    "snapshotPointStateHistoryAccessorPayloadBytes", "snapshotPointStateHistoryAccessorSidecarBytes",
    "snapshotPointStateHistoryAccessorSidecarShareMilli",
    "snapshotPointStateHistoryAccessorSnapshotShareMilli",
    "snapshotPointLatestBTreeSegments", "snapshotPointLatestBTreeBytes",
    "snapshotPointLatestBTreePayloadBytes", "snapshotPointLatestBTreeSidecarBytes",
    "snapshotPointLatestBTreeSidecarShareMilli", "snapshotPointLatestBTreeSnapshotShareMilli",
    "snapshotPointChainFreezerAccessorSegments", "snapshotPointChainFreezerAccessorBytes",
    "snapshotPointChainFreezerAccessorPayloadBytes", "snapshotPointChainFreezerAccessorSidecarBytes",
    "snapshotPointChainFreezerAccessorSidecarShareMilli",
    "snapshotPointChainFreezerAccessorSnapshotShareMilli",
    "snapshotPointCodeDomainSegments", "snapshotPointCodeDomainBytes",
    "snapshotPointCodeDomainPayloadBytes", "snapshotPointCodeDomainSidecarBytes",
    "snapshotPointCodeDomainSidecarShareMilli", "snapshotPointCodeDomainSnapshotShareMilli",
    "snapshotPointCommitmentSnapshotSegments", "snapshotPointCommitmentSnapshotBytes",
    "snapshotPointCommitmentSnapshotPayloadBytes", "snapshotPointCommitmentSnapshotSidecarBytes",
    "snapshotPointCommitmentSnapshotSidecarShareMilli",
    "snapshotPointCommitmentSnapshotSnapshotShareMilli",
    "coldFreezerToBlock", "derivedIndexToBlock", "derivedIndexSegments",
    "derivedIndexBuildSeconds", "eventLogIndexSegments", "eventLogIndexAddressKeys",
    "eventLogIndexAddressPostings", "eventLogIndexAddressAvgPostingsMilli",
    "eventLogIndexAddressMaxPostings", "eventLogIndexAddressSingletonKeys",
    "eventLogIndexAddressMultiPostingKeys", "eventLogIndexTopicKeys",
    "eventLogIndexTopicPostings", "eventLogIndexTopicAvgPostingsMilli",
    "eventLogIndexTopicMaxPostings", "eventLogIndexTopicSingletonKeys",
    "eventLogIndexTopicMultiPostingKeys", "eventLogIndexFromBlock",
    "eventLogIndexToBlock", "balanceTracePruneToBlock",
    "balanceTraceBlockRowsPruned", "balanceTraceAccountRowsPruned",
    "sectionBloomPruneToSection", "sectionBloomRowsPruned",
    "signedColdPrune", "chainLookupPruneToBlock",
    "chainLookupBlockIndexes", "chainLookupTxIndexes",
    "retiredPruneRan", "retiredPruneSegments", "retiredPruneDeleted",
    "retiredPruneMissing", "retiredPruneSkippedActive", "retiredPruneBytesDeleted",
    "tailPrunedThroughBlock", "tailPrunedFiles", "historyWindow",
    "storageAlertStatus", "freezerAlertStatus", "freezerAlertIssues", "freezerAlertHiddenBytes",
    "freezerAlertDetails", "stageVerifyStatus", "stageVerifyIssues",
    "stageVerifyDetails", "stageAlertPipelineComplete", "stageAlertPipelinePending",
    "stageAlertPipelineIssues", "stageAlertPipelineNext",
    "stageAlertPipelineNextStatus", "stageAlertPipelineNextTarget",
    "stageAlertPipelineNextUpstream", "stageAlertPipelineNextCurrent",
    "stageAlertPipelineTasks", "modeAlertStatus", "modeAlertIssues",
    "modeAlertDetails", "pruneMode", "pruneModePersisted",
    "snapshotAlertStatus", "snapshotAlertIssues", "snapshotAlertDetails", "snapshotRetiredSegments",
    "snapshotRetiredFiles", "snapshotRetiredMissing", "snapshotRetiredSkippedActive",
    "snapshotRetiredBytes", "archiveApiStatus", "archiveApiChecks", "archiveApiFailures",
    "archiveApiBlock", "archiveApiDepthBlocks", "archiveApiCallProbe", "archiveApiTraceTransactionProbe",
    "archiveApiTraceBlockProbe", "archiveApiMethods", "archiveApiTxProbe", "archiveApiTxHash",
    "archiveApiTxMethods",
    "snapshotStateHistoryBytes", "snapshotStateHistoryCompressedSegments",
    "snapshotStateHistoryCompressedBytes", "snapshotStateHistoryCompressedShareMilli",
    "storageBenchmarkPrometheus", "storageAlertPrometheus",
    "datadir", "log",
]
values = sys.argv[2:]
if len(values) != len(keys):
    raise SystemExit(f"storage benchmark field mismatch: {len(values)} values for {len(keys)} keys")
ints = {
    "targetBlock", "height", "elapsedSeconds",
    "datadirBytes", "chaindataBytes", "ancientBytes", "snapshotBytes",
    "ancientFiles", "snapshotFiles", "derivedIndexBytes", "derivedIndexFiles",
    "coldFreezerToBlock", "derivedIndexToBlock",
    "derivedIndexSegments", "derivedIndexBuildSeconds", "balanceTracePruneToBlock",
    "snapshotProfileSegments", "snapshotProfileTotalBytes", "snapshotProfileVerifiedSegments",
    "snapshotPayloadBytes",
    "snapshotSidecarBytes", "snapshotSidecarShareMilli", "snapshotLatestSidecarBytes",
    "snapshotLatestSidecarShareMilli", "snapshotStateHistorySidecarBytes",
    "snapshotStateHistorySidecarShareMilli", "snapshotChainFreezerSidecarBytes",
    "snapshotChainFreezerSidecarShareMilli", "snapshotEventLogSidecarBytes",
    "snapshotEventLogSidecarShareMilli", "snapshotBalanceTraceSidecarBytes",
    "snapshotBalanceTraceSidecarShareMilli", "snapshotSectionBloomSidecarBytes",
    "snapshotSectionBloomSidecarShareMilli",
    "snapshotPointTxHashLookupSegments", "snapshotPointTxHashLookupBytes",
    "snapshotPointTxHashLookupPayloadBytes", "snapshotPointTxHashLookupSidecarBytes",
    "snapshotPointTxHashLookupSidecarShareMilli", "snapshotPointTxHashLookupSnapshotShareMilli",
    "snapshotPointEventLogIndexSegments", "snapshotPointEventLogIndexBytes",
    "snapshotPointEventLogIndexPayloadBytes", "snapshotPointEventLogIndexSidecarBytes",
    "snapshotPointEventLogIndexSidecarShareMilli", "snapshotPointEventLogIndexSnapshotShareMilli",
    "snapshotPointStateHistoryAccessorSegments", "snapshotPointStateHistoryAccessorBytes",
    "snapshotPointStateHistoryAccessorPayloadBytes", "snapshotPointStateHistoryAccessorSidecarBytes",
    "snapshotPointStateHistoryAccessorSidecarShareMilli",
    "snapshotPointStateHistoryAccessorSnapshotShareMilli",
    "snapshotPointLatestBTreeSegments", "snapshotPointLatestBTreeBytes",
    "snapshotPointLatestBTreePayloadBytes", "snapshotPointLatestBTreeSidecarBytes",
    "snapshotPointLatestBTreeSidecarShareMilli", "snapshotPointLatestBTreeSnapshotShareMilli",
    "snapshotPointChainFreezerAccessorSegments", "snapshotPointChainFreezerAccessorBytes",
    "snapshotPointChainFreezerAccessorPayloadBytes", "snapshotPointChainFreezerAccessorSidecarBytes",
    "snapshotPointChainFreezerAccessorSidecarShareMilli",
    "snapshotPointChainFreezerAccessorSnapshotShareMilli",
    "snapshotPointCodeDomainSegments", "snapshotPointCodeDomainBytes",
    "snapshotPointCodeDomainPayloadBytes", "snapshotPointCodeDomainSidecarBytes",
    "snapshotPointCodeDomainSidecarShareMilli", "snapshotPointCodeDomainSnapshotShareMilli",
    "snapshotPointCommitmentSnapshotSegments", "snapshotPointCommitmentSnapshotBytes",
    "snapshotPointCommitmentSnapshotPayloadBytes", "snapshotPointCommitmentSnapshotSidecarBytes",
    "snapshotPointCommitmentSnapshotSidecarShareMilli",
    "snapshotPointCommitmentSnapshotSnapshotShareMilli",
    "eventLogIndexSegments", "eventLogIndexAddressKeys", "eventLogIndexAddressPostings",
    "eventLogIndexAddressAvgPostingsMilli", "eventLogIndexAddressMaxPostings",
    "eventLogIndexAddressSingletonKeys", "eventLogIndexAddressMultiPostingKeys",
    "eventLogIndexTopicKeys", "eventLogIndexTopicPostings",
    "eventLogIndexTopicAvgPostingsMilli", "eventLogIndexTopicMaxPostings",
    "eventLogIndexTopicSingletonKeys", "eventLogIndexTopicMultiPostingKeys",
    "eventLogIndexFromBlock", "eventLogIndexToBlock",
    "balanceTraceBlockRowsPruned", "balanceTraceAccountRowsPruned",
    "sectionBloomPruneToSection", "sectionBloomRowsPruned", "signedColdPrune",
    "chainLookupPruneToBlock", "chainLookupBlockIndexes", "chainLookupTxIndexes",
    "retiredPruneSegments", "retiredPruneDeleted", "retiredPruneMissing",
    "retiredPruneSkippedActive", "retiredPruneBytesDeleted",
    "tailPrunedThroughBlock", "tailPrunedFiles", "historyWindow",
    "freezerAlertIssues", "freezerAlertHiddenBytes",
    "stageVerifyIssues", "stageAlertPipelinePending", "stageAlertPipelineIssues",
    "stageAlertPipelineNextTarget", "stageAlertPipelineNextCurrent", "modeAlertIssues",
    "snapshotAlertIssues", "snapshotRetiredSegments", "snapshotRetiredFiles",
    "snapshotRetiredMissing", "snapshotRetiredSkippedActive", "snapshotRetiredBytes",
    "archiveApiChecks", "archiveApiFailures", "archiveApiBlock", "archiveApiDepthBlocks",
    "snapshotStateHistoryBytes", "snapshotStateHistoryCompressedSegments",
    "snapshotStateHistoryCompressedBytes", "snapshotStateHistoryCompressedShareMilli",
}
bools = {
    "snapshotProfileVerifyFiles",
    "pruneModePersisted",
    "retiredPruneRan",
    "stageAlertPipelineComplete",
    "archiveApiCallProbe",
    "archiveApiTraceTransactionProbe",
    "archiveApiTraceBlockProbe",
    "archiveApiTxProbe",
}
row = {"unix": int(time.time())}
for key, value in zip(keys, values):
    if key in ints:
        row[key] = int(value)
    elif key in bools:
        row[key] = str(value).lower() in {"1", "true", "yes"}
    else:
        row[key] = value
for key in (
    "freezerAlertDetails",
    "stageVerifyDetails",
    "stageAlertPipelineTasks",
    "modeAlertDetails",
    "snapshotAlertDetails",
    "archiveApiMethods",
    "archiveApiTxMethods",
):
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
  run_archive_api_probe "$((port_base + 3))" "$height" "$log_path"
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
  run_archive_api_probe "$((port_base + 13))" "$height" "$node_log"
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
