#!/usr/bin/env bash
#
# Emit one JSONL sample for a long-running Nile sync/soak datadir.
#
# This is a measurement helper, not a node launcher. It is safe to run from
# cron/systemd while gtron is live because the default path reads only HTTP and
# filesystem sizes. Use --offline-db-check only when the node is stopped or the
# datadir can be opened by gtron db utilities.
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/../.." && pwd)"
GTRON="${GTRON:-$BASEDIR/build/bin/gtron}"
DATADIR="${DATADIR:-}"
HTTP="http://127.0.0.1:8090"
OUTPUT=""
NETWORK="nile"
MODE="unknown"
LABEL="nile-sync"
START_UNIX=0
STAGE_STATUS_FILE=""
OFFLINE_DB_CHECK=0
STRICT_OFFLINE_DB_CHECK=0

usage() {
  cat <<'EOF'
Usage: scripts/dev/nile_sync_sample.sh --datadir DIR [options]

Options:
  --datadir DIR              gtron datadir to size and optionally inspect
  --http URL                 gtron HTTP base URL (default: http://127.0.0.1:8090)
  --output FILE              Append JSONL row to FILE; stdout is always printed
  --gtron PATH               gtron binary for optional offline db checks
  --network NAME             Network label (default: nile)
  --mode MODE                Prune/storage mode label when known
  --label LABEL              Free-form sample label (default: nile-sync)
  --start-unix SECONDS       Sync start unix timestamp; emits elapsed/blocksPerSecond
  --stage-status-file FILE   Parse a captured `gtron db stage-status` output file
  --offline-db-check         Also run gtron db storage-alerts against DATADIR
  --strict-offline-db-check  Fail when offline db check reports critical issues
  -h, --help                 Show this help

Examples:
  scripts/dev/nile_sync_sample.sh \
    --datadir /data/gtron/nile/datadir \
    --http http://127.0.0.1:8090 \
    --output /data/gtron/nile/sync-samples.jsonl

  # When the node is stopped:
  scripts/dev/nile_sync_sample.sh --datadir /data/gtron/nile/datadir --offline-db-check
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --datadir) DATADIR="${2:?}"; shift 2 ;;
    --http) HTTP="${2:?}"; shift 2 ;;
    --output) OUTPUT="${2:?}"; shift 2 ;;
    --gtron) GTRON="${2:?}"; shift 2 ;;
    --network) NETWORK="${2:?}"; shift 2 ;;
    --mode) MODE="${2:?}"; shift 2 ;;
    --label) LABEL="${2:?}"; shift 2 ;;
    --start-unix) START_UNIX="${2:?}"; shift 2 ;;
    --stage-status-file) STAGE_STATUS_FILE="${2:?}"; shift 2 ;;
    --offline-db-check) OFFLINE_DB_CHECK=1; shift ;;
    --strict-offline-db-check) OFFLINE_DB_CHECK=1; STRICT_OFFLINE_DB_CHECK=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) die "unknown option: $1" ;;
  esac
done

[ -n "$DATADIR" ] || die "--datadir is required"
case "$START_UNIX" in
  ''|*[!0-9]*) die "--start-unix must be a non-negative integer" ;;
esac
if [ -n "$OUTPUT" ]; then
  mkdir -p "$(dirname "$OUTPUT")"
fi
if [ -n "$STAGE_STATUS_FILE" ] && [ ! -r "$STAGE_STATUS_FILE" ]; then
  die "--stage-status-file is not readable: $STAGE_STATUS_FILE"
fi

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

http_get() {
  local path="$1"
  local out="$2"
  curl -sf --max-time 5 "$HTTP$path" >"$out"
}

tmpdir="$(mktemp -d)"
cleanup() {
  rm -rf "$tmpdir"
}
trap cleanup EXIT

nowblock_json="$tmpdir/getnowblock.json"
nodeinfo_json="$tmpdir/getnodeinfo.json"
nodes_json="$tmpdir/listnodes.json"
storage_alerts_out="$tmpdir/storage-alerts.out"

nowblock_status="ok"
nodeinfo_status="ok"
nodes_status="ok"
if ! http_get /wallet/getnowblock "$nowblock_json"; then
  nowblock_status="error"
  : >"$nowblock_json"
fi
if ! http_get /wallet/getnodeinfo "$nodeinfo_json"; then
  nodeinfo_status="error"
  : >"$nodeinfo_json"
fi
if ! http_get /wallet/listnodes "$nodes_json"; then
  nodes_status="error"
  : >"$nodes_json"
fi

offline_status="skipped"
offline_exit=0
if [ "$OFFLINE_DB_CHECK" -eq 1 ]; then
  if [ ! -x "$GTRON" ]; then
    offline_status="missing-gtron"
    offline_exit=127
    echo "gtron binary not executable: $GTRON" >"$storage_alerts_out"
  elif "$GTRON" db storage-alerts --datadir "$DATADIR" >"$storage_alerts_out" 2>&1; then
    offline_status="ok"
  else
    offline_exit=$?
    offline_status="error"
  fi
else
  : >"$storage_alerts_out"
fi

git_commit="unknown"
git_dirty="unknown"
if git -C "$BASEDIR" rev-parse --short HEAD >/dev/null 2>&1; then
  git_commit="$(git -C "$BASEDIR" rev-parse --short HEAD)"
  if [ -n "$(git -C "$BASEDIR" status --porcelain --untracked-files=normal)" ]; then
    git_dirty="true"
  else
    git_dirty="false"
  fi
fi

total_bytes="$(size_bytes "$DATADIR")"
chaindata_bytes="$(size_bytes "$DATADIR/gtron/chaindata")"
ancient_bytes="$(size_bytes "$DATADIR/gtron/ancient")"
snapshot_bytes="$(size_bytes "$DATADIR/gtron/state-snapshots")"
replay_bytes="$(size_bytes "$DATADIR/gtron/balance-trace-replay")"
ancient_files="$(file_count "$DATADIR/gtron/ancient")"
snapshot_files="$(file_count "$DATADIR/gtron/state-snapshots")"

python3 - "$OUTPUT" "$NETWORK" "$MODE" "$LABEL" "$HTTP" "$DATADIR" \
  "$nowblock_json" "$nodeinfo_json" "$nodes_json" "$storage_alerts_out" \
  "$STAGE_STATUS_FILE" \
  "$nowblock_status" "$nodeinfo_status" "$nodes_status" \
  "$offline_status" "$offline_exit" "$OFFLINE_DB_CHECK" "$STRICT_OFFLINE_DB_CHECK" \
  "$START_UNIX" "$total_bytes" "$chaindata_bytes" "$ancient_bytes" "$snapshot_bytes" \
  "$replay_bytes" "$ancient_files" "$snapshot_files" "$git_commit" "$git_dirty" <<'PY'
import json
import re
import sys
import time
from pathlib import Path

(
    output,
    network,
    mode,
    label,
    http,
    datadir,
    nowblock_path,
    nodeinfo_path,
    nodes_path,
    storage_alerts_path,
    stage_status_path,
    nowblock_status,
    nodeinfo_status,
    nodes_status,
    offline_status,
    offline_exit,
    offline_enabled,
    strict_offline,
    start_unix,
    total_bytes,
    chaindata_bytes,
    ancient_bytes,
    snapshot_bytes,
    replay_bytes,
    ancient_files,
    snapshot_files,
    git_commit,
    git_dirty,
) = sys.argv[1:]

def load_json(path):
    try:
        data = Path(path).read_text(encoding="utf-8")
        if not data.strip():
            return {}
        return json.loads(data)
    except Exception:
        return {}

def parse_alerts(text):
    row = {
        "freezerAlertStatus": "unknown",
        "freezerAlertIssues": -1,
        "freezerAlertHiddenBytes": -1,
        "stageVerifyStatus": "unknown",
        "stageVerifyIssues": -1,
        "snapshotAlertStatus": "unknown",
        "snapshotAlertIssues": -1,
        "snapshotRetiredBytes": -1,
    }
    patterns = {
        "freezerAlertStatus": r"freezerStatus=([^ ]+)",
        "freezerAlertIssues": r"freezerIssues=([0-9]+)",
        "freezerAlertHiddenBytes": r"hiddenSize=([0-9]+)",
        "stageVerifyStatus": r"stageStatus=([^ ]+)",
        "stageVerifyIssues": r"stageIssues=([0-9]+)",
        "snapshotAlertStatus": r"snapshotStatus=([^ ]+)",
        "snapshotAlertIssues": r"snapshotIssues=([0-9]+)",
        "snapshotRetiredBytes": r"retiredBytes=([0-9]+)",
    }
    for key, pattern in patterns.items():
        found = re.findall(pattern, text)
        if not found:
            continue
        value = found[-1]
        if key.endswith("Issues") or key.endswith("Bytes"):
            row[key] = int(value)
        else:
            row[key] = value
    return row

def parse_stage_status(path):
    row = {
        "stageStatusFile": path,
        "stageStatusFileStatus": "skipped" if not path else "missing",
        "stageKnown": -1,
        "stageRows": -1,
        "stageProgress": {},
        "stageMismatchRows": 0,
        "stageUnboundRows": 0,
        "stageMissingCanonicalRows": 0,
        "stageSyncInventory": -1,
        "stageSyncBodies": -1,
        "stageSyncBodiesReady": -1,
        "stageSyncImport": -1,
        "stageSyncExecution": -1,
        "stageSyncCommitment": -1,
        "stageSyncFinish": -1,
        "stageCanonicalFinish": -1,
        "stageChainFreezer": -1,
        "stageSnapshotEventLogBuild": -1,
        "stageSyncBodiesReadyGapBlocks": -1,
        "stageSyncImportExecutionLagBlocks": -1,
        "stageSyncExecutionCommitmentLagBlocks": -1,
        "stageSyncCommitmentFinishLagBlocks": -1,
    }
    if not path:
        return row
    try:
        text = Path(path).read_text(encoding="utf-8", errors="replace")
    except Exception:
        return row
    row["stageStatusFileStatus"] = "ok"
    for line in text.splitlines():
        if line.startswith("Stage status:"):
            known = re.findall(r"known=([0-9]+)", line)
            rows = re.findall(r"rows=([0-9]+)", line)
            if known:
                row["stageKnown"] = int(known[-1])
            if rows:
                row["stageRows"] = int(rows[-1])
            continue
        if not line.startswith("Stage progress:"):
            continue
        fields = {}
        for token in line.split()[2:]:
            if "=" not in token:
                continue
            key, value = token.split("=", 1)
            fields[key] = value
        name = fields.get("name")
        if not name:
            continue
        present = fields.get("status") != "missing"
        value = -1
        if present:
            try:
                value = int(fields.get("value", "-1"))
            except Exception:
                value = -1
        verified = fields.get("verified", "")
        entry = {
            "group": fields.get("group", ""),
            "present": present,
            "value": value,
            "hash": fields.get("hash", ""),
            "verified": verified,
            "canonicalHash": fields.get("canonicalHash", ""),
        }
        if not present:
            entry["status"] = fields.get("status", "missing")
        row["stageProgress"][name] = entry
        if verified == "mismatch":
            row["stageMismatchRows"] += 1
        elif verified == "unbound":
            row["stageUnboundRows"] += 1
        elif verified == "missing-canonical":
            row["stageMissingCanonicalRows"] += 1

    stage_fields = {
        "SyncInventory": "stageSyncInventory",
        "SyncBodies": "stageSyncBodies",
        "SyncBodiesReady": "stageSyncBodiesReady",
        "SyncImport": "stageSyncImport",
        "SyncExecution": "stageSyncExecution",
        "SyncCommitment": "stageSyncCommitment",
        "SyncFinish": "stageSyncFinish",
        "Finish": "stageCanonicalFinish",
        "ChainFreezer": "stageChainFreezer",
        "SnapshotEventLogBuild": "stageSnapshotEventLogBuild",
    }
    for stage, field in stage_fields.items():
        entry = row["stageProgress"].get(stage)
        if entry and entry.get("present"):
            row[field] = int(entry.get("value", -1))
    row["stageSyncBodiesReadyGapBlocks"] = lag(row["stageSyncBodies"], row["stageSyncBodiesReady"])
    row["stageSyncImportExecutionLagBlocks"] = lag(row["stageSyncImport"], row["stageSyncExecution"])
    row["stageSyncExecutionCommitmentLagBlocks"] = lag(row["stageSyncExecution"], row["stageSyncCommitment"])
    row["stageSyncCommitmentFinishLagBlocks"] = lag(row["stageSyncCommitment"], row["stageSyncFinish"])
    return row

def load_previous_sample(output_path):
    if not output_path:
        return {}
    try:
        path = Path(output_path)
        if not path.exists():
            return {}
        for line in reversed(path.read_text(encoding="utf-8").splitlines()):
            line = line.strip()
            if not line:
                continue
            return json.loads(line)
    except Exception:
        return {}
    return {}

def number(row, key, default=0):
    try:
        value = row.get(key, default)
        if value is None:
            return default
        return int(value)
    except Exception:
        return default

def lag(high, low):
    try:
        high = int(high)
        low = int(low)
    except Exception:
        return -1
    if high < 0 or low < 0 or high < low:
        return -1
    return high - low

def stage_bottleneck(candidates):
    valid = [(name, value) for name, value in candidates if value >= 0]
    if not valid:
        return "unknown", -1
    name, value = max(valid, key=lambda item: item[1])
    if value == 0:
        return "none", 0
    return name, value

def allocated_bytes(path):
    try:
        stat = path.stat()
    except Exception:
        return 0
    blocks = getattr(stat, "st_blocks", 0)
    if blocks:
        return int(blocks) * 512
    return int(stat.st_size)

def snapshot_derived_index_stats(datadir_path):
    root = Path(datadir_path) / "gtron" / "state-snapshots"
    if not root.exists():
        return 0, 0
    markers = (
        "chain-freezer-accessor",
        "chain-index",
        "balance-trace",
        "section-bloom",
        "event-log",
    )
    total_bytes = 0
    files = 0
    try:
        paths = root.rglob("*")
        for path in paths:
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            if not any(marker in rel for marker in markers):
                continue
            total_bytes += allocated_bytes(path)
            files += 1
    except Exception:
        return 0, 0
    return total_bytes, files

nowblock = load_json(nowblock_path)
nodeinfo = load_json(nodeinfo_path)
nodes = load_json(nodes_path)
raw = nowblock.get("block_header", {}).get("raw_data", {})
height = int(raw.get("number", 0) or 0)
block_id = str(nowblock.get("blockID", ""))
nodeinfo_current = int(nodeinfo.get("currentBlock", 0) or 0)
peers = len(nodes.get("nodes", []) or [])
now = int(time.time())
start = int(start_unix)
elapsed = now - start if start > 0 and now >= start else -1
blocks_per_second = float(height) / elapsed if elapsed > 0 and height > 0 else 0.0
blocks_per_minute = blocks_per_second * 60.0
alerts_text = Path(storage_alerts_path).read_text(encoding="utf-8", errors="replace")
alerts = parse_alerts(alerts_text)
stages = parse_stage_status(stage_status_path)
height_delta = nodeinfo_current - height if nodeinfo_current > 0 and height > 0 else 0
total = int(total_bytes)
chaindata = int(chaindata_bytes)
ancient = int(ancient_bytes)
snapshot = int(snapshot_bytes)
replay = int(replay_bytes)
derived_index, derived_index_files = snapshot_derived_index_stats(datadir)
cold_archive = ancient + snapshot
bytes_per_block = float(total) / height if height > 0 else 0.0
chaindata_bytes_per_block = float(chaindata) / height if height > 0 else 0.0
cold_archive_bytes_per_block = float(cold_archive) / height if height > 0 else 0.0
derived_index_bytes_per_block = float(derived_index) / height if height > 0 else 0.0
cold_to_hot_ratio = float(cold_archive) / chaindata if chaindata > 0 else 0.0
derived_index_to_hot_ratio = float(derived_index) / chaindata if chaindata > 0 else 0.0
derived_index_snapshot_ratio = float(derived_index) / snapshot if snapshot > 0 else 0.0
previous = load_previous_sample(output)
previous_unix = number(previous, "unix", 0)
previous_height = number(previous, "height", 0)
interval_seconds = now - previous_unix if previous_unix > 0 and now >= previous_unix else -1
interval_blocks = height - previous_height if interval_seconds > 0 and height >= previous_height else 0
interval_blocks_per_second = float(interval_blocks) / interval_seconds if interval_seconds > 0 else 0.0
datadir_bytes_delta = total - number(previous, "datadirBytes", total) if interval_seconds > 0 else 0
chaindata_bytes_delta = chaindata - number(previous, "chaindataBytes", chaindata) if interval_seconds > 0 else 0
ancient_bytes_delta = ancient - number(previous, "ancientBytes", ancient) if interval_seconds > 0 else 0
snapshot_bytes_delta = snapshot - number(previous, "snapshotBytes", snapshot) if interval_seconds > 0 else 0
cold_archive_bytes_delta = cold_archive - number(previous, "coldArchiveBytes", cold_archive) if interval_seconds > 0 else 0
derived_index_bytes_delta = derived_index - number(previous, "derivedIndexBytes", derived_index) if interval_seconds > 0 else 0
datadir_bytes_per_second = float(datadir_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
chaindata_bytes_per_second = float(chaindata_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
cold_archive_bytes_per_second = float(cold_archive_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
derived_index_bytes_per_second = float(derived_index_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
datadir_bytes_per_interval_block = float(datadir_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
chaindata_bytes_per_interval_block = float(chaindata_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
ancient_bytes_per_interval_block = float(ancient_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
snapshot_bytes_per_interval_block = float(snapshot_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
cold_archive_bytes_per_interval_block = float(cold_archive_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
derived_index_bytes_per_interval_block = float(derived_index_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
stage_sync_finish_head_lag = lag(height, stages.get("stageSyncFinish", -1))
stage_sync_bottleneck, stage_sync_bottleneck_lag = stage_bottleneck([
    ("bodies-ready-gap", stages.get("stageSyncBodiesReadyGapBlocks", -1)),
    ("import-execution", stages.get("stageSyncImportExecutionLagBlocks", -1)),
    ("execution-commitment", stages.get("stageSyncExecutionCommitmentLagBlocks", -1)),
    ("commitment-finish", stages.get("stageSyncCommitmentFinishLagBlocks", -1)),
    ("finish-head", stage_sync_finish_head_lag),
])
if nowblock_status != "ok":
    sample_status = "http-nowblock-error"
elif nodeinfo_status != "ok":
    sample_status = "http-nodeinfo-error"
elif nodes_status != "ok":
    sample_status = "http-listnodes-error"
elif peers == 0:
    sample_status = "no-peers"
elif abs(height_delta) > 1:
    sample_status = "height-mismatch"
else:
    sample_status = "ok"

row = {
    "unix": now,
    "iso": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now)),
    "profile": "nile-sync-sample",
    "network": network,
    "mode": mode,
    "label": label,
    "http": http,
    "httpNowBlockStatus": nowblock_status,
    "httpNodeInfoStatus": nodeinfo_status,
    "httpListNodesStatus": nodes_status,
    "height": height,
    "nodeInfoCurrentBlock": nodeinfo_current,
    "nodeInfoHeightDelta": height_delta,
    "blockId": block_id,
    "peers": peers,
    "sampleStatus": sample_status,
    "elapsedSeconds": elapsed,
    "blocksPerSecond": blocks_per_second,
    "blocksPerMinute": blocks_per_minute,
    "intervalSeconds": interval_seconds,
    "intervalBlocks": interval_blocks,
    "intervalBlocksPerSecond": interval_blocks_per_second,
    "datadirBytes": total,
    "chaindataBytes": chaindata,
    "ancientBytes": ancient,
    "snapshotBytes": snapshot,
    "replayBytes": replay,
    "coldArchiveBytes": cold_archive,
    "derivedIndexBytes": derived_index,
    "derivedIndexFiles": derived_index_files,
    "bytesPerBlock": bytes_per_block,
    "chaindataBytesPerBlock": chaindata_bytes_per_block,
    "coldArchiveBytesPerBlock": cold_archive_bytes_per_block,
    "derivedIndexBytesPerBlock": derived_index_bytes_per_block,
    "coldToHotBytesRatio": cold_to_hot_ratio,
    "derivedIndexToHotBytesRatio": derived_index_to_hot_ratio,
    "derivedIndexSnapshotBytesRatio": derived_index_snapshot_ratio,
    "datadirBytesDelta": datadir_bytes_delta,
    "chaindataBytesDelta": chaindata_bytes_delta,
    "ancientBytesDelta": ancient_bytes_delta,
    "snapshotBytesDelta": snapshot_bytes_delta,
    "coldArchiveBytesDelta": cold_archive_bytes_delta,
    "derivedIndexBytesDelta": derived_index_bytes_delta,
    "datadirBytesPerSecond": datadir_bytes_per_second,
    "chaindataBytesPerSecond": chaindata_bytes_per_second,
    "coldArchiveBytesPerSecond": cold_archive_bytes_per_second,
    "derivedIndexBytesPerSecond": derived_index_bytes_per_second,
    "intervalDatadirBytesPerBlock": datadir_bytes_per_interval_block,
    "intervalChaindataBytesPerBlock": chaindata_bytes_per_interval_block,
    "intervalAncientBytesPerBlock": ancient_bytes_per_interval_block,
    "intervalSnapshotBytesPerBlock": snapshot_bytes_per_interval_block,
    "intervalColdArchiveBytesPerBlock": cold_archive_bytes_per_interval_block,
    "intervalDerivedIndexBytesPerBlock": derived_index_bytes_per_interval_block,
    "stageSyncFinishHeadLagBlocks": stage_sync_finish_head_lag,
    "stageSyncBottleneck": stage_sync_bottleneck,
    "stageSyncBottleneckLagBlocks": stage_sync_bottleneck_lag,
    "ancientFiles": int(ancient_files),
    "snapshotFiles": int(snapshot_files),
    "coldArchiveFiles": int(ancient_files) + int(snapshot_files),
    "offlineDbCheck": bool(int(offline_enabled)),
    "offlineDbCheckStatus": offline_status,
    "offlineDbCheckExit": int(offline_exit),
    "gitCommit": git_commit,
    "gitDirty": git_dirty == "true",
    "datadir": datadir,
}
row.update(alerts)
row.update(stages)
if offline_status == "error":
    row["offlineDbCheckTail"] = "\n".join(alerts_text.splitlines()[-5:])

line = json.dumps(row, sort_keys=True)
if output:
    with open(output, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")
print(line)

if bool(int(strict_offline)) and offline_status != "ok":
    sys.exit(2)
PY
