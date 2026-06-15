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
SYNC_LOG_FILE=""
PID_FILE=""
DEBUG_METRICS_URL=""
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
  --sync-log-file FILE       Parse latest `Imported chain segment` log fields
  --pid-file FILE            Read gtron pid and emit process RSS/CPU/uptime/FD stats
  --debug-metrics-url URL    Fetch optional /debug/metrics JSON into the sample row
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
    --sync-log-file) SYNC_LOG_FILE="${2:?}"; shift 2 ;;
    --pid-file) PID_FILE="${2:?}"; shift 2 ;;
    --debug-metrics-url) DEBUG_METRICS_URL="${2:?}"; shift 2 ;;
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
if [ -n "$SYNC_LOG_FILE" ] && [ ! -r "$SYNC_LOG_FILE" ]; then
  die "--sync-log-file is not readable: $SYNC_LOG_FILE"
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
debug_metrics_json="$tmpdir/debug-metrics.json"

nowblock_status="ok"
nodeinfo_status="ok"
nodes_status="ok"
debug_metrics_status="skipped"
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
if [ -n "$DEBUG_METRICS_URL" ]; then
  if curl -sf --max-time 5 "$DEBUG_METRICS_URL" >"$debug_metrics_json"; then
    debug_metrics_status="ok"
  else
    debug_metrics_status="error"
    : >"$debug_metrics_json"
  fi
else
  : >"$debug_metrics_json"
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
  "$STAGE_STATUS_FILE" "$SYNC_LOG_FILE" "$PID_FILE" \
  "$DEBUG_METRICS_URL" "$debug_metrics_json" "$debug_metrics_status" \
  "$nowblock_status" "$nodeinfo_status" "$nodes_status" \
  "$offline_status" "$offline_exit" "$OFFLINE_DB_CHECK" "$STRICT_OFFLINE_DB_CHECK" \
  "$START_UNIX" "$total_bytes" "$chaindata_bytes" "$ancient_bytes" "$snapshot_bytes" \
  "$replay_bytes" "$ancient_files" "$snapshot_files" "$git_commit" "$git_dirty" <<'PY'
import json
import os
import re
import subprocess
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
    sync_log_path,
    pid_file,
    debug_metrics_url,
    debug_metrics_path,
    debug_metrics_status,
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

def parse_log_value(value):
    text = str(value).strip().strip('"')
    if text == "":
        return text
    lower = text.lower()
    if lower == "true":
        return True
    if lower == "false":
        return False
    try:
        if re.fullmatch(r"-?[0-9]+", text):
            return int(text)
        if re.fullmatch(r"-?[0-9]+\.[0-9]+", text):
            return float(text)
    except Exception:
        pass
    return text

def parse_logfmt_fields(line):
    fields = {}
    for match in re.finditer(r'([A-Za-z0-9_./-]+)=("([^"\\]|\\.)*"|[^ ]+)', line):
        key = match.group(1)
        raw = match.group(2)
        fields[key] = parse_log_value(raw)
    return fields

def imported_segment_fields_from_line(line):
    text = line.strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            msg = obj.get("msg") or obj.get("message") or ""
            if msg == "Imported chain segment":
                return obj
            return None
    except Exception:
        pass
    if "Imported chain segment details" in text:
        return None
    if "Imported chain segment" not in text:
        return None
    fields = parse_logfmt_fields(text)
    return fields if fields else {}

def startup_repair_fields_from_line(line):
    text = line.strip()
    if not text:
        return None
    try:
        obj = json.loads(text)
        if isinstance(obj, dict):
            msg = obj.get("msg") or obj.get("message") or ""
            if msg == "Sync startup repair summary":
                return obj
            return None
    except Exception:
        pass
    if "Sync startup repair summary" not in text:
        return None
    fields = parse_logfmt_fields(text)
    return fields if fields else {}

def parse_sync_log(path):
    row = {
        "syncLogFile": path,
        "syncLogStatus": "skipped" if not path else "missing",
        "syncLogImportedSegments": 0,
        "syncLogSegmentBlocks": -1,
        "syncLogSegmentTxs": -1,
        "syncLogSegmentHead": -1,
        "syncLogSegmentRemain": -1,
        "syncLogSegmentElapsed": "",
        "syncLogSegmentExecElapsed": "",
        "syncLogSegmentApplyElapsed": "",
        "syncLogBlocksPerSecond": -1.0,
        "syncLogTxsPerSecond": -1.0,
        "syncLogSlowPhase": "",
        "syncLogSlowElapsed": "",
        "syncLogSlowStateCommitPhase": "",
        "syncLogSlowStateCommitElapsed": "",
        "syncLogStateMutTop": "",
        "syncLogStateMutKVTop": "",
        "syncLogPeer": "",
        "syncLogStageComplete": False,
        "syncLogStageCompleted": -1,
        "syncLogStageScheduled": -1,
        "syncLogStageIncomplete": -1,
        "syncLogStageCompletionRatio": -1.0,
        "syncLogStageTasksPerBlock": -1.0,
        "syncLogStageCompletedPerBlock": -1.0,
        "syncLogStageNext": "",
        "syncLogStageNextBlock": -1,
        "syncLogStageNextCanonical": "",
        "syncLogStageNextSync": "",
        "syncLogStageBlockedStatus": "",
        "syncLogPhaseCursorComplete": False,
        "syncLogPhaseCursorCompletedPhases": -1,
        "syncLogPhaseCursorScheduledPhases": -1,
        "syncLogPhaseCursorIncompletePhases": -1,
        "syncLogPhaseCursorCompletionRatio": -1.0,
        "syncLogPhaseCursorCompletedTasks": -1,
        "syncLogPhaseCursorScheduledTasks": -1,
        "syncLogPhaseCursorTaskCompletionRatio": -1.0,
        "syncLogPhaseCursorCurrent": "",
        "syncLogPhaseCursorCurrentCanonical": "",
        "syncLogPhaseCursorCurrentSync": "",
        "syncLogPhaseCursorCurrentTaskIndex": -1,
        "syncLogPhaseCursorNextBlock": -1,
        "syncLogPhaseCursorBlockedStatus": "",
        "syncLogExecPlanBlocks": -1,
        "syncLogExecPlanStages": -1,
        "syncLogExecPlanBodyStages": -1,
        "syncLogExecPlanPostBodyStages": -1,
        "syncLogExecPlanExecutionStages": -1,
        "syncLogExecPlanCommitmentStages": -1,
        "syncLogExecPlanFinishStages": -1,
        "syncLogExecPlanFirst": -1,
        "syncLogExecPlanLast": -1,
        "syncLogExecPlanStagesPerBlock": -1.0,
        "syncLogExecPlanPostBodyStagesPerBlock": -1.0,
        "syncLogAppliedPlanBlocks": -1,
        "syncLogAppliedPlanStages": -1,
        "syncLogAppliedPlanBodyStages": -1,
        "syncLogAppliedPlanPostBodyStages": -1,
        "syncLogAppliedPlanExecutionStages": -1,
        "syncLogAppliedPlanCommitmentStages": -1,
        "syncLogAppliedPlanFinishStages": -1,
        "syncLogAppliedPlanFirst": -1,
        "syncLogAppliedPlanLast": -1,
        "syncLogAppliedPlanStagesPerBlock": -1.0,
        "syncLogAppliedPlanPostBodyStagesPerBlock": -1.0,
        "syncStartupRepairStatus": "skipped" if not path else "missing",
        "syncStartupRepairSummaries": 0,
        "syncStartupRepairComplete": False,
        "syncStartupRepairKept": -1,
        "syncStartupRepairMissing": -1,
        "syncStartupRepairDeleted": -1,
        "syncStartupRepairHasBlocked": False,
        "syncStartupRepairFirstBlocked": "",
        "syncStartupRepairInterrupted": False,
        "syncStartupRepairErrorStage": "",
        "syncStartupRepairRows": -1,
        "syncStartupPipelineOrderChecked": False,
        "syncStartupPipelineOrderIssues": -1,
        "syncStartupPipelineOrderFirstIssue": "",
        "syncStartupPipelineOrderReadErrors": -1,
        "syncStartupPipelineOrderFirstReadErrorStage": "",
        "syncStartupPipelineOrderRepairChecked": False,
        "syncStartupPipelineOrderRepairComplete": False,
        "syncStartupPipelineOrderRepairDeleted": -1,
        "syncStartupPipelineOrderRepairUpdated": -1,
        "syncStartupPipelineOrderRepairInterrupted": False,
        "syncStartupPipelineOrderRepairErrorStage": "",
        "syncStartupPipelineOrderRepairRows": -1,
        "syncStartupPipelineCursorChecked": False,
        "syncStartupPipelineCursorComplete": False,
        "syncStartupPipelineCursorRows": -1,
        "syncStartupPipelineCursorHasLast": False,
        "syncStartupPipelineCursorLastStage": "",
        "syncStartupPipelineCursorLastBlock": -1,
        "syncStartupPipelineCursorLastHasHash": False,
        "syncStartupPipelineCursorHasNext": False,
        "syncStartupPipelineCursorNextStage": "",
        "syncStartupPipelineCursorBlocked": False,
        "syncStartupPipelineCursorInterrupted": False,
        "syncStartupPipelineCursorErrorStage": "",
        "syncStartupStagedRestored": -1,
        "syncStartupStagedTargetHead": -1,
        "syncStartupStagedNextExpected": -1,
        "syncStartupStagedNeedPruneTail": False,
        "syncStartupStagedPruneFrom": -1,
        "syncStartupStagedHaveLastRestored": False,
        "syncStartupStagedLastRestored": -1,
    }
    if not path:
        return row
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return row
    latest = None
    count = 0
    latest_startup = None
    startup_count = 0
    for line in lines:
        fields = imported_segment_fields_from_line(line)
        if fields is not None:
            count += 1
            latest = fields
        startup_fields = startup_repair_fields_from_line(line)
        if startup_fields is not None:
            startup_count += 1
            latest_startup = startup_fields
    row["syncLogImportedSegments"] = count
    row["syncStartupRepairSummaries"] = startup_count
    if latest_startup is not None:
        row["syncStartupRepairStatus"] = "ok"
        startup_mappings = {
            "syncStartupRepairComplete": "syncStartupRepairComplete",
            "syncStartupRepairKept": "syncStartupRepairKept",
            "syncStartupRepairMissing": "syncStartupRepairMissing",
            "syncStartupRepairDeleted": "syncStartupRepairDeleted",
            "syncStartupRepairHasBlocked": "syncStartupRepairHasBlocked",
            "syncStartupRepairFirstBlocked": "syncStartupRepairFirstBlocked",
            "syncStartupRepairInterrupted": "syncStartupRepairInterrupted",
            "syncStartupRepairErrorStage": "syncStartupRepairErrorStage",
            "syncStartupRepairRows": "syncStartupRepairRows",
            "syncStartupPipelineOrderChecked": "syncStartupPipelineOrderChecked",
            "syncStartupPipelineOrderIssues": "syncStartupPipelineOrderIssues",
            "syncStartupPipelineOrderFirstIssue": "syncStartupPipelineOrderFirstIssue",
            "syncStartupPipelineOrderReadErrors": "syncStartupPipelineOrderReadErrors",
            "syncStartupPipelineOrderFirstReadErrorStage": "syncStartupPipelineOrderFirstReadErrorStage",
            "syncStartupPipelineOrderRepairChecked": "syncStartupPipelineOrderRepairChecked",
            "syncStartupPipelineOrderRepairComplete": "syncStartupPipelineOrderRepairComplete",
            "syncStartupPipelineOrderRepairDeleted": "syncStartupPipelineOrderRepairDeleted",
            "syncStartupPipelineOrderRepairUpdated": "syncStartupPipelineOrderRepairUpdated",
            "syncStartupPipelineOrderRepairInterrupted": "syncStartupPipelineOrderRepairInterrupted",
            "syncStartupPipelineOrderRepairErrorStage": "syncStartupPipelineOrderRepairErrorStage",
            "syncStartupPipelineOrderRepairRows": "syncStartupPipelineOrderRepairRows",
            "syncStartupPipelineCursorChecked": "syncStartupPipelineCursorChecked",
            "syncStartupPipelineCursorComplete": "syncStartupPipelineCursorComplete",
            "syncStartupPipelineCursorRows": "syncStartupPipelineCursorRows",
            "syncStartupPipelineCursorHasLast": "syncStartupPipelineCursorHasLast",
            "syncStartupPipelineCursorLastStage": "syncStartupPipelineCursorLastStage",
            "syncStartupPipelineCursorLastBlock": "syncStartupPipelineCursorLastBlock",
            "syncStartupPipelineCursorLastHasHash": "syncStartupPipelineCursorLastHasHash",
            "syncStartupPipelineCursorHasNext": "syncStartupPipelineCursorHasNext",
            "syncStartupPipelineCursorNextStage": "syncStartupPipelineCursorNextStage",
            "syncStartupPipelineCursorBlocked": "syncStartupPipelineCursorBlocked",
            "syncStartupPipelineCursorInterrupted": "syncStartupPipelineCursorInterrupted",
            "syncStartupPipelineCursorErrorStage": "syncStartupPipelineCursorErrorStage",
            "syncStartupStagedRestored": "syncStartupStagedRestored",
            "syncStartupStagedTargetHead": "syncStartupStagedTargetHead",
            "syncStartupStagedNextExpected": "syncStartupStagedNextExpected",
            "syncStartupStagedNeedPruneTail": "syncStartupStagedNeedPruneTail",
            "syncStartupStagedPruneFrom": "syncStartupStagedPruneFrom",
            "syncStartupStagedHaveLastRestored": "syncStartupStagedHaveLastRestored",
            "syncStartupStagedLastRestored": "syncStartupStagedLastRestored",
        }
        for source, dest in startup_mappings.items():
            if source in latest_startup:
                row[dest] = parse_log_value(latest_startup[source])
    elif row["syncStartupRepairStatus"] != "skipped":
        row["syncStartupRepairStatus"] = "no-summary"
    if latest is None:
        row["syncLogStatus"] = "no-segment"
        return row
    row["syncLogStatus"] = "ok"
    mappings = {
        "blocks": "syncLogSegmentBlocks",
        "txs": "syncLogSegmentTxs",
        "head": "syncLogSegmentHead",
        "remain": "syncLogSegmentRemain",
        "elapsed": "syncLogSegmentElapsed",
        "execElapsed": "syncLogSegmentExecElapsed",
        "applyElapsed": "syncLogSegmentApplyElapsed",
        "blocks/s": "syncLogBlocksPerSecond",
        "txs/s": "syncLogTxsPerSecond",
        "slowPhase": "syncLogSlowPhase",
        "slowElapsed": "syncLogSlowElapsed",
        "slowStateCommitPhase": "syncLogSlowStateCommitPhase",
        "slowStateCommitElapsed": "syncLogSlowStateCommitElapsed",
        "stateMutTop": "syncLogStateMutTop",
        "stateMutKVTop": "syncLogStateMutKVTop",
        "peer": "syncLogPeer",
        "syncStageComplete": "syncLogStageComplete",
        "syncStageCompleted": "syncLogStageCompleted",
        "syncStageScheduled": "syncLogStageScheduled",
        "syncStageNext": "syncLogStageNext",
        "syncStageNextBlock": "syncLogStageNextBlock",
        "syncStageNextCanonical": "syncLogStageNextCanonical",
        "syncStageNextSync": "syncLogStageNextSync",
        "syncStageBlockedStatus": "syncLogStageBlockedStatus",
        "syncPhaseCursorComplete": "syncLogPhaseCursorComplete",
        "syncPhaseCursorCompletedPhases": "syncLogPhaseCursorCompletedPhases",
        "syncPhaseCursorScheduledPhases": "syncLogPhaseCursorScheduledPhases",
        "syncPhaseCursorCompletedTasks": "syncLogPhaseCursorCompletedTasks",
        "syncPhaseCursorScheduledTasks": "syncLogPhaseCursorScheduledTasks",
        "syncPhaseCursorCurrent": "syncLogPhaseCursorCurrent",
        "syncPhaseCursorCurrentCanonical": "syncLogPhaseCursorCurrentCanonical",
        "syncPhaseCursorCurrentSync": "syncLogPhaseCursorCurrentSync",
        "syncPhaseCursorCurrentTaskIndex": "syncLogPhaseCursorCurrentTaskIndex",
        "syncPhaseCursorNextBlock": "syncLogPhaseCursorNextBlock",
        "syncPhaseCursorBlockedStatus": "syncLogPhaseCursorBlockedStatus",
        "syncExecPlanBlocks": "syncLogExecPlanBlocks",
        "syncExecPlanStages": "syncLogExecPlanStages",
        "syncExecPlanBodyStages": "syncLogExecPlanBodyStages",
        "syncExecPlanPostBodyStages": "syncLogExecPlanPostBodyStages",
        "syncExecPlanExecutionStages": "syncLogExecPlanExecutionStages",
        "syncExecPlanCommitmentStages": "syncLogExecPlanCommitmentStages",
        "syncExecPlanFinishStages": "syncLogExecPlanFinishStages",
        "syncExecPlanFirst": "syncLogExecPlanFirst",
        "syncExecPlanLast": "syncLogExecPlanLast",
        "syncAppliedPlanBlocks": "syncLogAppliedPlanBlocks",
        "syncAppliedPlanStages": "syncLogAppliedPlanStages",
        "syncAppliedPlanBodyStages": "syncLogAppliedPlanBodyStages",
        "syncAppliedPlanPostBodyStages": "syncLogAppliedPlanPostBodyStages",
        "syncAppliedPlanExecutionStages": "syncLogAppliedPlanExecutionStages",
        "syncAppliedPlanCommitmentStages": "syncLogAppliedPlanCommitmentStages",
        "syncAppliedPlanFinishStages": "syncLogAppliedPlanFinishStages",
        "syncAppliedPlanFirst": "syncLogAppliedPlanFirst",
        "syncAppliedPlanLast": "syncLogAppliedPlanLast",
    }
    for source, dest in mappings.items():
        if source in latest:
            row[dest] = parse_log_value(latest[source])
    scheduled = number(row, "syncLogStageScheduled", -1)
    completed = number(row, "syncLogStageCompleted", -1)
    segment_blocks = number(row, "syncLogSegmentBlocks", -1)
    exec_blocks = number(row, "syncLogExecPlanBlocks", -1)
    applied_blocks = number(row, "syncLogAppliedPlanBlocks", -1)
    phase_cursor_scheduled = number(row, "syncLogPhaseCursorScheduledPhases", -1)
    phase_cursor_completed = number(row, "syncLogPhaseCursorCompletedPhases", -1)
    phase_cursor_task_scheduled = number(row, "syncLogPhaseCursorScheduledTasks", -1)
    phase_cursor_task_completed = number(row, "syncLogPhaseCursorCompletedTasks", -1)
    if scheduled >= 0 and completed >= 0:
        row["syncLogStageIncomplete"] = max(scheduled - completed, 0)
        row["syncLogStageCompletionRatio"] = ratio(completed, scheduled)
    if phase_cursor_scheduled >= 0 and phase_cursor_completed >= 0:
        row["syncLogPhaseCursorIncompletePhases"] = max(phase_cursor_scheduled - phase_cursor_completed, 0)
        row["syncLogPhaseCursorCompletionRatio"] = ratio(phase_cursor_completed, phase_cursor_scheduled)
    if phase_cursor_task_scheduled >= 0 and phase_cursor_task_completed >= 0:
        row["syncLogPhaseCursorTaskCompletionRatio"] = ratio(phase_cursor_task_completed, phase_cursor_task_scheduled)
    if segment_blocks > 0:
        row["syncLogStageTasksPerBlock"] = ratio(scheduled, segment_blocks)
        row["syncLogStageCompletedPerBlock"] = ratio(completed, segment_blocks)
    if exec_blocks > 0:
        row["syncLogExecPlanStagesPerBlock"] = ratio(row["syncLogExecPlanStages"], exec_blocks)
        row["syncLogExecPlanPostBodyStagesPerBlock"] = ratio(row["syncLogExecPlanPostBodyStages"], exec_blocks)
    if applied_blocks > 0:
        row["syncLogAppliedPlanStagesPerBlock"] = ratio(row["syncLogAppliedPlanStages"], applied_blocks)
        row["syncLogAppliedPlanPostBodyStagesPerBlock"] = ratio(row["syncLogAppliedPlanPostBodyStages"], applied_blocks)
    return row

def parse_etime_seconds(value):
    try:
        text = str(value).strip()
        if not text:
            return -1
        days = 0
        if "-" in text:
            day_text, text = text.split("-", 1)
            days = int(day_text)
        parts = [int(part) for part in text.split(":")]
        if len(parts) == 3:
            hours, minutes, seconds = parts
        elif len(parts) == 2:
            hours = 0
            minutes, seconds = parts
        else:
            return -1
        return days * 86400 + hours * 3600 + minutes * 60 + seconds
    except Exception:
        return -1

def process_open_files(pid):
    proc_fd = Path("/proc") / str(pid) / "fd"
    try:
        if proc_fd.exists():
            return len(list(proc_fd.iterdir()))
    except Exception:
        return -1
    try:
        result = subprocess.run(
            ["lsof", "-n", "-p", str(pid)],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        if result.returncode == 0:
            lines = [line for line in result.stdout.splitlines() if line.strip()]
            return max(0, len(lines) - 1)
    except Exception:
        pass
    return -1

def read_process_stats(path):
    row = {
        "processPidFile": path,
        "processStatus": "skipped" if not path else "missing",
        "processPid": 0,
        "processRssBytes": -1,
        "processCpuPercent": -1.0,
        "processUptimeSeconds": -1,
        "processOpenFiles": -1,
    }
    if not path:
        return row
    try:
        pid_text = Path(path).read_text(encoding="utf-8").strip().split()[0]
    except Exception:
        return row
    try:
        pid = int(pid_text)
        if pid <= 0:
            raise ValueError("non-positive pid")
    except Exception:
        row["processStatus"] = "invalid"
        return row
    row["processPid"] = pid
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        row["processStatus"] = "not-running"
        return row
    except PermissionError:
        pass
    except Exception:
        row["processStatus"] = "unknown"
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "rss=", "-o", "pcpu=", "-o", "etime="],
            check=False,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=2,
        )
        if result.returncode != 0:
            row["processStatus"] = "ps-error"
            return row
        line = result.stdout.strip().splitlines()[-1].strip()
        fields = line.split(None, 2)
        if len(fields) >= 1:
            row["processRssBytes"] = int(float(fields[0]) * 1024)
        if len(fields) >= 2:
            row["processCpuPercent"] = float(fields[1])
        if len(fields) >= 3:
            row["processUptimeSeconds"] = parse_etime_seconds(fields[2])
        row["processOpenFiles"] = process_open_files(pid)
        row["processStatus"] = "ok"
    except Exception:
        row["processStatus"] = "ps-error"
    return row

def parse_debug_metrics(url, path, status):
    row = {
        "debugMetricsURL": url,
        "debugMetricsStatus": status,
        "debugMetricsPrefix": "",
        "debugMetricsCount": -1,
        "debugMetricsNumericCount": 0,
        "debugMetricsNames": [],
        "debugMetrics": {},
        "debugMetricChainFreezerBlocks": -1,
        "debugMetricChainFreezerPasses": -1,
        "debugMetricChainFreezerLastPassDuration": -1,
        "debugMetricChainFreezerPebbleSize": -1,
    }
    if status != "ok":
        return row
    data = load_json(path)
    if not isinstance(data, dict):
        row["debugMetricsStatus"] = "invalid"
        return row
    metrics = data.get("metrics", [])
    if not isinstance(metrics, list):
        row["debugMetricsStatus"] = "invalid"
        return row
    row["debugMetricsPrefix"] = str(data.get("prefix", ""))
    try:
        row["debugMetricsCount"] = int(data.get("count", len(metrics)))
    except Exception:
        row["debugMetricsCount"] = len(metrics)
    for metric in metrics:
        if not isinstance(metric, dict):
            continue
        name = str(metric.get("name", ""))
        values = metric.get("values", {})
        if not name or not isinstance(values, dict):
            continue
        clean_values = {}
        for key, value in values.items():
            if isinstance(value, bool):
                clean_values[str(key)] = value
                continue
            if isinstance(value, (int, float)):
                clean_values[str(key)] = value
                row["debugMetricsNumericCount"] += 1
                continue
            clean_values[str(key)] = str(value)
        row["debugMetricsNames"].append(name)
        row["debugMetrics"][name] = clean_values

    def metric_value(name):
        value = row["debugMetrics"].get(name, {}).get("value", -1)
        try:
            return int(value)
        except Exception:
            return -1

    row["debugMetricChainFreezerBlocks"] = metric_value("chain/freezer/blocks")
    row["debugMetricChainFreezerPasses"] = metric_value("chain/freezer/passes")
    row["debugMetricChainFreezerLastPassDuration"] = metric_value("chain/freezer/lastpass/duration")
    row["debugMetricChainFreezerPebbleSize"] = metric_value("chain/freezer/pebble/size")
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

def lag_sum(values):
    valid = []
    for value in values:
        try:
            value = int(value)
        except Exception:
            continue
        if value >= 0:
            valid.append(value)
    if not valid:
        return -1
    return sum(valid)

def stage_pipeline_violations(stages):
    pairs = [
        ("bodies-ready", "stageSyncBodies", "stageSyncBodiesReady"),
        ("ready-import", "stageSyncBodiesReady", "stageSyncImport"),
        ("import-execution", "stageSyncImport", "stageSyncExecution"),
        ("execution-commitment", "stageSyncExecution", "stageSyncCommitment"),
        ("commitment-finish", "stageSyncCommitment", "stageSyncFinish"),
    ]
    violations = []
    for name, upstream_key, downstream_key in pairs:
        upstream = number(stages, upstream_key, -1)
        downstream = number(stages, downstream_key, -1)
        if upstream < 0 or downstream < 0:
            continue
        if downstream <= upstream:
            continue
        violations.append({
            "name": name,
            "upstreamStage": upstream_key,
            "upstreamValue": upstream,
            "downstreamStage": downstream_key,
            "downstreamValue": downstream,
            "violationBlocks": downstream - upstream,
        })
    return violations

def full_staged_sync_summary(stages, pipeline_violations, bottleneck, bottleneck_lag, pipeline_lag, finish_head_lag, head):
    required = [
        ("SyncBodies", "stageSyncBodies"),
        ("SyncBodiesReady", "stageSyncBodiesReady"),
        ("SyncImport", "stageSyncImport"),
        ("SyncExecution", "stageSyncExecution"),
        ("SyncCommitment", "stageSyncCommitment"),
        ("SyncFinish", "stageSyncFinish"),
    ]
    row = {
        "fullStagedSyncStatus": "unknown",
        "fullStagedSyncReady": False,
        "fullStagedSyncCompleteAtHead": False,
        "fullStagedSyncRequiredStages": [name for name, _ in required],
        "fullStagedSyncStageCount": len(required),
        "fullStagedSyncPresentStageCount": 0,
        "fullStagedSyncVerifiedStageCount": 0,
        "fullStagedSyncMissingStages": [],
        "fullStagedSyncHashIssues": [],
        "fullStagedSyncUnverifiedStages": [],
        "fullStagedSyncCompleteBlock": -1,
        "fullStagedSyncHeadBlock": head,
        "fullStagedSyncMinStage": "",
        "fullStagedSyncMinStageBlock": -1,
        "fullStagedSyncHeadLagBlocks": finish_head_lag,
        "fullStagedSyncCompletionRatio": -1.0,
        "fullStagedSyncPipelineLagBlocks": pipeline_lag,
        "fullStagedSyncBottleneck": bottleneck,
        "fullStagedSyncBottleneckLagBlocks": bottleneck_lag,
        "fullStagedSyncBottleneckLagShare": ratio(bottleneck_lag, pipeline_lag),
        "fullStagedSyncStageCoverageRatio": 0.0,
        "fullStagedSyncVerificationRatio": 0.0,
    }
    if stages.get("stageStatusFileStatus") != "ok":
        return row

    present_values = []
    progress = stages.get("stageProgress", {})
    for name, field in required:
        value = number(stages, field, -1)
        entry = progress.get(name, {})
        present = bool(entry.get("present")) and value >= 0
        if not present:
            row["fullStagedSyncMissingStages"].append(name)
            continue
        row["fullStagedSyncPresentStageCount"] += 1
        present_values.append((name, value))
        verified = str(entry.get("verified", ""))
        if verified == "canonical":
            row["fullStagedSyncVerifiedStageCount"] += 1
        elif verified:
            row["fullStagedSyncHashIssues"].append({"stage": name, "verified": verified})
        else:
            row["fullStagedSyncUnverifiedStages"].append(name)

    if present_values:
        min_stage, min_value = min(present_values, key=lambda item: item[1])
        row["fullStagedSyncMinStage"] = min_stage
        row["fullStagedSyncMinStageBlock"] = min_value
    row["fullStagedSyncCompleteBlock"] = number(stages, "stageSyncFinish", -1)
    row["fullStagedSyncCompletionRatio"] = ratio(row["fullStagedSyncCompleteBlock"], head)
    row["fullStagedSyncStageCoverageRatio"] = ratio(row["fullStagedSyncPresentStageCount"], row["fullStagedSyncStageCount"])
    row["fullStagedSyncVerificationRatio"] = ratio(row["fullStagedSyncVerifiedStageCount"], row["fullStagedSyncStageCount"])

    if row["fullStagedSyncMissingStages"]:
        row["fullStagedSyncStatus"] = "missing-stage"
    elif row["fullStagedSyncHashIssues"]:
        row["fullStagedSyncStatus"] = "hash-issue"
    elif pipeline_violations:
        row["fullStagedSyncStatus"] = "pipeline-violation"
    elif finish_head_lag > 0:
        row["fullStagedSyncStatus"] = "catching-up"
    else:
        row["fullStagedSyncStatus"] = "caught-up"

    row["fullStagedSyncReady"] = row["fullStagedSyncStatus"] in {"catching-up", "caught-up"}
    row["fullStagedSyncCompleteAtHead"] = row["fullStagedSyncReady"] and finish_head_lag == 0
    return row

def progress_regressions(current, previous_row, keys, missing_is_regression):
    if not previous_row:
        return []
    regressions = []
    for key in keys:
        previous_value = number(previous_row, key, -1)
        if previous_value < 0:
            continue
        current_value = number(current, key, -1)
        if current_value < 0 and not missing_is_regression:
            continue
        if current_value >= previous_value:
            continue
        regressions.append({
            "stage": key,
            "previousValue": previous_value,
            "currentValue": current_value,
            "regressionBlocks": previous_value - current_value if current_value >= 0 else previous_value,
        })
    return regressions

def stage_last_progress_map(previous_row):
    raw = previous_row.get("stageLastProgressUnix", {}) if previous_row else {}
    if not isinstance(raw, dict):
        return {}
    out = {}
    for key, value in raw.items():
        try:
            value = int(value)
        except Exception:
            continue
        if value > 0:
            out[key] = value
    return out

def stage_stagnation(current, previous_row, now, previous_unix, interval_seconds, height, target_height):
    keys = [
        "stageSyncBodies",
        "stageSyncBodiesReady",
        "stageSyncImport",
        "stageSyncExecution",
        "stageSyncCommitment",
        "stageSyncFinish",
        "stageChainFreezer",
        "stageSnapshotEventLogBuild",
    ]
    previous_last = stage_last_progress_map(previous_row)
    last_progress = {}
    stalls = []
    for key in keys:
        value = number(current, key, -1)
        if value < 0:
            continue
        previous_value = number(previous_row, key, -1)
        if previous_value >= 0 and value == previous_value:
            last = previous_last.get(key, previous_unix if previous_unix > 0 else now)
        else:
            last = now
        last_progress[key] = last
        lag_blocks = stage_stagnation_lag(current, key, height, target_height)
        stalled_seconds = now - last if interval_seconds > 0 and previous_value >= 0 and value == previous_value and lag_blocks > 0 else 0
        if stalled_seconds <= 0:
            continue
        stalls.append({
            "stage": key,
            "value": value,
            "previousValue": previous_value,
            "intervalBlocks": value - previous_value,
            "lagBlocks": lag_blocks,
            "stalledSeconds": stalled_seconds,
            "lastProgressUnix": last,
        })
    return last_progress, stalls

def stage_stagnation_lag(current, key, height, target_height):
    if key == "stageSyncBodies":
        return lag(target_height, current.get(key, -1))
    if key == "stageSyncBodiesReady":
        return lag(current.get("stageSyncBodies", -1), current.get(key, -1))
    if key == "stageSyncImport":
        return lag(current.get("stageSyncBodiesReady", -1), current.get(key, -1))
    if key == "stageSyncExecution":
        return lag(current.get("stageSyncImport", -1), current.get(key, -1))
    if key == "stageSyncCommitment":
        return lag(current.get("stageSyncExecution", -1), current.get(key, -1))
    if key == "stageSyncFinish":
        return lag(height, current.get(key, -1))
    if key == "stageChainFreezer":
        return lag(height, current.get(key, -1))
    if key == "stageSnapshotEventLogBuild":
        return lag(height, current.get(key, -1))
    return -1

def primary_stage_stall(stalls):
    if not stalls:
        return {}
    return max(stalls, key=lambda row: (row.get("stalledSeconds", 0), row.get("lagBlocks", 0)))

def ratio(numerator, denominator):
    try:
        numerator = float(numerator)
        denominator = float(denominator)
    except Exception:
        return -1.0
    if numerator < 0 or denominator <= 0:
        return -1.0
    return numerator / denominator

def nonnegative(value):
    try:
        value = float(value)
    except Exception:
        return 0.0
    return value if value > 0 else 0.0

def growth_share(delta, total_positive_growth):
    if total_positive_growth <= 0:
        return 0.0
    return ratio(nonnegative(delta), total_positive_growth)

def disk_growth_primary(candidates, total_positive_growth):
    positive = [(name, int(value)) for name, value in candidates if value > 0]
    if not positive or total_positive_growth <= 0:
        return "none", 0, 0.0
    name, value = max(positive, key=lambda item: item[1])
    return name, value, ratio(value, total_positive_growth)

def interval_stage_delta(current, previous_row, key, interval):
    try:
        current = int(current)
        previous_value = int(previous_row.get(key, -1))
    except Exception:
        return 0
    if interval <= 0 or current < 0 or previous_value < 0:
        return 0
    return current - previous_value

def interval_rate(delta, interval):
    return float(delta) / interval if interval > 0 else 0.0

def eta_seconds(lag_blocks, rate):
    try:
        lag_blocks = int(lag_blocks)
        rate = float(rate)
    except Exception:
        return -1.0
    if lag_blocks < 0 or rate <= 0:
        return -1.0
    return float(lag_blocks) / rate

def interval_stage_rate_summary(stage, field, blocks, rate, head_lag, head_eta):
    return {
        "stage": stage,
        "field": field,
        "blocks": int(blocks),
        "blocksPerSecond": float(rate),
        "blocksPerMinute": float(rate) * 60.0,
        "headLagBlocks": int(head_lag),
        "headEtaSeconds": float(head_eta),
    }

def build_soak_health(
    sample_status,
    height_regression_blocks,
    stage_progress_regressions,
    stage_sync_pipeline_violations,
    restart_recovery_status,
    stages,
    alerts,
    offline_status,
    offline_enabled,
    stage_sync_bottleneck,
    stage_sync_bottleneck_lag,
    sync_log,
    stage_stalled,
):
    critical = []
    warning = []

    def add(target, issue):
        if issue not in target:
            target.append(issue)

    if height_regression_blocks > 0:
        add(critical, "height-regression")
    if stage_progress_regressions:
        add(critical, "stage-progress-regression")
    if stage_sync_pipeline_violations:
        add(critical, "stage-pipeline-violation")
    if sample_status in {"http-nowblock-error", "http-nodeinfo-error", "http-listnodes-error", "no-peers"}:
        add(critical, f"sample-status:{sample_status}")
    elif sample_status != "ok":
        add(warning, f"sample-status:{sample_status}")
    if number(stages, "stageMismatchRows", 0) > 0:
        add(critical, "stage-hash-mismatch")
    if number(stages, "stageMissingCanonicalRows", 0) > 0:
        add(critical, "stage-missing-canonical")
    if number(stages, "stageUnboundRows", 0) > 0:
        add(warning, "stage-unbound-rows")
    if restart_recovery_status == "stalled":
        add(warning, "restart-stalled")
    if stage_stalled:
        add(warning, "stage-stalled")

    for field, issue in (
        ("freezerAlertStatus", "freezer-alert"),
        ("stageVerifyStatus", "stage-verify-alert"),
        ("snapshotAlertStatus", "snapshot-alert"),
    ):
        status = alerts.get(field, "unknown")
        if status == "critical":
            add(critical, issue)
        elif status == "warning":
            add(warning, issue)
    if bool(int(offline_enabled)) and offline_status != "ok":
        add(critical, f"offline-db-check:{offline_status}")

    if critical:
        status = "critical"
    elif warning:
        status = "warning"
    else:
        status = "ok"

    primary = "none"
    primary_source = "none"
    primary_lag = -1
    if critical:
        primary = critical[0]
        primary_source = "health"
    elif stage_sync_bottleneck not in {"", "none", "unknown"} and stage_sync_bottleneck_lag > 0:
        primary = stage_sync_bottleneck
        primary_source = "sync-stage"
        primary_lag = int(stage_sync_bottleneck_lag)
    elif sync_log.get("syncLogStageNext"):
        primary = f"sync-log:{sync_log.get('syncLogStageNext')}"
        primary_source = "sync-log-stage"
    elif warning:
        primary = warning[0]
        primary_source = "health"

    return {
        "soakHealthStatus": status,
        "soakHealthIssues": critical + warning,
        "soakHealthCriticalIssues": len(critical),
        "soakHealthWarningIssues": len(warning),
        "soakPrimaryBottleneck": primary,
        "soakPrimaryBottleneckSource": primary_source,
        "soakPrimaryBottleneckLagBlocks": primary_lag,
    }

def allocated_bytes(path):
    try:
        stat = path.stat()
    except Exception:
        return 0
    blocks = getattr(stat, "st_blocks", 0)
    if blocks:
        return int(blocks) * 512
    return int(stat.st_size)

def empty_bucket_stats(names):
    stats = {}
    for name in names:
        stats[name] = {"bytes": 0, "files": 0}
    return stats

def chaindata_file_stats(datadir_path):
    root = Path(datadir_path) / "gtron" / "chaindata"
    names = ("sst", "wal", "log", "manifest", "options", "other")
    stats = empty_bucket_stats(names)
    if not root.exists():
        return stats
    try:
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            filename = path.name
            if filename.endswith(".sst"):
                bucket = "sst"
            elif re.match(r"^[0-9]+\.log$", filename):
                bucket = "wal"
            elif filename == "LOG" or filename.startswith("LOG.") or filename.startswith("LOG.old"):
                bucket = "log"
            elif filename.startswith("MANIFEST"):
                bucket = "manifest"
            elif filename.startswith("OPTIONS"):
                bucket = "options"
            else:
                bucket = "other"
            stats[bucket]["bytes"] += allocated_bytes(path)
            stats[bucket]["files"] += 1
    except Exception:
        return empty_bucket_stats(names)
    return stats

def ancient_table_stats(datadir_path):
    root = Path(datadir_path) / "gtron" / "ancient"
    names = ("bodies", "txInfos", "stateRoots", "other")
    stats = empty_bucket_stats(names)
    if not root.exists():
        return stats
    table_names = {
        "bodies": "bodies",
        "tx_infos": "txInfos",
        "state_roots": "stateRoots",
    }
    try:
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            bucket = "other"
            filename = path.name
            for table, name in table_names.items():
                if filename == f"{table}.meta" or filename == f"{table}.cidx" or filename == f"{table}.ridx" or filename.startswith(f"{table}."):
                    bucket = name
                    break
            stats[bucket]["bytes"] += allocated_bytes(path)
            stats[bucket]["files"] += 1
    except Exception:
        return empty_bucket_stats(names)
    return stats

def snapshot_bucket_stats(datadir_path):
    root = Path(datadir_path) / "gtron" / "state-snapshots"
    names = ("root", "latest", "history", "chain", "log", "trace", "commitment", "retired", "other")
    stats = empty_bucket_stats(names)
    if not root.exists():
        return stats
    try:
        for path in root.rglob("*"):
            if not path.is_file():
                continue
            rel = path.relative_to(root)
            if len(rel.parts) == 1:
                bucket = "root"
            else:
                first = rel.parts[0]
                bucket = first if first in stats else "other"
            stats[bucket]["bytes"] += allocated_bytes(path)
            stats[bucket]["files"] += 1
    except Exception:
        return empty_bucket_stats(names)
    return stats

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
sync_log = parse_sync_log(sync_log_path)
process = read_process_stats(pid_file)
debug_metrics = parse_debug_metrics(debug_metrics_url, debug_metrics_path, debug_metrics_status)
height_delta = nodeinfo_current - height if nodeinfo_current > 0 and height > 0 else 0
sync_target_height = max(height, nodeinfo_current)
sync_target_lag_blocks = sync_target_height - height if sync_target_height > height else 0
total = int(total_bytes)
chaindata = int(chaindata_bytes)
ancient = int(ancient_bytes)
snapshot = int(snapshot_bytes)
replay = int(replay_bytes)
chaindata_files = chaindata_file_stats(datadir)
ancient_tables = ancient_table_stats(datadir)
snapshot_buckets = snapshot_bucket_stats(datadir)
derived_index, derived_index_files = snapshot_derived_index_stats(datadir)
cold_archive = ancient + snapshot
datadir_other = total - chaindata - ancient - snapshot - replay
bytes_per_block = float(total) / height if height > 0 else 0.0
chaindata_bytes_per_block = float(chaindata) / height if height > 0 else 0.0
cold_archive_bytes_per_block = float(cold_archive) / height if height > 0 else 0.0
derived_index_bytes_per_block = float(derived_index) / height if height > 0 else 0.0
cold_to_hot_ratio = float(cold_archive) / chaindata if chaindata > 0 else 0.0
derived_index_to_hot_ratio = float(derived_index) / chaindata if chaindata > 0 else 0.0
derived_index_snapshot_ratio = float(derived_index) / snapshot if snapshot > 0 else 0.0
cold_archive_datadir_share = ratio(cold_archive, total)
derived_index_cold_archive_ratio = ratio(derived_index, cold_archive)
chaindata_sst_to_hot_ratio = ratio(chaindata_files["sst"]["bytes"], chaindata)
chaindata_wal_to_hot_ratio = ratio(chaindata_files["wal"]["bytes"], chaindata)
chaindata_wal_to_sst_ratio = ratio(chaindata_files["wal"]["bytes"], chaindata_files["sst"]["bytes"])
previous = load_previous_sample(output)
previous_unix = number(previous, "unix", 0)
previous_height = number(previous, "height", 0)
interval_seconds = now - previous_unix if previous_unix > 0 and now >= previous_unix else -1
interval_blocks = height - previous_height if interval_seconds > 0 and height >= previous_height else 0
interval_blocks_per_second = float(interval_blocks) / interval_seconds if interval_seconds > 0 else 0.0
sync_eta_seconds = eta_seconds(sync_target_lag_blocks, blocks_per_second)
interval_sync_eta_seconds = eta_seconds(sync_target_lag_blocks, interval_blocks_per_second)
datadir_bytes_delta = total - number(previous, "datadirBytes", total) if interval_seconds > 0 else 0
chaindata_bytes_delta = chaindata - number(previous, "chaindataBytes", chaindata) if interval_seconds > 0 else 0
ancient_bytes_delta = ancient - number(previous, "ancientBytes", ancient) if interval_seconds > 0 else 0
snapshot_bytes_delta = snapshot - number(previous, "snapshotBytes", snapshot) if interval_seconds > 0 else 0
replay_bytes_delta = replay - number(previous, "replayBytes", replay) if interval_seconds > 0 else 0
cold_archive_bytes_delta = cold_archive - number(previous, "coldArchiveBytes", cold_archive) if interval_seconds > 0 else 0
derived_index_bytes_delta = derived_index - number(previous, "derivedIndexBytes", derived_index) if interval_seconds > 0 else 0
positive_hot_growth_bytes = nonnegative(chaindata_bytes_delta)
positive_cold_archive_growth_bytes = nonnegative(cold_archive_bytes_delta)
positive_ancient_growth_bytes = nonnegative(ancient_bytes_delta)
positive_snapshot_growth_bytes = nonnegative(snapshot_bytes_delta)
positive_derived_index_growth_bytes = nonnegative(derived_index_bytes_delta)
interval_cold_to_hot_growth_ratio = ratio(positive_cold_archive_growth_bytes, positive_hot_growth_bytes)
interval_ancient_to_hot_growth_ratio = ratio(positive_ancient_growth_bytes, positive_hot_growth_bytes)
interval_snapshot_to_hot_growth_ratio = ratio(positive_snapshot_growth_bytes, positive_hot_growth_bytes)
interval_derived_index_to_hot_growth_ratio = ratio(positive_derived_index_growth_bytes, positive_hot_growth_bytes)
ancient_bodies_bytes_delta = ancient_tables["bodies"]["bytes"] - number(previous, "ancientBodiesBytes", ancient_tables["bodies"]["bytes"]) if interval_seconds > 0 else 0
ancient_tx_infos_bytes_delta = ancient_tables["txInfos"]["bytes"] - number(previous, "ancientTxInfosBytes", ancient_tables["txInfos"]["bytes"]) if interval_seconds > 0 else 0
ancient_state_roots_bytes_delta = ancient_tables["stateRoots"]["bytes"] - number(previous, "ancientStateRootsBytes", ancient_tables["stateRoots"]["bytes"]) if interval_seconds > 0 else 0
ancient_other_bytes_delta = ancient_tables["other"]["bytes"] - number(previous, "ancientOtherBytes", ancient_tables["other"]["bytes"]) if interval_seconds > 0 else 0
snapshot_root_bytes_delta = snapshot_buckets["root"]["bytes"] - number(previous, "snapshotRootBytes", snapshot_buckets["root"]["bytes"]) if interval_seconds > 0 else 0
snapshot_latest_bytes_delta = snapshot_buckets["latest"]["bytes"] - number(previous, "snapshotLatestBytes", snapshot_buckets["latest"]["bytes"]) if interval_seconds > 0 else 0
snapshot_history_bytes_delta = snapshot_buckets["history"]["bytes"] - number(previous, "snapshotHistoryBytes", snapshot_buckets["history"]["bytes"]) if interval_seconds > 0 else 0
snapshot_chain_bytes_delta = snapshot_buckets["chain"]["bytes"] - number(previous, "snapshotChainBytes", snapshot_buckets["chain"]["bytes"]) if interval_seconds > 0 else 0
snapshot_log_bytes_delta = snapshot_buckets["log"]["bytes"] - number(previous, "snapshotLogBytes", snapshot_buckets["log"]["bytes"]) if interval_seconds > 0 else 0
snapshot_trace_bytes_delta = snapshot_buckets["trace"]["bytes"] - number(previous, "snapshotTraceBytes", snapshot_buckets["trace"]["bytes"]) if interval_seconds > 0 else 0
snapshot_commitment_bytes_delta = snapshot_buckets["commitment"]["bytes"] - number(previous, "snapshotCommitmentBytes", snapshot_buckets["commitment"]["bytes"]) if interval_seconds > 0 else 0
snapshot_retired_bytes_delta = snapshot_buckets["retired"]["bytes"] - number(previous, "snapshotRetiredDirectoryBytes", snapshot_buckets["retired"]["bytes"]) if interval_seconds > 0 else 0
snapshot_other_bytes_delta = snapshot_buckets["other"]["bytes"] - number(previous, "snapshotOtherBytes", snapshot_buckets["other"]["bytes"]) if interval_seconds > 0 else 0
chaindata_sst_bytes_delta = chaindata_files["sst"]["bytes"] - number(previous, "chaindataSSTBytes", chaindata_files["sst"]["bytes"]) if interval_seconds > 0 else 0
chaindata_wal_bytes_delta = chaindata_files["wal"]["bytes"] - number(previous, "chaindataWALBytes", chaindata_files["wal"]["bytes"]) if interval_seconds > 0 else 0
chaindata_log_bytes_delta = chaindata_files["log"]["bytes"] - number(previous, "chaindataLogBytes", chaindata_files["log"]["bytes"]) if interval_seconds > 0 else 0
chaindata_manifest_bytes_delta = chaindata_files["manifest"]["bytes"] - number(previous, "chaindataManifestBytes", chaindata_files["manifest"]["bytes"]) if interval_seconds > 0 else 0
chaindata_options_bytes_delta = chaindata_files["options"]["bytes"] - number(previous, "chaindataOptionsBytes", chaindata_files["options"]["bytes"]) if interval_seconds > 0 else 0
chaindata_other_bytes_delta = chaindata_files["other"]["bytes"] - number(previous, "chaindataOtherBytes", chaindata_files["other"]["bytes"]) if interval_seconds > 0 else 0
datadir_other_bytes_delta = datadir_bytes_delta - chaindata_bytes_delta - ancient_bytes_delta - snapshot_bytes_delta - replay_bytes_delta if interval_seconds > 0 else 0
top_level_growth_candidates = [
    ("chaindata", chaindata_bytes_delta),
    ("ancient", ancient_bytes_delta),
    ("snapshot", snapshot_bytes_delta),
    ("replay", replay_bytes_delta),
    ("other", datadir_other_bytes_delta),
]
interval_positive_disk_growth_bytes = int(sum(nonnegative(value) for _, value in top_level_growth_candidates))
interval_disk_growth_primary, interval_disk_growth_primary_bytes, interval_disk_growth_primary_share = disk_growth_primary(
    top_level_growth_candidates,
    interval_positive_disk_growth_bytes,
)
detailed_growth_candidates = [
    ("chaindata.sst", chaindata_sst_bytes_delta),
    ("chaindata.wal", chaindata_wal_bytes_delta),
    ("chaindata.log", chaindata_log_bytes_delta),
    ("chaindata.manifest", chaindata_manifest_bytes_delta),
    ("chaindata.options", chaindata_options_bytes_delta),
    ("chaindata.other", chaindata_other_bytes_delta),
    ("ancient.bodies", ancient_bodies_bytes_delta),
    ("ancient.txInfos", ancient_tx_infos_bytes_delta),
    ("ancient.stateRoots", ancient_state_roots_bytes_delta),
    ("ancient.other", ancient_other_bytes_delta),
    ("snapshot.root", snapshot_root_bytes_delta),
    ("snapshot.latest", snapshot_latest_bytes_delta),
    ("snapshot.history", snapshot_history_bytes_delta),
    ("snapshot.chain", snapshot_chain_bytes_delta),
    ("snapshot.log", snapshot_log_bytes_delta),
    ("snapshot.trace", snapshot_trace_bytes_delta),
    ("snapshot.commitment", snapshot_commitment_bytes_delta),
    ("snapshot.retired", snapshot_retired_bytes_delta),
    ("snapshot.other", snapshot_other_bytes_delta),
    ("replay", replay_bytes_delta),
    ("datadir.other", datadir_other_bytes_delta),
]
interval_detailed_positive_disk_growth_bytes = int(sum(nonnegative(value) for _, value in detailed_growth_candidates))
interval_disk_growth_primary_detailed, interval_disk_growth_primary_detailed_bytes, interval_disk_growth_primary_detailed_share = disk_growth_primary(
    detailed_growth_candidates,
    interval_detailed_positive_disk_growth_bytes,
)
current_detailed_disk_candidates = [
    ("chaindata.sst", chaindata_files["sst"]["bytes"]),
    ("chaindata.wal", chaindata_files["wal"]["bytes"]),
    ("chaindata.log", chaindata_files["log"]["bytes"]),
    ("chaindata.manifest", chaindata_files["manifest"]["bytes"]),
    ("chaindata.options", chaindata_files["options"]["bytes"]),
    ("chaindata.other", chaindata_files["other"]["bytes"]),
    ("ancient.bodies", ancient_tables["bodies"]["bytes"]),
    ("ancient.txInfos", ancient_tables["txInfos"]["bytes"]),
    ("ancient.stateRoots", ancient_tables["stateRoots"]["bytes"]),
    ("ancient.other", ancient_tables["other"]["bytes"]),
    ("snapshot.root", snapshot_buckets["root"]["bytes"]),
    ("snapshot.latest", snapshot_buckets["latest"]["bytes"]),
    ("snapshot.history", snapshot_buckets["history"]["bytes"]),
    ("snapshot.chain", snapshot_buckets["chain"]["bytes"]),
    ("snapshot.log", snapshot_buckets["log"]["bytes"]),
    ("snapshot.trace", snapshot_buckets["trace"]["bytes"]),
    ("snapshot.commitment", snapshot_buckets["commitment"]["bytes"]),
    ("snapshot.retired", snapshot_buckets["retired"]["bytes"]),
    ("snapshot.other", snapshot_buckets["other"]["bytes"]),
    ("replay", replay),
    ("datadir.other", datadir_other),
]
current_detailed_positive_disk_bytes = int(sum(nonnegative(value) for _, value in current_detailed_disk_candidates))
current_disk_primary_detailed, current_disk_primary_detailed_bytes, current_disk_primary_detailed_share = disk_growth_primary(
    current_detailed_disk_candidates,
    current_detailed_positive_disk_bytes,
)
datadir_bytes_per_second = float(datadir_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
chaindata_bytes_per_second = float(chaindata_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
chaindata_sst_bytes_per_second = float(chaindata_sst_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
chaindata_wal_bytes_per_second = float(chaindata_wal_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
cold_archive_bytes_per_second = float(cold_archive_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
derived_index_bytes_per_second = float(derived_index_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
replay_bytes_per_second = float(replay_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
datadir_other_bytes_per_second = float(datadir_other_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
datadir_bytes_per_interval_block = float(datadir_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
chaindata_bytes_per_interval_block = float(chaindata_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
chaindata_sst_bytes_per_interval_block = float(chaindata_sst_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
chaindata_wal_bytes_per_interval_block = float(chaindata_wal_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
ancient_bytes_per_interval_block = float(ancient_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
snapshot_bytes_per_interval_block = float(snapshot_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
cold_archive_bytes_per_interval_block = float(cold_archive_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
derived_index_bytes_per_interval_block = float(derived_index_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
replay_bytes_per_interval_block = float(replay_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
datadir_other_bytes_per_interval_block = float(datadir_other_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
stage_sync_finish_head_lag = lag(height, stages.get("stageSyncFinish", -1))
stage_sync_bodies_head_lag = lag(height, stages.get("stageSyncBodies", -1))
stage_sync_bodies_ready_head_lag = lag(height, stages.get("stageSyncBodiesReady", -1))
stage_sync_import_head_lag = lag(height, stages.get("stageSyncImport", -1))
stage_sync_execution_head_lag = lag(height, stages.get("stageSyncExecution", -1))
stage_sync_commitment_head_lag = lag(height, stages.get("stageSyncCommitment", -1))
stage_chain_freezer_head_lag = lag(height, stages.get("stageChainFreezer", -1))
stage_snapshot_event_log_build_head_lag = lag(height, stages.get("stageSnapshotEventLogBuild", -1))
stage_sync_bottleneck, stage_sync_bottleneck_lag = stage_bottleneck([
    ("bodies-ready-gap", stages.get("stageSyncBodiesReadyGapBlocks", -1)),
    ("import-execution", stages.get("stageSyncImportExecutionLagBlocks", -1)),
    ("execution-commitment", stages.get("stageSyncExecutionCommitmentLagBlocks", -1)),
    ("commitment-finish", stages.get("stageSyncCommitmentFinishLagBlocks", -1)),
    ("finish-head", stage_sync_finish_head_lag),
])
stage_sync_pipeline_lag = lag_sum([
    stages.get("stageSyncBodiesReadyGapBlocks", -1),
    stages.get("stageSyncImportExecutionLagBlocks", -1),
    stages.get("stageSyncExecutionCommitmentLagBlocks", -1),
    stages.get("stageSyncCommitmentFinishLagBlocks", -1),
    stage_sync_finish_head_lag,
])
stage_sync_bottleneck_lag_share = ratio(stage_sync_bottleneck_lag, stage_sync_pipeline_lag)
stage_sync_pipeline_violations = stage_pipeline_violations(stages)
stage_sync_pipeline_violation = ""
stage_sync_pipeline_max_violation_blocks = 0
if stage_sync_pipeline_violations:
    stage_sync_pipeline_violation = stage_sync_pipeline_violations[0]["name"]
    stage_sync_pipeline_max_violation_blocks = max(
        violation["violationBlocks"] for violation in stage_sync_pipeline_violations
    )
full_staged_sync = full_staged_sync_summary(
    stages,
    stage_sync_pipeline_violations,
    stage_sync_bottleneck,
    stage_sync_bottleneck_lag,
    stage_sync_pipeline_lag,
    stage_sync_finish_head_lag,
    height,
)
interval_stage_sync_bodies = interval_stage_delta(stages.get("stageSyncBodies", -1), previous, "stageSyncBodies", interval_seconds)
interval_stage_sync_bodies_ready = interval_stage_delta(stages.get("stageSyncBodiesReady", -1), previous, "stageSyncBodiesReady", interval_seconds)
interval_stage_sync_import = interval_stage_delta(stages.get("stageSyncImport", -1), previous, "stageSyncImport", interval_seconds)
interval_stage_sync_execution = interval_stage_delta(stages.get("stageSyncExecution", -1), previous, "stageSyncExecution", interval_seconds)
interval_stage_sync_commitment = interval_stage_delta(stages.get("stageSyncCommitment", -1), previous, "stageSyncCommitment", interval_seconds)
interval_stage_sync_finish = interval_stage_delta(stages.get("stageSyncFinish", -1), previous, "stageSyncFinish", interval_seconds)
interval_stage_chain_freezer = interval_stage_delta(stages.get("stageChainFreezer", -1), previous, "stageChainFreezer", interval_seconds)
interval_stage_snapshot_event_log_build = interval_stage_delta(stages.get("stageSnapshotEventLogBuild", -1), previous, "stageSnapshotEventLogBuild", interval_seconds)
interval_stage_sync_bodies_rate = interval_rate(interval_stage_sync_bodies, interval_seconds)
interval_stage_sync_bodies_ready_rate = interval_rate(interval_stage_sync_bodies_ready, interval_seconds)
interval_stage_sync_import_rate = interval_rate(interval_stage_sync_import, interval_seconds)
interval_stage_sync_execution_rate = interval_rate(interval_stage_sync_execution, interval_seconds)
interval_stage_sync_commitment_rate = interval_rate(interval_stage_sync_commitment, interval_seconds)
interval_stage_sync_finish_rate = interval_rate(interval_stage_sync_finish, interval_seconds)
interval_stage_chain_freezer_rate = interval_rate(interval_stage_chain_freezer, interval_seconds)
interval_stage_snapshot_event_log_build_rate = interval_rate(interval_stage_snapshot_event_log_build, interval_seconds)
interval_stage_sync_bodies_per_minute = interval_stage_sync_bodies_rate * 60.0
interval_stage_sync_bodies_ready_per_minute = interval_stage_sync_bodies_ready_rate * 60.0
interval_stage_sync_import_per_minute = interval_stage_sync_import_rate * 60.0
interval_stage_sync_execution_per_minute = interval_stage_sync_execution_rate * 60.0
interval_stage_sync_commitment_per_minute = interval_stage_sync_commitment_rate * 60.0
interval_stage_sync_finish_per_minute = interval_stage_sync_finish_rate * 60.0
interval_stage_chain_freezer_per_minute = interval_stage_chain_freezer_rate * 60.0
interval_stage_snapshot_event_log_build_per_minute = interval_stage_snapshot_event_log_build_rate * 60.0
stage_sync_bodies_head_eta_seconds = eta_seconds(stage_sync_bodies_head_lag, interval_stage_sync_bodies_rate)
stage_sync_bodies_ready_head_eta_seconds = eta_seconds(stage_sync_bodies_ready_head_lag, interval_stage_sync_bodies_ready_rate)
stage_sync_import_head_eta_seconds = eta_seconds(stage_sync_import_head_lag, interval_stage_sync_import_rate)
stage_sync_execution_head_eta_seconds = eta_seconds(stage_sync_execution_head_lag, interval_stage_sync_execution_rate)
stage_sync_commitment_head_eta_seconds = eta_seconds(stage_sync_commitment_head_lag, interval_stage_sync_commitment_rate)
stage_sync_finish_head_eta_seconds = eta_seconds(stage_sync_finish_head_lag, interval_stage_sync_finish_rate)
stage_chain_freezer_head_eta_seconds = eta_seconds(stage_chain_freezer_head_lag, interval_stage_chain_freezer_rate)
stage_snapshot_event_log_build_head_eta_seconds = eta_seconds(stage_snapshot_event_log_build_head_lag, interval_stage_snapshot_event_log_build_rate)
stage_interval_rates = [
    interval_stage_rate_summary("SyncBodies", "stageSyncBodies", interval_stage_sync_bodies, interval_stage_sync_bodies_rate, stage_sync_bodies_head_lag, stage_sync_bodies_head_eta_seconds),
    interval_stage_rate_summary("SyncBodiesReady", "stageSyncBodiesReady", interval_stage_sync_bodies_ready, interval_stage_sync_bodies_ready_rate, stage_sync_bodies_ready_head_lag, stage_sync_bodies_ready_head_eta_seconds),
    interval_stage_rate_summary("SyncImport", "stageSyncImport", interval_stage_sync_import, interval_stage_sync_import_rate, stage_sync_import_head_lag, stage_sync_import_head_eta_seconds),
    interval_stage_rate_summary("SyncExecution", "stageSyncExecution", interval_stage_sync_execution, interval_stage_sync_execution_rate, stage_sync_execution_head_lag, stage_sync_execution_head_eta_seconds),
    interval_stage_rate_summary("SyncCommitment", "stageSyncCommitment", interval_stage_sync_commitment, interval_stage_sync_commitment_rate, stage_sync_commitment_head_lag, stage_sync_commitment_head_eta_seconds),
    interval_stage_rate_summary("SyncFinish", "stageSyncFinish", interval_stage_sync_finish, interval_stage_sync_finish_rate, stage_sync_finish_head_lag, stage_sync_finish_head_eta_seconds),
    interval_stage_rate_summary("ChainFreezer", "stageChainFreezer", interval_stage_chain_freezer, interval_stage_chain_freezer_rate, stage_chain_freezer_head_lag, stage_chain_freezer_head_eta_seconds),
    interval_stage_rate_summary("SnapshotEventLogBuild", "stageSnapshotEventLogBuild", interval_stage_snapshot_event_log_build, interval_stage_snapshot_event_log_build_rate, stage_snapshot_event_log_build_head_lag, stage_snapshot_event_log_build_head_eta_seconds),
]
height_regression_blocks = previous_height - height if interval_seconds > 0 and previous_height > 0 and height > 0 and height < previous_height else 0
stage_progress_regressions = progress_regressions(stages, previous, [
    "stageSyncBodies",
    "stageSyncBodiesReady",
    "stageSyncImport",
    "stageSyncExecution",
    "stageSyncCommitment",
    "stageSyncFinish",
    "stageCanonicalFinish",
    "stageChainFreezer",
    "stageSnapshotEventLogBuild",
], stages.get("stageStatusFileStatus") == "ok")
stage_progress_max_regression_blocks = 0
if stage_progress_regressions:
    stage_progress_max_regression_blocks = max(
        regression["regressionBlocks"] for regression in stage_progress_regressions
    )
max_stage_interval_blocks = max(
    0,
    interval_stage_sync_bodies,
    interval_stage_sync_bodies_ready,
    interval_stage_sync_import,
    interval_stage_sync_execution,
    interval_stage_sync_commitment,
    interval_stage_sync_finish,
)
stage_last_progress_unix, stage_stalls = stage_stagnation(
    stages,
    previous,
    now,
    previous_unix,
    interval_seconds,
    height,
    sync_target_height,
)
stage_primary_stall = primary_stage_stall(stage_stalls)
stage_stalled = bool(stage_primary_stall)
stage_stalled_stage = stage_primary_stall.get("stage", "")
stage_stalled_seconds = int(stage_primary_stall.get("stalledSeconds", 0)) if stage_primary_stall else 0
stage_stalled_lag_blocks = int(stage_primary_stall.get("lagBlocks", -1)) if stage_primary_stall else -1
if height_regression_blocks > 0:
    restart_recovery_status = "height-regression"
elif stage_progress_regressions:
    restart_recovery_status = "stage-regression"
elif stage_sync_pipeline_violations:
    restart_recovery_status = "pipeline-violation"
elif interval_seconds <= 0:
    restart_recovery_status = "no-previous"
elif sync_target_lag_blocks > 0 and interval_blocks == 0 and max_stage_interval_blocks <= 0:
    restart_recovery_status = "stalled"
elif interval_blocks > 0 or max_stage_interval_blocks > 0:
    restart_recovery_status = "progressing"
else:
    restart_recovery_status = "steady"
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
soak_health = build_soak_health(
    sample_status,
    height_regression_blocks,
    stage_progress_regressions,
    stage_sync_pipeline_violations,
    restart_recovery_status,
    stages,
    alerts,
    offline_status,
    offline_enabled,
    stage_sync_bottleneck,
    stage_sync_bottleneck_lag,
    sync_log,
    stage_stalled,
)
if interval_seconds > 0:
    soak_efficiency_window = "interval"
    soak_efficiency_blocks_per_second = interval_blocks_per_second
    soak_efficiency_eta_seconds = interval_sync_eta_seconds
    soak_efficiency_datadir_bytes_per_block = datadir_bytes_per_interval_block
    soak_efficiency_hot_bytes_per_block = chaindata_bytes_per_interval_block
    soak_efficiency_cold_archive_bytes_per_block = cold_archive_bytes_per_interval_block
    soak_efficiency_derived_index_bytes_per_block = derived_index_bytes_per_interval_block
    soak_efficiency_disk_primary = interval_disk_growth_primary_detailed
    soak_efficiency_disk_primary_bytes = interval_disk_growth_primary_detailed_bytes
    soak_efficiency_disk_primary_share = interval_disk_growth_primary_detailed_share
else:
    soak_efficiency_window = "cumulative"
    soak_efficiency_blocks_per_second = blocks_per_second
    soak_efficiency_eta_seconds = sync_eta_seconds
    soak_efficiency_datadir_bytes_per_block = bytes_per_block
    soak_efficiency_hot_bytes_per_block = chaindata_bytes_per_block
    soak_efficiency_cold_archive_bytes_per_block = cold_archive_bytes_per_block
    soak_efficiency_derived_index_bytes_per_block = derived_index_bytes_per_block
    soak_efficiency_disk_primary = current_disk_primary_detailed
    soak_efficiency_disk_primary_bytes = current_disk_primary_detailed_bytes
    soak_efficiency_disk_primary_share = current_disk_primary_detailed_share

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
    "syncTargetHeight": sync_target_height,
    "syncTargetLagBlocks": sync_target_lag_blocks,
    "blockId": block_id,
    "peers": peers,
    "sampleStatus": sample_status,
    "elapsedSeconds": elapsed,
    "blocksPerSecond": blocks_per_second,
    "blocksPerMinute": blocks_per_minute,
    "syncEtaSeconds": sync_eta_seconds,
    "intervalSyncEtaSeconds": interval_sync_eta_seconds,
    "intervalSeconds": interval_seconds,
    "intervalBlocks": interval_blocks,
    "intervalBlocksPerSecond": interval_blocks_per_second,
    "datadirBytes": total,
    "chaindataBytes": chaindata,
    "chaindataFiles": sum(bucket["files"] for bucket in chaindata_files.values()),
    "chaindataSSTBytes": chaindata_files["sst"]["bytes"],
    "chaindataSSTFiles": chaindata_files["sst"]["files"],
    "chaindataWALBytes": chaindata_files["wal"]["bytes"],
    "chaindataWALFiles": chaindata_files["wal"]["files"],
    "chaindataLogBytes": chaindata_files["log"]["bytes"],
    "chaindataLogFiles": chaindata_files["log"]["files"],
    "chaindataManifestBytes": chaindata_files["manifest"]["bytes"],
    "chaindataManifestFiles": chaindata_files["manifest"]["files"],
    "chaindataOptionsBytes": chaindata_files["options"]["bytes"],
    "chaindataOptionsFiles": chaindata_files["options"]["files"],
    "chaindataOtherBytes": chaindata_files["other"]["bytes"],
    "chaindataOtherFiles": chaindata_files["other"]["files"],
    "ancientBytes": ancient,
    "ancientBodiesBytes": ancient_tables["bodies"]["bytes"],
    "ancientBodiesFiles": ancient_tables["bodies"]["files"],
    "ancientTxInfosBytes": ancient_tables["txInfos"]["bytes"],
    "ancientTxInfosFiles": ancient_tables["txInfos"]["files"],
    "ancientStateRootsBytes": ancient_tables["stateRoots"]["bytes"],
    "ancientStateRootsFiles": ancient_tables["stateRoots"]["files"],
    "ancientOtherBytes": ancient_tables["other"]["bytes"],
    "ancientOtherFiles": ancient_tables["other"]["files"],
    "snapshotBytes": snapshot,
    "snapshotRootBytes": snapshot_buckets["root"]["bytes"],
    "snapshotRootFiles": snapshot_buckets["root"]["files"],
    "snapshotLatestBytes": snapshot_buckets["latest"]["bytes"],
    "snapshotLatestFiles": snapshot_buckets["latest"]["files"],
    "snapshotHistoryBytes": snapshot_buckets["history"]["bytes"],
    "snapshotHistoryFiles": snapshot_buckets["history"]["files"],
    "snapshotChainBytes": snapshot_buckets["chain"]["bytes"],
    "snapshotChainFiles": snapshot_buckets["chain"]["files"],
    "snapshotLogBytes": snapshot_buckets["log"]["bytes"],
    "snapshotLogFiles": snapshot_buckets["log"]["files"],
    "snapshotTraceBytes": snapshot_buckets["trace"]["bytes"],
    "snapshotTraceFiles": snapshot_buckets["trace"]["files"],
    "snapshotCommitmentBytes": snapshot_buckets["commitment"]["bytes"],
    "snapshotCommitmentFiles": snapshot_buckets["commitment"]["files"],
    "snapshotRetiredDirectoryBytes": snapshot_buckets["retired"]["bytes"],
    "snapshotRetiredDirectoryFiles": snapshot_buckets["retired"]["files"],
    "snapshotOtherBytes": snapshot_buckets["other"]["bytes"],
    "snapshotOtherFiles": snapshot_buckets["other"]["files"],
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
    "coldArchiveDatadirShare": cold_archive_datadir_share,
    "derivedIndexColdArchiveRatio": derived_index_cold_archive_ratio,
    "chaindataSSTToHotBytesRatio": chaindata_sst_to_hot_ratio,
    "chaindataWALToHotBytesRatio": chaindata_wal_to_hot_ratio,
    "chaindataWALToSSTBytesRatio": chaindata_wal_to_sst_ratio,
    "datadirBytesDelta": datadir_bytes_delta,
    "chaindataBytesDelta": chaindata_bytes_delta,
    "ancientBytesDelta": ancient_bytes_delta,
    "snapshotBytesDelta": snapshot_bytes_delta,
    "replayBytesDelta": replay_bytes_delta,
    "coldArchiveBytesDelta": cold_archive_bytes_delta,
    "derivedIndexBytesDelta": derived_index_bytes_delta,
    "ancientBodiesBytesDelta": ancient_bodies_bytes_delta,
    "ancientTxInfosBytesDelta": ancient_tx_infos_bytes_delta,
    "ancientStateRootsBytesDelta": ancient_state_roots_bytes_delta,
    "ancientOtherBytesDelta": ancient_other_bytes_delta,
    "snapshotRootBytesDelta": snapshot_root_bytes_delta,
    "snapshotLatestBytesDelta": snapshot_latest_bytes_delta,
    "snapshotHistoryBytesDelta": snapshot_history_bytes_delta,
    "snapshotChainBytesDelta": snapshot_chain_bytes_delta,
    "snapshotLogBytesDelta": snapshot_log_bytes_delta,
    "snapshotTraceBytesDelta": snapshot_trace_bytes_delta,
    "snapshotCommitmentBytesDelta": snapshot_commitment_bytes_delta,
    "snapshotRetiredDirectoryBytesDelta": snapshot_retired_bytes_delta,
    "snapshotOtherBytesDelta": snapshot_other_bytes_delta,
    "chaindataSSTBytesDelta": chaindata_sst_bytes_delta,
    "chaindataWALBytesDelta": chaindata_wal_bytes_delta,
    "chaindataLogBytesDelta": chaindata_log_bytes_delta,
    "chaindataManifestBytesDelta": chaindata_manifest_bytes_delta,
    "chaindataOptionsBytesDelta": chaindata_options_bytes_delta,
    "chaindataOtherBytesDelta": chaindata_other_bytes_delta,
    "datadirOtherBytesDelta": datadir_other_bytes_delta,
    "intervalPositiveDiskGrowthBytes": interval_positive_disk_growth_bytes,
    "intervalDiskGrowthPrimary": interval_disk_growth_primary,
    "intervalDiskGrowthPrimaryBytes": interval_disk_growth_primary_bytes,
    "intervalDiskGrowthPrimaryShare": interval_disk_growth_primary_share,
    "intervalDetailedPositiveDiskGrowthBytes": interval_detailed_positive_disk_growth_bytes,
    "intervalDiskGrowthPrimaryDetailed": interval_disk_growth_primary_detailed,
    "intervalDiskGrowthPrimaryDetailedBytes": interval_disk_growth_primary_detailed_bytes,
    "intervalDiskGrowthPrimaryDetailedShare": interval_disk_growth_primary_detailed_share,
    "intervalChaindataGrowthShare": growth_share(chaindata_bytes_delta, interval_positive_disk_growth_bytes),
    "intervalAncientGrowthShare": growth_share(ancient_bytes_delta, interval_positive_disk_growth_bytes),
    "intervalSnapshotGrowthShare": growth_share(snapshot_bytes_delta, interval_positive_disk_growth_bytes),
    "intervalReplayGrowthShare": growth_share(replay_bytes_delta, interval_positive_disk_growth_bytes),
    "intervalDatadirOtherGrowthShare": growth_share(datadir_other_bytes_delta, interval_positive_disk_growth_bytes),
    "intervalColdArchiveGrowthShare": growth_share(cold_archive_bytes_delta, interval_positive_disk_growth_bytes),
    "intervalDerivedIndexGrowthShare": growth_share(derived_index_bytes_delta, interval_positive_disk_growth_bytes),
    "intervalColdToHotGrowthRatio": interval_cold_to_hot_growth_ratio,
    "intervalAncientToHotGrowthRatio": interval_ancient_to_hot_growth_ratio,
    "intervalSnapshotToHotGrowthRatio": interval_snapshot_to_hot_growth_ratio,
    "intervalDerivedIndexToHotGrowthRatio": interval_derived_index_to_hot_growth_ratio,
    "intervalChaindataSSTGrowthShare": growth_share(chaindata_sst_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalChaindataWALGrowthShare": growth_share(chaindata_wal_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalChaindataLogGrowthShare": growth_share(chaindata_log_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalChaindataManifestGrowthShare": growth_share(chaindata_manifest_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalChaindataOptionsGrowthShare": growth_share(chaindata_options_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalChaindataOtherGrowthShare": growth_share(chaindata_other_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalAncientBodiesGrowthShare": growth_share(ancient_bodies_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalAncientTxInfosGrowthShare": growth_share(ancient_tx_infos_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalAncientStateRootsGrowthShare": growth_share(ancient_state_roots_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalAncientOtherGrowthShare": growth_share(ancient_other_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotRootGrowthShare": growth_share(snapshot_root_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotLatestGrowthShare": growth_share(snapshot_latest_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotHistoryGrowthShare": growth_share(snapshot_history_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotChainGrowthShare": growth_share(snapshot_chain_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotLogGrowthShare": growth_share(snapshot_log_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotTraceGrowthShare": growth_share(snapshot_trace_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotCommitmentGrowthShare": growth_share(snapshot_commitment_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotRetiredDirectoryGrowthShare": growth_share(snapshot_retired_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalSnapshotOtherGrowthShare": growth_share(snapshot_other_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalReplayDetailedGrowthShare": growth_share(replay_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "intervalDatadirOtherDetailedGrowthShare": growth_share(datadir_other_bytes_delta, interval_detailed_positive_disk_growth_bytes),
    "datadirBytesPerSecond": datadir_bytes_per_second,
    "chaindataBytesPerSecond": chaindata_bytes_per_second,
    "chaindataSSTBytesPerSecond": chaindata_sst_bytes_per_second,
    "chaindataWALBytesPerSecond": chaindata_wal_bytes_per_second,
    "coldArchiveBytesPerSecond": cold_archive_bytes_per_second,
    "derivedIndexBytesPerSecond": derived_index_bytes_per_second,
    "replayBytesPerSecond": replay_bytes_per_second,
    "datadirOtherBytesPerSecond": datadir_other_bytes_per_second,
    "intervalDatadirBytesPerBlock": datadir_bytes_per_interval_block,
    "intervalChaindataBytesPerBlock": chaindata_bytes_per_interval_block,
    "intervalChaindataSSTBytesPerBlock": chaindata_sst_bytes_per_interval_block,
    "intervalChaindataWALBytesPerBlock": chaindata_wal_bytes_per_interval_block,
    "intervalAncientBytesPerBlock": ancient_bytes_per_interval_block,
    "intervalSnapshotBytesPerBlock": snapshot_bytes_per_interval_block,
    "intervalColdArchiveBytesPerBlock": cold_archive_bytes_per_interval_block,
    "intervalDerivedIndexBytesPerBlock": derived_index_bytes_per_interval_block,
    "intervalReplayBytesPerBlock": replay_bytes_per_interval_block,
    "intervalDatadirOtherBytesPerBlock": datadir_other_bytes_per_interval_block,
    "stageSyncBodiesHeadLagBlocks": stage_sync_bodies_head_lag,
    "stageSyncBodiesHeadEtaSeconds": stage_sync_bodies_head_eta_seconds,
    "stageSyncBodiesReadyHeadLagBlocks": stage_sync_bodies_ready_head_lag,
    "stageSyncBodiesReadyHeadEtaSeconds": stage_sync_bodies_ready_head_eta_seconds,
    "stageSyncImportHeadLagBlocks": stage_sync_import_head_lag,
    "stageSyncImportHeadEtaSeconds": stage_sync_import_head_eta_seconds,
    "stageSyncExecutionHeadLagBlocks": stage_sync_execution_head_lag,
    "stageSyncExecutionHeadEtaSeconds": stage_sync_execution_head_eta_seconds,
    "stageSyncCommitmentHeadLagBlocks": stage_sync_commitment_head_lag,
    "stageSyncCommitmentHeadEtaSeconds": stage_sync_commitment_head_eta_seconds,
    "stageSyncFinishHeadLagBlocks": stage_sync_finish_head_lag,
    "stageSyncFinishHeadEtaSeconds": stage_sync_finish_head_eta_seconds,
    "stageChainFreezerHeadLagBlocks": stage_chain_freezer_head_lag,
    "stageChainFreezerHeadEtaSeconds": stage_chain_freezer_head_eta_seconds,
    "stageSnapshotEventLogBuildHeadLagBlocks": stage_snapshot_event_log_build_head_lag,
    "stageSnapshotEventLogBuildHeadEtaSeconds": stage_snapshot_event_log_build_head_eta_seconds,
    "stageSyncBottleneck": stage_sync_bottleneck,
    "stageSyncBottleneckLagBlocks": stage_sync_bottleneck_lag,
    "stageSyncPipelineLagBlocks": stage_sync_pipeline_lag,
    "stageSyncBottleneckLagShare": stage_sync_bottleneck_lag_share,
    "stageSyncPipelineMonotonic": len(stage_sync_pipeline_violations) == 0,
    "stageSyncPipelineViolation": stage_sync_pipeline_violation,
    "stageSyncPipelineViolationCount": len(stage_sync_pipeline_violations),
    "stageSyncPipelineMaxViolationBlocks": stage_sync_pipeline_max_violation_blocks,
    "stageSyncPipelineViolations": stage_sync_pipeline_violations,
    "restartRecoveryStatus": restart_recovery_status,
    "heightRegressionBlocks": height_regression_blocks,
    "stageProgressRegressionCount": len(stage_progress_regressions),
    "stageProgressMaxRegressionBlocks": stage_progress_max_regression_blocks,
    "stageProgressRegressions": stage_progress_regressions,
    "stageLastProgressUnix": stage_last_progress_unix,
    "stageStalled": stage_stalled,
    "stageStalledCount": len(stage_stalls),
    "stageStalledStage": stage_stalled_stage,
    "stageStalledSeconds": stage_stalled_seconds,
    "stageStalledLagBlocks": stage_stalled_lag_blocks,
    "stageStalls": stage_stalls,
    "intervalStageSyncBodiesBlocks": interval_stage_sync_bodies,
    "intervalStageSyncBodiesBlocksPerSecond": interval_stage_sync_bodies_rate,
    "intervalStageSyncBodiesBlocksPerMinute": interval_stage_sync_bodies_per_minute,
    "intervalStageSyncBodiesReadyBlocks": interval_stage_sync_bodies_ready,
    "intervalStageSyncBodiesReadyBlocksPerSecond": interval_stage_sync_bodies_ready_rate,
    "intervalStageSyncBodiesReadyBlocksPerMinute": interval_stage_sync_bodies_ready_per_minute,
    "intervalStageSyncBodiesReadyToBodiesRatio": ratio(interval_stage_sync_bodies_ready, interval_stage_sync_bodies),
    "intervalStageSyncImportBlocks": interval_stage_sync_import,
    "intervalStageSyncImportBlocksPerSecond": interval_stage_sync_import_rate,
    "intervalStageSyncImportBlocksPerMinute": interval_stage_sync_import_per_minute,
    "intervalStageSyncImportToBodiesReadyRatio": ratio(interval_stage_sync_import, interval_stage_sync_bodies_ready),
    "intervalStageSyncExecutionBlocks": interval_stage_sync_execution,
    "intervalStageSyncExecutionBlocksPerSecond": interval_stage_sync_execution_rate,
    "intervalStageSyncExecutionBlocksPerMinute": interval_stage_sync_execution_per_minute,
    "intervalStageSyncExecutionToImportRatio": ratio(interval_stage_sync_execution, interval_stage_sync_import),
    "intervalStageSyncCommitmentBlocks": interval_stage_sync_commitment,
    "intervalStageSyncCommitmentBlocksPerSecond": interval_stage_sync_commitment_rate,
    "intervalStageSyncCommitmentBlocksPerMinute": interval_stage_sync_commitment_per_minute,
    "intervalStageSyncCommitmentToExecutionRatio": ratio(interval_stage_sync_commitment, interval_stage_sync_execution),
    "intervalStageSyncFinishBlocks": interval_stage_sync_finish,
    "intervalStageSyncFinishBlocksPerSecond": interval_stage_sync_finish_rate,
    "intervalStageSyncFinishBlocksPerMinute": interval_stage_sync_finish_per_minute,
    "intervalStageSyncFinishToCommitmentRatio": ratio(interval_stage_sync_finish, interval_stage_sync_commitment),
    "intervalStageChainFreezerBlocks": interval_stage_chain_freezer,
    "intervalStageChainFreezerBlocksPerSecond": interval_stage_chain_freezer_rate,
    "intervalStageChainFreezerBlocksPerMinute": interval_stage_chain_freezer_per_minute,
    "intervalStageSnapshotEventLogBuildBlocks": interval_stage_snapshot_event_log_build,
    "intervalStageSnapshotEventLogBuildBlocksPerSecond": interval_stage_snapshot_event_log_build_rate,
    "intervalStageSnapshotEventLogBuildBlocksPerMinute": interval_stage_snapshot_event_log_build_per_minute,
    "stageIntervalRates": stage_interval_rates,
    "soakEfficiencyWindow": soak_efficiency_window,
    "soakEfficiencyStatus": restart_recovery_status,
    "soakEfficiencyBlocksPerSecond": soak_efficiency_blocks_per_second,
    "soakEfficiencyEtaSeconds": soak_efficiency_eta_seconds,
    "soakEfficiencyDatadirBytesPerBlock": soak_efficiency_datadir_bytes_per_block,
    "soakEfficiencyHotBytesPerBlock": soak_efficiency_hot_bytes_per_block,
    "soakEfficiencyColdArchiveBytesPerBlock": soak_efficiency_cold_archive_bytes_per_block,
    "soakEfficiencyDerivedIndexBytesPerBlock": soak_efficiency_derived_index_bytes_per_block,
    "soakEfficiencyDiskPrimary": soak_efficiency_disk_primary,
    "soakEfficiencyDiskPrimaryBytes": soak_efficiency_disk_primary_bytes,
    "soakEfficiencyDiskPrimaryShare": soak_efficiency_disk_primary_share,
    "soakEfficiencyStageBottleneck": stage_sync_bottleneck,
    "soakEfficiencyStageBottleneckLagBlocks": stage_sync_bottleneck_lag,
    "soakEfficiencyStageBottleneckLagShare": stage_sync_bottleneck_lag_share,
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
row.update(soak_health)
row.update(full_staged_sync)
row.update(alerts)
row.update(stages)
row.update(sync_log)
row.update(process)
row.update(debug_metrics)
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
