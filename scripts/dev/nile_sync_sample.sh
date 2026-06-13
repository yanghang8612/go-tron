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
  "$STAGE_STATUS_FILE" "$SYNC_LOG_FILE" "$PID_FILE" \
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
        "syncLogStageNext": "",
        "syncLogStageNextBlock": -1,
        "syncLogStageNextCanonical": "",
        "syncLogStageNextSync": "",
        "syncLogStageBlockedStatus": "",
        "syncLogExecPlanBlocks": -1,
        "syncLogExecPlanStages": -1,
        "syncLogExecPlanPostBodyStages": -1,
        "syncLogExecPlanFirst": -1,
        "syncLogExecPlanLast": -1,
    }
    if not path:
        return row
    try:
        lines = Path(path).read_text(encoding="utf-8", errors="replace").splitlines()
    except Exception:
        return row
    latest = None
    count = 0
    for line in lines:
        fields = imported_segment_fields_from_line(line)
        if fields is None:
            continue
        count += 1
        latest = fields
    row["syncLogImportedSegments"] = count
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
        "syncExecPlanBlocks": "syncLogExecPlanBlocks",
        "syncExecPlanStages": "syncLogExecPlanStages",
        "syncExecPlanPostBodyStages": "syncLogExecPlanPostBodyStages",
        "syncExecPlanFirst": "syncLogExecPlanFirst",
        "syncExecPlanLast": "syncLogExecPlanLast",
    }
    for source, dest in mappings.items():
        if source in latest:
            row[dest] = parse_log_value(latest[source])
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

def ratio(numerator, denominator):
    try:
        numerator = float(numerator)
        denominator = float(denominator)
    except Exception:
        return -1.0
    if numerator < 0 or denominator <= 0:
        return -1.0
    return numerator / denominator

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
bytes_per_block = float(total) / height if height > 0 else 0.0
chaindata_bytes_per_block = float(chaindata) / height if height > 0 else 0.0
cold_archive_bytes_per_block = float(cold_archive) / height if height > 0 else 0.0
derived_index_bytes_per_block = float(derived_index) / height if height > 0 else 0.0
cold_to_hot_ratio = float(cold_archive) / chaindata if chaindata > 0 else 0.0
derived_index_to_hot_ratio = float(derived_index) / chaindata if chaindata > 0 else 0.0
derived_index_snapshot_ratio = float(derived_index) / snapshot if snapshot > 0 else 0.0
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
cold_archive_bytes_delta = cold_archive - number(previous, "coldArchiveBytes", cold_archive) if interval_seconds > 0 else 0
derived_index_bytes_delta = derived_index - number(previous, "derivedIndexBytes", derived_index) if interval_seconds > 0 else 0
ancient_bodies_bytes_delta = ancient_tables["bodies"]["bytes"] - number(previous, "ancientBodiesBytes", ancient_tables["bodies"]["bytes"]) if interval_seconds > 0 else 0
ancient_tx_infos_bytes_delta = ancient_tables["txInfos"]["bytes"] - number(previous, "ancientTxInfosBytes", ancient_tables["txInfos"]["bytes"]) if interval_seconds > 0 else 0
ancient_state_roots_bytes_delta = ancient_tables["stateRoots"]["bytes"] - number(previous, "ancientStateRootsBytes", ancient_tables["stateRoots"]["bytes"]) if interval_seconds > 0 else 0
snapshot_latest_bytes_delta = snapshot_buckets["latest"]["bytes"] - number(previous, "snapshotLatestBytes", snapshot_buckets["latest"]["bytes"]) if interval_seconds > 0 else 0
snapshot_history_bytes_delta = snapshot_buckets["history"]["bytes"] - number(previous, "snapshotHistoryBytes", snapshot_buckets["history"]["bytes"]) if interval_seconds > 0 else 0
snapshot_chain_bytes_delta = snapshot_buckets["chain"]["bytes"] - number(previous, "snapshotChainBytes", snapshot_buckets["chain"]["bytes"]) if interval_seconds > 0 else 0
snapshot_log_bytes_delta = snapshot_buckets["log"]["bytes"] - number(previous, "snapshotLogBytes", snapshot_buckets["log"]["bytes"]) if interval_seconds > 0 else 0
snapshot_trace_bytes_delta = snapshot_buckets["trace"]["bytes"] - number(previous, "snapshotTraceBytes", snapshot_buckets["trace"]["bytes"]) if interval_seconds > 0 else 0
chaindata_sst_bytes_delta = chaindata_files["sst"]["bytes"] - number(previous, "chaindataSSTBytes", chaindata_files["sst"]["bytes"]) if interval_seconds > 0 else 0
chaindata_wal_bytes_delta = chaindata_files["wal"]["bytes"] - number(previous, "chaindataWALBytes", chaindata_files["wal"]["bytes"]) if interval_seconds > 0 else 0
chaindata_log_bytes_delta = chaindata_files["log"]["bytes"] - number(previous, "chaindataLogBytes", chaindata_files["log"]["bytes"]) if interval_seconds > 0 else 0
chaindata_manifest_bytes_delta = chaindata_files["manifest"]["bytes"] - number(previous, "chaindataManifestBytes", chaindata_files["manifest"]["bytes"]) if interval_seconds > 0 else 0
chaindata_options_bytes_delta = chaindata_files["options"]["bytes"] - number(previous, "chaindataOptionsBytes", chaindata_files["options"]["bytes"]) if interval_seconds > 0 else 0
chaindata_other_bytes_delta = chaindata_files["other"]["bytes"] - number(previous, "chaindataOtherBytes", chaindata_files["other"]["bytes"]) if interval_seconds > 0 else 0
datadir_bytes_per_second = float(datadir_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
chaindata_bytes_per_second = float(chaindata_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
chaindata_sst_bytes_per_second = float(chaindata_sst_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
chaindata_wal_bytes_per_second = float(chaindata_wal_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
cold_archive_bytes_per_second = float(cold_archive_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
derived_index_bytes_per_second = float(derived_index_bytes_delta) / interval_seconds if interval_seconds > 0 else 0.0
datadir_bytes_per_interval_block = float(datadir_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
chaindata_bytes_per_interval_block = float(chaindata_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
chaindata_sst_bytes_per_interval_block = float(chaindata_sst_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
chaindata_wal_bytes_per_interval_block = float(chaindata_wal_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
ancient_bytes_per_interval_block = float(ancient_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
snapshot_bytes_per_interval_block = float(snapshot_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
cold_archive_bytes_per_interval_block = float(cold_archive_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
derived_index_bytes_per_interval_block = float(derived_index_bytes_delta) / interval_blocks if interval_blocks > 0 else 0.0
stage_sync_finish_head_lag = lag(height, stages.get("stageSyncFinish", -1))
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
interval_stage_sync_bodies = interval_stage_delta(stages.get("stageSyncBodies", -1), previous, "stageSyncBodies", interval_seconds)
interval_stage_sync_bodies_ready = interval_stage_delta(stages.get("stageSyncBodiesReady", -1), previous, "stageSyncBodiesReady", interval_seconds)
interval_stage_sync_import = interval_stage_delta(stages.get("stageSyncImport", -1), previous, "stageSyncImport", interval_seconds)
interval_stage_sync_execution = interval_stage_delta(stages.get("stageSyncExecution", -1), previous, "stageSyncExecution", interval_seconds)
interval_stage_sync_commitment = interval_stage_delta(stages.get("stageSyncCommitment", -1), previous, "stageSyncCommitment", interval_seconds)
interval_stage_sync_finish = interval_stage_delta(stages.get("stageSyncFinish", -1), previous, "stageSyncFinish", interval_seconds)
interval_stage_chain_freezer = interval_stage_delta(stages.get("stageChainFreezer", -1), previous, "stageChainFreezer", interval_seconds)
interval_stage_snapshot_event_log_build = interval_stage_delta(stages.get("stageSnapshotEventLogBuild", -1), previous, "stageSnapshotEventLogBuild", interval_seconds)
interval_stage_sync_finish_rate = interval_rate(interval_stage_sync_finish, interval_seconds)
interval_stage_chain_freezer_rate = interval_rate(interval_stage_chain_freezer, interval_seconds)
interval_stage_snapshot_event_log_build_rate = interval_rate(interval_stage_snapshot_event_log_build, interval_seconds)
stage_sync_finish_head_eta_seconds = eta_seconds(stage_sync_finish_head_lag, interval_stage_sync_finish_rate)
stage_chain_freezer_head_eta_seconds = eta_seconds(stage_chain_freezer_head_lag, interval_stage_chain_freezer_rate)
stage_snapshot_event_log_build_head_eta_seconds = eta_seconds(stage_snapshot_event_log_build_head_lag, interval_stage_snapshot_event_log_build_rate)
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
    "chaindataSSTToHotBytesRatio": chaindata_sst_to_hot_ratio,
    "chaindataWALToHotBytesRatio": chaindata_wal_to_hot_ratio,
    "chaindataWALToSSTBytesRatio": chaindata_wal_to_sst_ratio,
    "datadirBytesDelta": datadir_bytes_delta,
    "chaindataBytesDelta": chaindata_bytes_delta,
    "ancientBytesDelta": ancient_bytes_delta,
    "snapshotBytesDelta": snapshot_bytes_delta,
    "coldArchiveBytesDelta": cold_archive_bytes_delta,
    "derivedIndexBytesDelta": derived_index_bytes_delta,
    "ancientBodiesBytesDelta": ancient_bodies_bytes_delta,
    "ancientTxInfosBytesDelta": ancient_tx_infos_bytes_delta,
    "ancientStateRootsBytesDelta": ancient_state_roots_bytes_delta,
    "snapshotLatestBytesDelta": snapshot_latest_bytes_delta,
    "snapshotHistoryBytesDelta": snapshot_history_bytes_delta,
    "snapshotChainBytesDelta": snapshot_chain_bytes_delta,
    "snapshotLogBytesDelta": snapshot_log_bytes_delta,
    "snapshotTraceBytesDelta": snapshot_trace_bytes_delta,
    "chaindataSSTBytesDelta": chaindata_sst_bytes_delta,
    "chaindataWALBytesDelta": chaindata_wal_bytes_delta,
    "chaindataLogBytesDelta": chaindata_log_bytes_delta,
    "chaindataManifestBytesDelta": chaindata_manifest_bytes_delta,
    "chaindataOptionsBytesDelta": chaindata_options_bytes_delta,
    "chaindataOtherBytesDelta": chaindata_other_bytes_delta,
    "datadirBytesPerSecond": datadir_bytes_per_second,
    "chaindataBytesPerSecond": chaindata_bytes_per_second,
    "chaindataSSTBytesPerSecond": chaindata_sst_bytes_per_second,
    "chaindataWALBytesPerSecond": chaindata_wal_bytes_per_second,
    "coldArchiveBytesPerSecond": cold_archive_bytes_per_second,
    "derivedIndexBytesPerSecond": derived_index_bytes_per_second,
    "intervalDatadirBytesPerBlock": datadir_bytes_per_interval_block,
    "intervalChaindataBytesPerBlock": chaindata_bytes_per_interval_block,
    "intervalChaindataSSTBytesPerBlock": chaindata_sst_bytes_per_interval_block,
    "intervalChaindataWALBytesPerBlock": chaindata_wal_bytes_per_interval_block,
    "intervalAncientBytesPerBlock": ancient_bytes_per_interval_block,
    "intervalSnapshotBytesPerBlock": snapshot_bytes_per_interval_block,
    "intervalColdArchiveBytesPerBlock": cold_archive_bytes_per_interval_block,
    "intervalDerivedIndexBytesPerBlock": derived_index_bytes_per_interval_block,
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
    "intervalStageSyncBodiesBlocks": interval_stage_sync_bodies,
    "intervalStageSyncBodiesBlocksPerSecond": interval_rate(interval_stage_sync_bodies, interval_seconds),
    "intervalStageSyncBodiesReadyBlocks": interval_stage_sync_bodies_ready,
    "intervalStageSyncBodiesReadyBlocksPerSecond": interval_rate(interval_stage_sync_bodies_ready, interval_seconds),
    "intervalStageSyncBodiesReadyToBodiesRatio": ratio(interval_stage_sync_bodies_ready, interval_stage_sync_bodies),
    "intervalStageSyncImportBlocks": interval_stage_sync_import,
    "intervalStageSyncImportBlocksPerSecond": interval_rate(interval_stage_sync_import, interval_seconds),
    "intervalStageSyncImportToBodiesReadyRatio": ratio(interval_stage_sync_import, interval_stage_sync_bodies_ready),
    "intervalStageSyncExecutionBlocks": interval_stage_sync_execution,
    "intervalStageSyncExecutionBlocksPerSecond": interval_rate(interval_stage_sync_execution, interval_seconds),
    "intervalStageSyncExecutionToImportRatio": ratio(interval_stage_sync_execution, interval_stage_sync_import),
    "intervalStageSyncCommitmentBlocks": interval_stage_sync_commitment,
    "intervalStageSyncCommitmentBlocksPerSecond": interval_rate(interval_stage_sync_commitment, interval_seconds),
    "intervalStageSyncCommitmentToExecutionRatio": ratio(interval_stage_sync_commitment, interval_stage_sync_execution),
    "intervalStageSyncFinishBlocks": interval_stage_sync_finish,
    "intervalStageSyncFinishBlocksPerSecond": interval_stage_sync_finish_rate,
    "intervalStageSyncFinishToCommitmentRatio": ratio(interval_stage_sync_finish, interval_stage_sync_commitment),
    "intervalStageChainFreezerBlocks": interval_stage_chain_freezer,
    "intervalStageChainFreezerBlocksPerSecond": interval_stage_chain_freezer_rate,
    "intervalStageSnapshotEventLogBuildBlocks": interval_stage_snapshot_event_log_build,
    "intervalStageSnapshotEventLogBuildBlocksPerSecond": interval_stage_snapshot_event_log_build_rate,
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
row.update(sync_log)
row.update(process)
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
