#!/usr/bin/env bash
set -euo pipefail

# Nile lite database comparison launcher.
#
# This file can be run from either location:
#   /data/gtron/go-tron/compare.sh
#   /data/gtron/compare.sh
# Results always go to /data/gtron/result by default.

SCRIPT_PATH="$(realpath "${BASH_SOURCE[0]}")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"

if [[ -d "$SCRIPT_DIR/.git" ]]; then
  REPO_DIR="$SCRIPT_DIR"
  BASE_DIR="$(dirname "$SCRIPT_DIR")"
elif [[ -d "$SCRIPT_DIR/go-tron/.git" ]]; then
  BASE_DIR="$SCRIPT_DIR"
  REPO_DIR="$SCRIPT_DIR/go-tron"
else
  echo "cannot locate go-tron repository beside $SCRIPT_PATH" >&2
  exit 2
fi

HEIGHT="${HEIGHT:-69296404}"
GTRON_DB="${GTRON_DB:-$BASE_DIR/nile/datadir}"
JAVA_DB="${JAVA_DB:-/data/nile/output-directory}"
RESULT_DIR="${RESULT_DIR:-$BASE_DIR/result}"
BRANCH="${BRANCH:-master}"
WORKERS="${WORKERS:-8}"
MAX_DIFFS="${MAX_DIFFS:-100000}"
MAX_DIFFS_PER_STORE="${MAX_DIFFS_PER_STORE:-500}"
LIVE_MAX_DIFFS="${LIVE_MAX_DIFFS:-1000}"

JSON_FILE="$RESULT_DIR/db-compare.json"
LOG_FILE="$RESULT_DIR/db-compare.log"
PID_FILE="$RESULT_DIR/db-compare.pid"
STATUS_FILE="$RESULT_DIR/status"
EXIT_FILE="$RESULT_DIR/exit-code"
BIN_FILE="$REPO_DIR/build/bin/db-compare"

mkdir -p "$RESULT_DIR"

read_pid() {
  if [[ -s "$PID_FILE" ]]; then
    tr -dc '0-9' < "$PID_FILE"
  fi
}

is_running() {
  local pid
  pid="$(read_pid)"
  [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

show_status() {
  local phase="not-started"
  local pid=""
  [[ -s "$STATUS_FILE" ]] && phase="$(<"$STATUS_FILE")"
  pid="$(read_pid)"

  echo "status: $phase"
  [[ -n "$pid" ]] && echo "pid: $pid"
  echo "json: $JSON_FILE"
  echo "log: $LOG_FILE"
  [[ -s "$EXIT_FILE" ]] && echo "exit-code: $(<"$EXIT_FILE")"
  if [[ -s "$LOG_FILE" ]]; then
    echo
    tail -n 8 "$LOG_FILE"
  fi
}

run_compare() {
  echo "$$" > "$PID_FILE"
  local finished=0
  cleanup() {
    local rc=$?
    rm -f "$PID_FILE"
    if [[ "$finished" -eq 0 ]]; then
      echo "$rc" > "$EXIT_FILE"
      echo "failed" > "$STATUS_FILE"
      echo "launcher failed with exit code $rc"
    fi
  }
  trap cleanup EXIT

  exec >> "$LOG_FILE" 2>&1
  echo "[$(date '+%F %T')] preparing comparison"
  echo "repo=$REPO_DIR branch=$BRANCH height=$HEIGHT"
  echo "gtron=$GTRON_DB"
  echo "java=$JAVA_DB"

  if [[ ! -d "$GTRON_DB" ]]; then
    echo "gtron database directory not found: $GTRON_DB"
    exit 2
  fi
  if [[ ! -d "$JAVA_DB" ]]; then
    echo "java-tron database directory not found: $JAVA_DB"
    exit 2
  fi

  local current_branch
  current_branch="$(cd "$REPO_DIR" && git symbolic-ref --quiet --short HEAD)"
  if [[ "$current_branch" != "$BRANCH" ]]; then
    echo "go-tron is on branch $current_branch; expected $BRANCH"
    exit 2
  fi

  echo "[$(date '+%F %T')] pulling origin/$BRANCH"
  (cd "$REPO_DIR" && git pull --ff-only origin "$BRANCH")

  echo "[$(date '+%F %T')] building db-compare"
  (cd "$REPO_DIR" && make db-compare)

  echo "[$(date '+%F %T')] starting database comparison"
  echo "running" > "$STATUS_FILE"
  set +e
  "$BIN_FILE" \
    --height "$HEIGHT" \
    --gtron "$GTRON_DB" \
    --java "$JAVA_DB" \
    --json \
    --workers "$WORKERS" \
    --max-diffs "$MAX_DIFFS" \
    --max-diffs-per-store "$MAX_DIFFS_PER_STORE" \
    --live-max-diffs "$LIVE_MAX_DIFFS" \
    > "$JSON_FILE"
  local rc=$?
  set -e

  echo "$rc" > "$EXIT_FILE"
  case "$rc" in
    0)
      echo "completed-no-diff" > "$STATUS_FILE"
      echo "[$(date '+%F %T')] comparison completed without differences"
      ;;
    1)
      echo "completed-with-diff" > "$STATUS_FILE"
      echo "[$(date '+%F %T')] comparison completed with differences"
      ;;
    *)
      echo "failed" > "$STATUS_FILE"
      echo "[$(date '+%F %T')] comparison failed with exit code $rc"
      ;;
  esac
  finished=1
}

start_compare() {
  if is_running; then
    echo "db-compare is already running (pid $(read_pid))"
    show_status
    exit 1
  fi

  : > "$JSON_FILE"
  : > "$LOG_FILE"
  rm -f "$EXIT_FILE"
  echo "preparing" > "$STATUS_FILE"

  nohup setsid "$SCRIPT_PATH" __run </dev/null >/dev/null 2>&1 &
  local pid=$!
  echo "$pid" > "$PID_FILE"

  echo "comparison launcher started (pid $pid)"
  echo "the SSH session may now be disconnected safely"
  echo "status: $SCRIPT_PATH status"
  echo "logs:   $SCRIPT_PATH logs"
  echo "json:   $JSON_FILE"
}

stop_compare() {
  if ! is_running; then
    echo "db-compare is not running"
    show_status
    return
  fi
  local pid
  pid="$(read_pid)"
  kill -- "-$pid"
  echo "stopped" > "$STATUS_FILE"
  echo "comparison stopped (process group $pid)"
}

case "${1:-start}" in
  start)
    start_compare
    ;;
  status)
    show_status
    ;;
  logs)
    touch "$LOG_FILE"
    tail -n 100 -F "$LOG_FILE"
    ;;
  stop)
    stop_compare
    ;;
  __run)
    run_compare
    ;;
  *)
    echo "usage: $0 {start|status|logs|stop}" >&2
    exit 2
    ;;
esac
