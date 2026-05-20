#!/usr/bin/env bash
set -Eeuo pipefail

APP_ROOT="${APP_ROOT:-/data/gtron}"
REPO_DIR="${REPO_DIR:-$APP_ROOT/go-tron}"
SERVICE="${SERVICE:-gtron-nile}"
REMOTE="${REMOTE:-origin}"
BRANCH="${BRANCH:-master}"
LOG_FILE="${LOG_FILE:-$APP_ROOT/start.log}"
LOCK_FILE="${LOCK_FILE:-$APP_ROOT/start.lock}"
RUN_TESTS="${RUN_TESTS:-0}"

mkdir -p "$APP_ROOT"
exec > >(tee -a "$LOG_FILE") 2>&1
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "another start.sh is already running"
  exit 1
fi

log() {
  printf '[%s] %s\n' "$(date -u '+%Y-%m-%dT%H:%M:%SZ')" "$*"
}

need_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1"
    exit 1
  fi
}

need_cmd git
need_cmd go
need_cmd make
need_cmd cargo
need_cmd cc
need_cmd sudo
need_cmd systemctl

if [[ ! -d "$REPO_DIR/.git" ]]; then
  echo "repo not found: $REPO_DIR"
  exit 1
fi

cd "$REPO_DIR"

log "updating source: $REMOTE/$BRANCH"
current_branch="$(git branch --show-current)"
if [[ "$current_branch" != "$BRANCH" ]]; then
  git checkout "$BRANCH"
fi
git fetch --tags --recurse-submodules "$REMOTE" "$BRANCH"
git pull --ff-only --recurse-submodules "$REMOTE" "$BRANCH"
git submodule sync --recursive
git submodule update --init --recursive

log "building librustzcash dependency"
export CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"
make zksnark-deps

if [[ "$RUN_TESTS" == "1" ]]; then
  log "running sapling tests"
  make test-sapling
fi

deploy_build_dir="$REPO_DIR/build/deploy-$(date -u '+%Y%m%d%H%M%S')"
rm -rf "$deploy_build_dir"
mkdir -p "$deploy_build_dir" "$REPO_DIR/build/bin"

log "building sapling-enabled gtron"
make GOBIN="$deploy_build_dir" gtron-sapling

log "installing binary"
install -m 0755 "$deploy_build_dir/gtron" "$REPO_DIR/build/bin/gtron.new"
mv -f "$REPO_DIR/build/bin/gtron.new" "$REPO_DIR/build/bin/gtron"
rm -rf "$deploy_build_dir"

log "restarting $SERVICE"
sudo systemctl daemon-reload
sudo systemctl restart "$SERVICE"
sleep 2
sudo systemctl --no-pager --full status "$SERVICE"

log "done"
