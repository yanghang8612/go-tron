#!/usr/bin/env bash
set -Eeuo pipefail

APP_ROOT="${APP_ROOT:-/data/gtron}"
REPO_DIR="${REPO_DIR:-$APP_ROOT/go-tron}"
SERVICE_NAME="${SERVICE_NAME:-gtron.service}"
BRANCH="${BRANCH:-master}"
LOCK_FILE="${LOCK_FILE:-$APP_ROOT/start.lock}"
LOG_FILE="${LOG_FILE:-$APP_ROOT/start.log}"
STATE_FILE="${STATE_FILE:-$APP_ROOT/deployed-$BRANCH.rev}"
RELEASE_DIR="${RELEASE_DIR:-$APP_ROOT/releases}"
HEALTH_URL="${HEALTH_URL:-http://127.0.0.1:8090/wallet/getnowblock}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-180}"
RUN_TESTS="${RUN_TESTS:-1}"
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-1}"

log() {
  printf '[%s] %s\n' "$(date '+%Y-%m-%d %H:%M:%S')" "$*"
}

die() {
  log "ERROR: $*"
  exit 1
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 ||
    die "missing required command: $1"
}

service_active() {
  sudo systemctl is-active --quiet "$SERVICE_NAME"
}

wallet_healthy() {
  curl --fail --silent --show-error --max-time 5 "$HEALTH_URL" \
    >/dev/null 2>&1
}

wait_healthy() {
  local deadline=$((SECONDS + HEALTH_TIMEOUT))

  while ((SECONDS < deadline)); do
    if service_active && wallet_healthy; then
      return 0
    fi
    sleep 2
  done

  return 1
}

show_failure_context() {
  sudo systemctl --no-pager --full status "$SERVICE_NAME" || true
  sudo journalctl -u "$SERVICE_NAME" -n 100 --no-pager || true
}

for cmd in git go make cargo cc curl sudo systemctl flock install mktemp; do
  need_cmd "$cmd"
done

case "$HEALTH_TIMEOUT" in
  ''|*[!0-9]*) die "HEALTH_TIMEOUT must be a positive integer" ;;
esac
((HEALTH_TIMEOUT > 0)) || die "HEALTH_TIMEOUT must be greater than zero"

mkdir -p "$APP_ROOT" "$RELEASE_DIR"
exec > >(tee -a "$LOG_FILE") 2>&1

exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  log "another deploy is already running; skipping"
  exit 0
fi

[[ -d "$REPO_DIR/.git" ]] ||
  die "repo not found: $REPO_DIR"

cd "$REPO_DIR"

if [[ -n "$(git status --porcelain --untracked-files=no --ignore-submodules=dirty)" ]]; then
  log "tracked local changes found; refusing to deploy"
  git status --short
  exit 1
fi

log "checking origin/$BRANCH"
git fetch --prune --tags --recurse-submodules origin "$BRANCH"

remote_rev="$(git rev-parse "origin/$BRANCH")"
deployed_rev=""
if [[ -f "$STATE_FILE" ]]; then
  deployed_rev="$(tr -d '[:space:]' <"$STATE_FILE")"
fi

log "remote revision:   $remote_rev"
log "deployed revision: ${deployed_rev:-unknown}"

if [[ "$remote_rev" == "$deployed_rev" ]]; then
  if service_active && wallet_healthy; then
    log "$SERVICE_NAME is already up to date and healthy"
    exit 0
  fi

  log "$SERVICE_NAME is up to date but unhealthy; restarting"
  sudo systemctl restart "$SERVICE_NAME"
  if wait_healthy; then
    log "$SERVICE_NAME recovered and is healthy"
    exit 0
  fi

  show_failure_context
  die "$SERVICE_NAME did not become healthy after restart"
fi

current_branch="$(git symbolic-ref --short HEAD)"
if [[ "$current_branch" != "$BRANCH" ]]; then
  log "switching branch: $current_branch -> $BRANCH"
  git checkout "$BRANCH"
fi

log "fast-forwarding source to $remote_rev"
git merge --ff-only "$remote_rev"
git submodule sync --recursive
git submodule update --init --recursive

export CARGO_BUILD_JOBS

log "building librustzcash with CARGO_BUILD_JOBS=$CARGO_BUILD_JOBS"
make zksnark-deps

if [[ "$RUN_TESTS" == "1" ]]; then
  log "running Sapling-enabled tests"
  make test-sapling
fi

mkdir -p "$REPO_DIR/build/bin"
deploy_build_dir="$(mktemp -d "$REPO_DIR/build/deploy.XXXXXX")"
binary_path="$REPO_DIR/build/bin/gtron"
new_release="$RELEASE_DIR/gtron.$remote_rev"
previous_release=""

cleanup() {
  if [[ -n "${deploy_build_dir:-}" && -d "$deploy_build_dir" ]]; then
    rm -rf -- "$deploy_build_dir"
  fi
  rm -f -- "$binary_path.new" "$binary_path.rollback"
}
trap cleanup EXIT

log "building Sapling-enabled gtron"
make GOBIN="$deploy_build_dir" gtron-sapling

[[ -x "$deploy_build_dir/gtron" ]] ||
  die "binary missing: $deploy_build_dir/gtron"

# Preserve the exact currently installed binary. The source tree may already
# point at origin/master after an earlier failed deployment, so git HEAD is not
# a reliable rollback identifier.
if [[ -x "$binary_path" ]]; then
  if [[ -n "$deployed_rev" ]]; then
    previous_release="$RELEASE_DIR/gtron.$deployed_rev"
  else
    previous_release="$RELEASE_DIR/gtron.bootstrap.$(date '+%Y%m%d%H%M%S')"
  fi

  install -m 0755 "$binary_path" "$previous_release"
fi

log "saving release: $new_release"
install -m 0755 "$deploy_build_dir/gtron" "$new_release"

log "installing binary"
install -m 0755 "$new_release" "$binary_path.new"
mv -f "$binary_path.new" "$binary_path"

deployment_failed=0
log "restarting $SERVICE_NAME"
if ! sudo systemctl restart "$SERVICE_NAME"; then
  deployment_failed=1
elif ! wait_healthy; then
  deployment_failed=1
fi

if ((deployment_failed != 0)); then
  log "deployment health check failed"
  show_failure_context

  if [[ -n "$previous_release" && -x "$previous_release" ]]; then
    log "rolling back binary to: $previous_release"
    install -m 0755 "$previous_release" "$binary_path.rollback"
    mv -f "$binary_path.rollback" "$binary_path"

    if sudo systemctl restart "$SERVICE_NAME" && wait_healthy; then
      log "rollback succeeded; previous deployment remains active"
    else
      log "ERROR: rollback did not restore a healthy service"
      show_failure_context
    fi
  else
    log "ERROR: no previous binary is available for rollback"
  fi

  exit 1
fi

state_tmp="$STATE_FILE.tmp.$$"
printf '%s\n' "$remote_rev" >"$state_tmp"
mv -f "$state_tmp" "$STATE_FILE"

log "successfully deployed $remote_rev"
log "$SERVICE_NAME is active and Wallet API is healthy"
sudo systemctl --no-pager --full status "$SERVICE_NAME" | sed -n '1,16p'

# Keep the five newest release binaries. The current and immediately previous
# releases are among the newest after a successful deployment.
ls -1t "$RELEASE_DIR"/gtron.* 2>/dev/null |
  tail -n +6 |
  xargs -r rm -f --
