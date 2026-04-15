# shellcheck shell=bash
#
# java-tron control library. Callers:
#   source lib/java-tron-ctl.sh
#   trap jt_stop EXIT
#   jt_init    "$workdir" "$config_path"
#   jt_start   "$workdir" "$config_path"
#   jt_wait_ready "$http_port"
#   ...work...
#   jt_stop
#   jt_cleanup "$workdir"
#
# Env:
#   FULLNODE_JAR  — path to FullNode.jar (default:
#                   /Users/asuka/Projects/tron/java-tron/build/libs/FullNode.jar)
#   JAVA          — java binary (default: java)

: "${FULLNODE_JAR:=/Users/asuka/Projects/tron/java-tron/build/libs/FullNode.jar}"
: "${JAVA:=java}"

JT_PID=""
JT_WORKDIR=""

# jt_init <workdir> <config_path>
# Clean workdir, ensure config exists.
jt_init() {
    local workdir="$1"
    local config="$2"
    if [[ -z "$workdir" || -z "$config" ]]; then
        echo "jt_init: usage: jt_init <workdir> <config_path>" >&2
        return 1
    fi
    if [[ ! -f "$config" ]]; then
        echo "jt_init: config file not found: $config" >&2
        return 1
    fi
    if [[ ! -f "$FULLNODE_JAR" ]]; then
        echo "jt_init: FullNode.jar not found at $FULLNODE_JAR (set FULLNODE_JAR to override)" >&2
        return 1
    fi
    rm -rf "$workdir"
    mkdir -p "$workdir"
    JT_WORKDIR="$workdir"
    return 0
}

# jt_start <workdir> <config_path>
# Launch java-tron in the background; PID stashed in JT_PID.
jt_start() {
    local workdir="$1"
    local config="$2"
    if [[ -z "$workdir" || -z "$config" ]]; then
        echo "jt_start: usage: jt_start <workdir> <config_path>" >&2
        return 1
    fi
    if [[ -n "$JT_PID" ]] && kill -0 "$JT_PID" 2>/dev/null; then
        echo "jt_start: java-tron already running (pid=$JT_PID)" >&2
        return 1
    fi
    (
        cd "$workdir" || exit 1
        "$JAVA" -jar "$FULLNODE_JAR" -c "$config" > "$workdir/java-tron.log" 2>&1 &
        echo $! > "$workdir/java-tron.pid"
    )
    JT_PID=$(cat "$workdir/java-tron.pid")
    JT_WORKDIR="$workdir"
    return 0
}

# jt_wait_ready <http_port> [timeout_seconds]
# Poll wallet/getnowblock until 200 or timeout (default 60s).
jt_wait_ready() {
    local port="$1"
    local timeout="${2:-60}"
    if [[ -z "$port" ]]; then
        echo "jt_wait_ready: usage: jt_wait_ready <http_port> [timeout_seconds]" >&2
        return 1
    fi
    local deadline=$(( $(date +%s) + timeout ))
    while (( $(date +%s) < deadline )); do
        if curl -sf -o /dev/null -m 2 "http://127.0.0.1:${port}/wallet/getnowblock"; then
            return 0
        fi
        # Bail out early if the java process died.
        if [[ -n "$JT_PID" ]] && ! kill -0 "$JT_PID" 2>/dev/null; then
            echo "jt_wait_ready: java-tron process $JT_PID exited; last 20 log lines:" >&2
            [[ -n "$JT_WORKDIR" && -f "$JT_WORKDIR/java-tron.log" ]] && tail -n 20 "$JT_WORKDIR/java-tron.log" >&2
            return 1
        fi
        sleep 1
    done
    echo "jt_wait_ready: timed out after ${timeout}s on port $port" >&2
    [[ -n "$JT_WORKDIR" && -f "$JT_WORKDIR/java-tron.log" ]] && tail -n 20 "$JT_WORKDIR/java-tron.log" >&2
    return 1
}

# jt_stop
# Idempotent: safe to call when nothing is running.
jt_stop() {
    if [[ -z "$JT_PID" ]]; then
        return 0
    fi
    if kill -0 "$JT_PID" 2>/dev/null; then
        kill "$JT_PID" 2>/dev/null || true
        # Give it up to 15s to shut down cleanly before SIGKILL.
        for _ in $(seq 1 15); do
            if ! kill -0 "$JT_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        if kill -0 "$JT_PID" 2>/dev/null; then
            kill -9 "$JT_PID" 2>/dev/null || true
        fi
    fi
    JT_PID=""
    return 0
}

# jt_cleanup <workdir>
# Remove workdir after stopping. Does NOT touch /tmp/tron-* from unrelated runs.
jt_cleanup() {
    local workdir="$1"
    if [[ -z "$workdir" ]]; then
        echo "jt_cleanup: usage: jt_cleanup <workdir>" >&2
        return 1
    fi
    # Be paranoid: workdir must match the /tmp/fixture-tron-* convention to
    # avoid accidentally removing something the caller passed by mistake.
    case "$workdir" in
        /tmp/fixture-tron-*) ;;
        *)
            echo "jt_cleanup: refusing to remove $workdir (must start with /tmp/fixture-tron-)" >&2
            return 1
            ;;
    esac
    rm -rf "$workdir"
    [[ "$JT_WORKDIR" == "$workdir" ]] && JT_WORKDIR=""
    return 0
}
