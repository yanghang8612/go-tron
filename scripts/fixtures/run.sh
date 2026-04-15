#!/usr/bin/env bash
#
# Fixture extraction entrypoint.
# Usage:
#   ./scripts/fixtures/run.sh <scenario>      # run a single scenario
#   ./scripts/fixtures/run.sh all             # run every scenario in order
#   ./scripts/fixtures/run.sh list            # list scenarios
#
# Each scenario lives at scripts/fixtures/scenarios/<name>/ and provides:
#   - config.conf : java-tron config (self-contained; ports + genesis)
#   - setup.sh    : pre-chain-activity hook (can be empty)
#   - run.sh      : chain-activity hook (broadcasts, waits)
#   - dump.sh     : produces test/fixtures/<name>/fixture.json

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
SCENARIOS_DIR="$SCRIPT_DIR/scenarios"
FIXTURES_OUT_DIR="$REPO_ROOT/test/fixtures"

# shellcheck source=lib/java-tron-ctl.sh
source "$SCRIPT_DIR/lib/java-tron-ctl.sh"
# shellcheck source=lib/api.sh
source "$SCRIPT_DIR/lib/api.sh"
# shellcheck source=lib/dump.sh
source "$SCRIPT_DIR/lib/dump.sh"

list_scenarios() {
    local name
    for dir in "$SCENARIOS_DIR"/*/; do
        [[ -d "$dir" ]] || continue
        name=$(basename "$dir")
        echo "$name"
    done
}

usage() {
    cat >&2 <<'EOF'
Usage:
  run.sh <scenario>
  run.sh all
  run.sh list
EOF
    return 1
}

# _extract_port_from_config <config_path>
# Pulls the HTTP port from HOCON. Kept naïve intentionally: we control the
# config files, and each one declares http.fullNodePort on a dedicated line.
_extract_port_from_config() {
    local config="$1"
    awk '
        /^[[:space:]]*fullNodePort[[:space:]]*=/ {
            gsub(/[^0-9]/, "", $0); if ($0 != "") { print $0; exit }
        }
    ' "$config"
}

run_scenario() {
    local name="$1"
    local scenario_dir="$SCENARIOS_DIR/$name"
    local config="$scenario_dir/config.conf"

    if [[ ! -d "$scenario_dir" ]]; then
        echo "run: unknown scenario '$name'" >&2
        return 1
    fi
    if [[ ! -f "$config" ]]; then
        echo "run: $scenario_dir/config.conf missing" >&2
        return 1
    fi

    local port
    port=$(_extract_port_from_config "$config")
    if [[ -z "$port" ]]; then
        echo "run: could not extract http.fullNodePort from $config" >&2
        return 1
    fi

    local workdir="/tmp/fixture-tron-$$-$name"
    local out_dir="$FIXTURES_OUT_DIR/$name"
    local out_path="$out_dir/fixture.json"

    echo "--- scenario $name (port=$port workdir=$workdir) ---"

    jt_init "$workdir" "$config"
    jt_start "$workdir" "$config"

    # Ensure clean stop/cleanup even on error.
    local err=0
    {
        jt_wait_ready "$port" 90
        if [[ -x "$scenario_dir/setup.sh" ]]; then
            "$scenario_dir/setup.sh" "$port"
        fi
        if [[ -x "$scenario_dir/run.sh" ]]; then
            "$scenario_dir/run.sh" "$port"
        fi
        "$scenario_dir/dump.sh" "$out_path" "$config" "$port"
    } || err=$?

    jt_stop
    jt_cleanup "$workdir"

    if (( err != 0 )); then
        echo "--- scenario $name FAILED (code $err) ---" >&2
        return "$err"
    fi
    echo "--- scenario $name OK → $out_path ---"
    return 0
}

main() {
    local arg="${1:-}"
    case "$arg" in
        "") usage ;;
        list) list_scenarios ;;
        all)
            local fails=0
            while IFS= read -r name; do
                if ! run_scenario "$name"; then
                    fails=$(( fails + 1 ))
                fi
            done < <(list_scenarios)
            if (( fails > 0 )); then
                echo "$fails scenario(s) failed" >&2
                return "$fails"
            fi
            ;;
        *) run_scenario "$arg" ;;
    esac
}

# Safety: any unexpected exit must tear java-tron down.
_on_exit() {
    local ec=$?
    jt_stop || true
    exit "$ec"
}
trap _on_exit EXIT INT TERM

main "$@"
