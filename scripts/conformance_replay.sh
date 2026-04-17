#!/usr/bin/env bash
#
# Iterate over one or more mainnet-blocks ranges and run gtron-replay on
# each. Environment variables:
#
#   RANGES     space-separated range dir names under test/fixtures/mainnet-blocks/
#              default: "smoke"
#   EXIT_GATE  1 = fail if any range has allowlist hits or stale entries
#              default: 0
#   VERBOSE    1 = pass --verbose to gtron-replay (prints C-digest diffs)
#              default: 0
#
# Exit: 0 iff every range passes (respecting EXIT_GATE). Non-zero otherwise.
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
GTRON_REPLAY="$BASEDIR/build/bin/gtron-replay"
CORPUS_DIR="$BASEDIR/test/fixtures/mainnet-blocks"
RANGES="${RANGES:-smoke}"
EXIT_GATE="${EXIT_GATE:-0}"
VERBOSE="${VERBOSE:-0}"

if [ ! -x "$GTRON_REPLAY" ]; then
    echo "Building gtron-replay..."
    (cd "$BASEDIR" && go build -o build/bin/gtron-replay ./cmd/gtron-replay)
fi

summary_pass=0
summary_fail=0
summary_gate=0

echo "================================"
echo "  Conformance replay"
echo "================================"

for r in $RANGES; do
    dir="$CORPUS_DIR/$r"
    if [ ! -d "$dir" ]; then
        echo "=== $r === SKIP (missing directory: $dir)"
        continue
    fi
    if [ ! -f "$dir/blocks.bin" ] || [ ! -s "$dir/blocks.bin" ]; then
        echo "=== $r === SKIP (missing or empty blocks.bin — did you run 'git lfs pull'?)"
        continue
    fi
    echo ""
    echo "=== $r ==="

    args=(--range="$dir")
    [ "$EXIT_GATE" = "1" ] && args+=(--exit-gate)
    [ "$VERBOSE" = "1" ] && args+=(--verbose)

    if "$GTRON_REPLAY" "${args[@]}"; then
        summary_pass=$((summary_pass + 1))
    else
        rc=$?
        case "$rc" in
            1) echo "  (exit-gate failed: allowlist non-empty or stale)"
               summary_gate=$((summary_gate + 1)) ;;
            2) echo "  (hard divergence)"
               summary_fail=$((summary_fail + 1)) ;;
            *) echo "  (harness error: exit $rc)"
               summary_fail=$((summary_fail + 1)) ;;
        esac
    fi
done

echo ""
echo "================================"
echo "  Passed: $summary_pass  Failed: $summary_fail  Gate: $summary_gate"
echo "================================"

if [ "$summary_fail" -gt 0 ] || [ "$summary_gate" -gt 0 ]; then
    exit 1
fi
exit 0
