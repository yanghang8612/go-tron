#!/usr/bin/env bash
# CI wrapper: run system_test_flows.sh, enforce PASS >= 30 and WARN <= 4.
set -uo pipefail

BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
PASS_MIN=30
WARN_MAX=4

output="$("$BASEDIR/scripts/system_test_flows.sh" --start 2>&1)"
echo "$output"

# Extract counts from "Results: N pass | M warn | ..." line
pass=$(echo "$output" | grep -E 'Results:' | grep -oE '[0-9]+ pass' | grep -oE '[0-9]+' || echo "0")
warn=$(echo "$output" | grep -E 'Results:' | grep -oE '[0-9]+ warn' | grep -oE '[0-9]+' || echo "0")

echo ""
echo "CI check: PASS=$pass (min=$PASS_MIN), WARN=$warn (max=$WARN_MAX)"

rc=0
if [ "$pass" -lt "$PASS_MIN" ]; then
    echo "FAIL: PASS count $pass < $PASS_MIN"
    rc=1
fi
if [ "$warn" -gt "$WARN_MAX" ]; then
    echo "FAIL: WARN count $warn > $WARN_MAX"
    rc=1
fi

exit $rc
