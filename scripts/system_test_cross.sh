#!/usr/bin/env bash
#
# Cross-implementation interop smoke: gtron <-> java-tron private chain.
#
# Preconditions:
#   1. java-tron is already running (the script does NOT start one — it only
#      validates the run). Default address: 127.0.0.1:18888 (override with
#      JAVA_TRON_ADDR=host:port).
#   2. java-tron's genesis matches test/fixtures/cross-impl/java-tron-private.json
#      (single SR, networkId=0, chain_id=9999, the genesis at
#      /Users/asuka/Works/Tests/TVM/run/config.conf).
#
# Optional:
#   JAVA_TRON_HTTP=host:port — java-tron HTTP/JSON port (defaults to 127.0.0.1:8090).
#                              If set and reachable, the script byte-compares block
#                              hashes and SR DPoS counters across both nodes.
#                              If unset/unreachable, only gtron-side sync progress
#                              is checked.
#
# Usage:
#   scripts/system_test_cross.sh
#   JAVA_TRON_ADDR=127.0.0.1:18888 scripts/system_test_cross.sh
#   JAVA_TRON_ADDR=127.0.0.1:18888 JAVA_TRON_HTTP=127.0.0.1:8090 scripts/system_test_cross.sh
#
# Exit:
#   0 on success, 1 on any FAIL.
#
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
GTRON="$BASEDIR/build/bin/gtron"
FIXTURE="$BASEDIR/test/fixtures/cross-impl/java-tron-private.json"
TMPDIR=$(mktemp -d)
DATADIR="$TMPDIR/gtron"

JAVA_TRON_ADDR="${JAVA_TRON_ADDR:-127.0.0.1:18888}"
JAVA_TRON_HTTP="${JAVA_TRON_HTTP:-127.0.0.1:8090}"

# Match the manual procedure documented in the genesis-file-loader plan
# (docs/superpowers/plans/2026-05-02-genesis-file-loader.md slice 3).
GTRON_P2P=19999
GTRON_HTTP=8190
GTRON_JRPC=8546

GTRON_PID=""
PASS=0
FAIL=0
SKIP=0

echo "================================"
echo "  go-tron <-> java-tron cross-impl interop"
echo "================================"
echo "java-tron P2P:  $JAVA_TRON_ADDR"
echo "java-tron HTTP: $JAVA_TRON_HTTP (optional, used for byte-level cross-check)"
echo "gtron datadir:  $DATADIR"
echo "gtron HTTP:     localhost:$GTRON_HTTP"
echo ""

# ── Helpers ───────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "=== Cleanup ==="
    if [ -n "$GTRON_PID" ]; then
        kill "$GTRON_PID" 2>/dev/null || true
        wait "$GTRON_PID" 2>/dev/null || true
    fi
    if [ -f "$TMPDIR/gtron.log" ] && [ "$FAIL" -gt 0 ]; then
        echo ""
        echo "=== gtron log (last 40 lines) ==="
        tail -40 "$TMPDIR/gtron.log" | sed 's/^/  /'
    fi
    rm -rf "$TMPDIR"
    echo ""
    echo "================================"
    echo "  Results: $PASS passed, $FAIL failed, $SKIP skipped"
    echo "================================"
    if [ "$FAIL" -gt 0 ]; then
        exit 1
    fi
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL + 1)); }
skip_step() { echo "  SKIP: $1 ($2)"; SKIP=$((SKIP + 1)); }

check_eq() {
    local desc="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" = "$expected" ]; then
        pass "$desc"
    else
        fail "$desc (expected=$expected, got=$actual)"
    fi
}

http_post() {
    curl -sf --max-time 5 -X POST -H "Content-Type: application/json" \
        -d "$3" "http://$1$2" 2>/dev/null || echo ""
}

http_get() {
    curl -sf --max-time 5 "http://$1$2" 2>/dev/null || echo ""
}

json_field() {
    python3 -c "import sys,json; d=json.load(sys.stdin); print($1)" 2>/dev/null <<< "$2"
}

# ── Preflight ─────────────────────────────────────────────────────
echo "=== Preflight ==="
if [ ! -f "$FIXTURE" ]; then
    fail "fixture not found: $FIXTURE"
    exit 1
fi
pass "fixture present: $FIXTURE"

if [ ! -f "$GTRON" ]; then
    echo "Building gtron..."
    (cd "$BASEDIR" && go build -o build/bin/gtron ./cmd/gtron/)
fi
[ -f "$GTRON" ] && pass "gtron binary present" || { fail "gtron build failed"; exit 1; }

# Check java-tron P2P reachable
JAVA_TRON_HOST="${JAVA_TRON_ADDR%:*}"
JAVA_TRON_PORT="${JAVA_TRON_ADDR##*:}"
if ! python3 -c "
import socket, sys
try:
    s = socket.create_connection(('$JAVA_TRON_HOST', $JAVA_TRON_PORT), timeout=3)
    s.close()
    sys.exit(0)
except Exception as e:
    print(f'  not reachable: {e}', file=sys.stderr)
    sys.exit(1)
"; then
    fail "java-tron P2P at $JAVA_TRON_ADDR is not reachable"
    echo ""
    echo "  Start a java-tron private chain first; see docs/dev/java-tron-local.md."
    exit 1
fi
pass "java-tron P2P reachable at $JAVA_TRON_ADDR"

# Probe java-tron HTTP (optional)
HAVE_JAVA_HTTP=0
if [ -n "$JAVA_TRON_HTTP" ]; then
    if http_post "$JAVA_TRON_HTTP" "/wallet/getnowblock" '{}' | grep -q '"block_header"'; then
        HAVE_JAVA_HTTP=1
        pass "java-tron HTTP reachable at $JAVA_TRON_HTTP (cross-check enabled)"
    else
        skip_step "java-tron HTTP cross-check" "HTTP at $JAVA_TRON_HTTP not reachable"
    fi
fi

# ── Init gtron with the cross-impl fixture ───────────────────────
echo ""
echo "=== Init gtron ==="
"$GTRON" init --genesis "$FIXTURE" --datadir "$DATADIR" \
    > "$TMPDIR/init.log" 2>&1 || { cat "$TMPDIR/init.log"; fail "gtron init failed"; exit 1; }
INIT_HASH=$(grep -oE 'hash=[0-9a-f]+' "$TMPDIR/init.log" | head -1 | cut -d= -f2 || echo "")
if [ -n "$INIT_HASH" ]; then
    pass "gtron init produced genesis hash: ${INIT_HASH:0:16}..."
else
    fail "gtron init did not log a genesis hash"
fi

# ── Start gtron pointing at java-tron ────────────────────────────
echo ""
echo "=== Start gtron ==="
"$GTRON" --datadir "$DATADIR" \
    --genesis "$FIXTURE" \
    --p2p.port "$GTRON_P2P" \
    --http.port "$GTRON_HTTP" \
    --jsonrpc.port "$GTRON_JRPC" \
    --grpc.port 0 \
    --seednode "$JAVA_TRON_ADDR" \
    > "$TMPDIR/gtron.log" 2>&1 &
GTRON_PID=$!
echo "gtron PID=$GTRON_PID"

# Wait for HTTP to come up
echo "Waiting for gtron HTTP (max 30s)..."
tries=0
until http_post "127.0.0.1:$GTRON_HTTP" "/wallet/getnowblock" '{}' | grep -q '"block_header"'; do
    tries=$((tries + 1))
    if [ $tries -ge 30 ]; then
        fail "gtron HTTP never came up"
        exit 1
    fi
    sleep 1
done
pass "gtron HTTP up after ${tries}s"

# ── Verify gtron syncs from java-tron ────────────────────────────
echo ""
echo "=== Sync from java-tron ==="
echo "Polling gtron block height (max 90s, expect monotonic increase past 0)..."
SYNC_TARGET=10  # any block past genesis is enough to confirm sync started
GTRON_HEAD=0
deadline=$(( $(date +%s) + 90 ))
while [ "$(date +%s)" -lt $deadline ]; do
    blk=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/getnowblock" '{}')
    GTRON_HEAD=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$blk" || echo "0")
    if [ "$GTRON_HEAD" -ge "$SYNC_TARGET" ] 2>/dev/null; then
        break
    fi
    sleep 2
done

if [ "$GTRON_HEAD" -ge "$SYNC_TARGET" ] 2>/dev/null; then
    pass "gtron synced past block #$SYNC_TARGET (current head: #$GTRON_HEAD)"
else
    fail "gtron did not sync (head: #$GTRON_HEAD, target: #$SYNC_TARGET)"
fi

# ── Cross-check against java-tron HTTP (if available) ────────────
if [ "$HAVE_JAVA_HTTP" -eq 1 ] && [ "$GTRON_HEAD" -gt 0 ]; then
    echo ""
    echo "=== Cross-check against java-tron HTTP ==="

    # Block #1 hash
    GTRON_B1=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/getblockbynum" '{"num":1}')
    JAVA_B1=$(http_post "$JAVA_TRON_HTTP" "/wallet/getblockbynum" '{"num":1}')
    GTRON_B1_ID=$(json_field "d.get('blockID','')" "$GTRON_B1" || echo "")
    JAVA_B1_ID=$(json_field "d.get('blockID','')" "$JAVA_B1" || echo "")
    if [ -n "$GTRON_B1_ID" ] && [ -n "$JAVA_B1_ID" ]; then
        check_eq "block #1 blockID byte-identical" "$GTRON_B1_ID" "$JAVA_B1_ID"
    else
        skip_step "block #1 cross-check" "missing blockID on one side"
    fi

    # Block at gtron head (java-tron may be slightly ahead)
    GTRON_HEAD_BLK=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/getblockbynum" "{\"num\":$GTRON_HEAD}")
    JAVA_HEAD_BLK=$(http_post "$JAVA_TRON_HTTP" "/wallet/getblockbynum" "{\"num\":$GTRON_HEAD}")
    GTRON_HEAD_ID=$(json_field "d.get('blockID','')" "$GTRON_HEAD_BLK" || echo "")
    JAVA_HEAD_ID=$(json_field "d.get('blockID','')" "$JAVA_HEAD_BLK" || echo "")
    if [ -n "$GTRON_HEAD_ID" ] && [ -n "$JAVA_HEAD_ID" ]; then
        check_eq "block #$GTRON_HEAD blockID byte-identical" "$GTRON_HEAD_ID" "$JAVA_HEAD_ID"
    else
        skip_step "block #$GTRON_HEAD cross-check" "missing blockID on one side"
    fi

    # Mid-range block
    if [ "$GTRON_HEAD" -gt 4 ]; then
        MID=$((GTRON_HEAD / 2))
        GTRON_MID=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/getblockbynum" "{\"num\":$MID}")
        JAVA_MID=$(http_post "$JAVA_TRON_HTTP" "/wallet/getblockbynum" "{\"num\":$MID}")
        GTRON_MID_ID=$(json_field "d.get('blockID','')" "$GTRON_MID" || echo "")
        JAVA_MID_ID=$(json_field "d.get('blockID','')" "$JAVA_MID" || echo "")
        if [ -n "$GTRON_MID_ID" ] && [ -n "$JAVA_MID_ID" ]; then
            check_eq "block #$MID blockID byte-identical" "$GTRON_MID_ID" "$JAVA_MID_ID"
        fi
    fi

    # Active witness DPoS counters (single-SR private chain)
    GTRON_WL=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/listwitnesses" '{}')
    JAVA_WL=$(http_post "$JAVA_TRON_HTTP" "/wallet/listwitnesses" '{}')

    # Pick the SR that has produced blocks (totalProduced > 0) on java-tron
    JAVA_SR=$(python3 <<EOF 2>/dev/null
import json, sys
d = json.loads('''$JAVA_WL''')
ws = d.get('witnesses', [])
active = [w for w in ws if int(w.get('totalProduced', 0)) > 0]
print(active[0]['address'] if active else (ws[0]['address'] if ws else ''))
EOF
)
    if [ -n "$JAVA_SR" ]; then
        for FIELD in totalProduced totalMissed latestBlockNum latestSlotNum; do
            G=$(python3 <<EOF 2>/dev/null
import json
d = json.loads('''$GTRON_WL''')
for w in d.get('witnesses', []):
    if w.get('address') == '$JAVA_SR':
        print(w.get('$FIELD', '')); break
else:
    print('')
EOF
)
            J=$(python3 <<EOF 2>/dev/null
import json
d = json.loads('''$JAVA_WL''')
for w in d.get('witnesses', []):
    if w.get('address') == '$JAVA_SR':
        print(w.get('$FIELD', '')); break
else:
    print('')
EOF
)
            if [ -n "$G" ] && [ -n "$J" ]; then
                check_eq "SR $FIELD byte-identical" "$G" "$J"
            else
                skip_step "SR $FIELD cross-check" "field missing on one side"
            fi
        done
    else
        skip_step "SR DPoS counter cross-check" "no active witness on java-tron"
    fi
else
    skip_step "byte-level cross-check" "java-tron HTTP not configured"
fi

echo ""
echo "=== Done ==="
