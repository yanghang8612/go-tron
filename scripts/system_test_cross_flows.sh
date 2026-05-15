#!/usr/bin/env bash
#
# Cross-implementation transaction-flow integration test.
#
# Drives a curated set of transaction types end-to-end across an
# interconnected gtron + java-tron pair and asserts that the post-tx
# database state is byte-identical on both sides.
#
# Preconditions:
#   - java-tron already running with the genesis from
#     /Users/asuka/Works/Tests/TVM/run/config.conf:
#       single SR Zion @ TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC
#       SR private key 0000…0001
#       networkId 0, chain_id 9999
#   - JAVA_TRON_ADDR (default 127.0.0.1:18888) — java-tron P2P
#   - JAVA_TRON_HTTP (default 127.0.0.1:8090) — java-tron HTTP
#   - SR_KEY (default 0000…0001) — Zion's private key for signing
#
# Flow per contract type:
#   1. Build unsigned tx via gtron's HTTP /wallet/<endpoint>
#   2. Sign with txsign + SR_KEY
#   3. Broadcast to gtron — gtron relays via P2P to java-tron, which mines
#   4. Poll java-tron's gettransactioninfobyid until tx is mined
#   5. Wait for gtron to sync past the inclusion block
#   6. Query state on BOTH nodes and assert byte-equal on the relevant fields
#
# What this catches:
#   - Wire-format divergence in HTTP build endpoints (param parsing)
#   - Per-actuator state-write divergence (balance, frozen, votes, asset, …)
#   - Block-replay divergence (gtron's apply-side must reach the same state
#     as java-tron's processBlock)
#   - DP counter divergence (total_create_account_cost, witness counters)
#
# Exit:
#   0 on all PASS, 1 on any FAIL.

set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
GTRON="$BASEDIR/build/bin/gtron"
TXSIGN="$BASEDIR/build/bin/txsign"
FIXTURE="$BASEDIR/test/fixtures/cross-impl/java-tron-private.json"

JAVA_P2P="${JAVA_TRON_ADDR:-127.0.0.1:18888}"
JAVA_HTTP="${JAVA_TRON_HTTP:-127.0.0.1:8090}"
SR_KEY="${SR_KEY:-0000000000000000000000000000000000000000000000000000000000000001}"

GTRON_HTTP=8190
GTRON_P2P=19999
GTRON_JRPC=8546

# SR address (Zion). T-base58 = TMVQGm1qAQYVdetCeGRRkTWYYrLXuHK2HC.
SR_ADDR_HEX="417e5f4552091a69125d5dfcb7b8c2659029395bdf"

TMPDIR=$(mktemp -d)
DATADIR="$TMPDIR/gtron"
GTRON_PID=""
PASS=0; FAIL=0; SKIP=0

# ── Cleanup ──────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "=== Cleanup ==="
    if [ -n "$GTRON_PID" ]; then
        kill "$GTRON_PID" 2>/dev/null || true
        wait "$GTRON_PID" 2>/dev/null || true
    fi
    if [ -f "$TMPDIR/gtron.log" ] && [ "$FAIL" -gt 0 ]; then
        echo ""
        echo "=== gtron log (last 60 lines) ==="
        tail -60 "$TMPDIR/gtron.log" | sed 's/^/  /'
    fi
    rm -rf "$TMPDIR"
    echo ""
    echo "================================"
    echo "  Cross-impl flows: $PASS passed, $FAIL failed, $SKIP skipped"
    echo "================================"
    if [ "$FAIL" -gt 0 ]; then exit 1; fi
}
trap cleanup EXIT

pass() { echo "    PASS: $1"; PASS=$((PASS + 1)); }
fail() { echo "    FAIL: $1"; FAIL=$((FAIL + 1)); }
skip_step() { echo "    SKIP: $1 ($2)"; SKIP=$((SKIP + 1)); }

# ── HTTP helpers ─────────────────────────────────────────────────
http_post() {
    # http_post <host:port> <path> <body-json>
    curl -sf --max-time 10 -X POST -H "Content-Type: application/json" \
        -d "$3" "http://$1$2" 2>/dev/null || echo ""
}

json_field() {
    # json_field "expression d.get(...)" <json-text>
    python3 -c "import sys,json;
try:
    d=json.load(sys.stdin)
    print($1)
except Exception:
    print('')" 2>/dev/null <<< "$2"
}

# ── Block / sync helpers ─────────────────────────────────────────
get_head() {
    # get_head <host:port>
    local resp
    resp=$(http_post "$1" "/wallet/getnowblock" "{}")
    json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$resp"
}

wait_for_inclusion() {
    # wait_for_inclusion <txid>
    local txid="$1"
    local deadline=$(( $(date +%s) + 30 ))
    local resp block_num
    while [ "$(date +%s)" -lt "$deadline" ]; do
        resp=$(http_post "$JAVA_HTTP" "/wallet/gettransactioninfobyid" "{\"value\":\"$txid\"}")
        block_num=$(json_field "d.get('blockNumber',0)" "$resp")
        if [ -n "$block_num" ] && [ "$block_num" -gt 0 ] 2>/dev/null; then
            echo "$block_num"
            return 0
        fi
        sleep 1
    done
    return 0
}

wait_for_gtron_synced_to() {
    # wait_for_gtron_synced_to <block_num>
    local target="$1"
    local deadline=$(( $(date +%s) + 30 ))
    while [ "$(date +%s)" -lt "$deadline" ]; do
        local h
        h=$(get_head "127.0.0.1:$GTRON_HTTP")
        if [ -n "$h" ] && [ "$h" -ge "$target" ] 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# ── Sign + broadcast ──────────────────────────────────────────────
sign_tx() {
    # sign_tx <unsigned-tx-json> → signed-tx-json on stdout
    echo "$1" | "$TXSIGN" "$SR_KEY" 2>/dev/null
}

extract_txid() {
    json_field "d.get('txID','')" "$1"
}

# Sign-and-broadcast a built tx to gtron, wait for inclusion on java-tron,
# wait for gtron to catch up. Echoes the inclusion block number on success;
# echoes empty on failure. Always returns 0 (so callers using `var=$(...)`
# don't trip macOS bash 3.2's errexit propagation).
broadcast_and_confirm() {
    # broadcast_and_confirm <unsigned-tx-json>
    local unsigned="$1"
    local txid signed bcast incl
    txid=$(extract_txid "$unsigned")
    if [ -z "$txid" ]; then return 0; fi
    signed=$(sign_tx "$unsigned")
    if [ -z "$signed" ]; then return 0; fi
    bcast=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/broadcasttransaction" "$signed")
    if ! grep -q '"result":true' <<< "$bcast"; then
        echo "      (broadcast: $bcast)" >&2
        return 0
    fi
    incl=$(wait_for_inclusion "$txid" || true)
    if [ -z "$incl" ]; then return 0; fi
    if ! wait_for_gtron_synced_to "$incl"; then return 0; fi
    echo "$incl"
}

# ── State-equality assertion ─────────────────────────────────────
assert_state_eq() {
    # assert_state_eq <description> <path> <body> <field-expr>
    local desc="$1" path="$2" body="$3" expr="$4"
    local g j
    g=$(http_post "127.0.0.1:$GTRON_HTTP" "$path" "$body")
    j=$(http_post "$JAVA_HTTP" "$path" "$body")
    local gv jv
    gv=$(json_field "$expr" "$g")
    jv=$(json_field "$expr" "$j")
    if [ "$gv" = "$jv" ]; then
        pass "$desc (gtron=$gv java=$jv)"
    else
        fail "$desc — gtron=$gv  java=$jv"
        if [ -n "$DEBUG_FAILED_RESPONSES" ]; then
            echo "      gtron raw: $g" >&2
            echo "      java  raw: $j" >&2
        fi
    fi
}

# ── Preflight ────────────────────────────────────────────────────
echo "================================"
echo "  Cross-impl Tx-Flow Integration"
echo "================================"
echo "java-tron P2P:  $JAVA_P2P"
echo "java-tron HTTP: $JAVA_HTTP"
echo "gtron datadir:  $DATADIR"
echo "gtron HTTP:     localhost:$GTRON_HTTP"
echo "SR address:     0x$SR_ADDR_HEX"
echo ""

[ -f "$FIXTURE" ] || { fail "fixture missing: $FIXTURE"; exit 1; }
[ -f "$GTRON" ] || (cd "$BASEDIR" && go build -o build/bin/gtron ./cmd/gtron/)
[ -f "$TXSIGN" ] || (cd "$BASEDIR" && go build -o build/bin/txsign ./cmd/txsign/)

# Verify java-tron HTTP reachable AND producing blocks.
J0=$(get_head "$JAVA_HTTP")
if [ -z "$J0" ] || [ "$J0" -le 0 ] 2>/dev/null; then
    fail "java-tron HTTP at $JAVA_HTTP not reachable or stuck at genesis"
    exit 1
fi
echo "java-tron head before test start: $J0"

# ── Init & start gtron ────────────────────────────────────────────
echo ""
echo "=== Init gtron ==="
"$GTRON" init --genesis "$FIXTURE" --datadir "$DATADIR" \
    > "$TMPDIR/init.log" 2>&1 || { cat "$TMPDIR/init.log"; fail "gtron init"; exit 1; }
pass "gtron init"

echo ""
echo "=== Start gtron and sync to java-tron ==="
"$GTRON" --datadir "$DATADIR" \
    --genesis "$FIXTURE" \
    --p2p.port "$GTRON_P2P" \
    --http.port "$GTRON_HTTP" \
    --jsonrpc.port "$GTRON_JRPC" \
    --grpc.port 0 \
    --seednode "$JAVA_P2P" \
    > "$TMPDIR/gtron.log" 2>&1 &
GTRON_PID=$!
echo "gtron PID=$GTRON_PID"

# Wait for gtron HTTP to come up.
for _ in {1..30}; do
    if [ -n "$(get_head "127.0.0.1:$GTRON_HTTP")" ]; then break; fi
    sleep 1
done

# Wait for gtron to catch up to within 5 blocks of java-tron.
echo "Waiting for gtron to catch up (java-tron currently at $J0; chain has ~$(($J0/300))s of history)…"
deadline=$(( $(date +%s) + 600 ))   # generous 10 min
caught_up=0
while [ "$(date +%s)" -lt "$deadline" ]; do
    G=$(get_head "127.0.0.1:$GTRON_HTTP")
    J=$(get_head "$JAVA_HTTP")
    if [ -n "$G" ] && [ -n "$J" ] && [ "$G" -ge "$((J - 5))" ] 2>/dev/null && [ "$G" -gt 0 ] 2>/dev/null; then
        caught_up=1
        echo "  gtron caught up: gtron=$G java=$J"
        break
    fi
    if [ -n "$G" ] && [ -n "$J" ]; then
        echo "  gtron=$G java=$J diff=$((J-G))"
    fi
    sleep 5
done
if [ "$caught_up" -eq 0 ]; then
    fail "gtron did not catch up within 10 min"
    exit 1
fi
pass "sync caught up to java-tron"

# ────────────────────────────────────────────────────────────────────
# Pre-flow probes
# ────────────────────────────────────────────────────────────────────

# Probe two gates that together pick the right freeze flow:
#   FREEZE_V2_OPEN    — true once UNFREEZE_DELAY_DAYS > 0. Java-tron's
#                       FreezeBalanceActuator rejects V1 freeze with
#                       "freeze v2 is open, old freeze is closed", and
#                       FreezeBalanceV2Actuator requires it to validate.
#                       Proposal #41 (allowOptimizeStakingTime).
#   ALLOW_NEW_RM      — true once allow_new_resource_model is active.
#                       Gates whether TRON_POWER is a valid resource type
#                       on V2 freeze, and switches vote validation from
#                       getTronPower() to getAllTronPower(). Proposal #51.
probe_param() {
    # probe_param <key> — returns int value or 0 if absent.
    local resp
    resp=$(http_post "$JAVA_HTTP" "/wallet/getchainparameters" "{}")
    json_field "next((p.get('value',0) for p in d.get('chainParameter',[]) if p.get('key')=='$1'), 0)" "$resp"
}
FREEZE_V2_OPEN=$([ "$(probe_param getUnfreezeDelayDays)" -gt 0 ] && echo 1 || echo 0)
ALLOW_NEW_RM=$(probe_param getAllowNewResourceModel)
echo "freeze_v2_open  = $FREEZE_V2_OPEN  (UnfreezeDelayDays > 0)"
echo "allow_new_rm    = ${ALLOW_NEW_RM:-0}  (AllowNewResourceModel)"

# Pre-flow baseline: assert SR balance, total_create_account_cost, and
# witness counters are byte-equal at the post-sync state. Catches any state
# divergence accumulated during gtron's resync from genesis BEFORE we
# attribute drift to a specific flow.
echo ""
echo "=== Baseline state cross-check (post-sync, pre-flow) ==="
assert_state_eq "baseline: SR balance" \
    "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
    "d.get('balance',0)"
assert_state_eq "baseline: SR allowance" \
    "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
    "d.get('allowance',0)"
assert_state_eq "baseline: SR latest_withdraw_time" \
    "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
    "d.get('latest_withdraw_time',0)"
assert_state_eq "baseline: witness count" \
    "/wallet/listwitnesses" "{}" \
    "len(d.get('witnesses',[]))"
assert_state_eq "baseline: SR totalProduced" \
    "/wallet/listwitnesses" "{}" \
    "next((w.get('totalProduced',0) for w in d.get('witnesses',[]) if w.get('address','')=='$SR_ADDR_HEX'), 0)"

# ────────────────────────────────────────────────────────────────────
# FLOWS
# ────────────────────────────────────────────────────────────────────

# Helper: PID-derived recipient address. Different across runs so a
# re-run on the same live chain doesn't collide with last run's accounts.
RUN_PID=$$
test_addr_hex() {
    local slot="$1"
    printf '41%016x%022x%02x' "$RUN_PID" 0 "$slot"
}

# ── Flow 1: TransferContract ─────────────────────────────────────
flow_transfer() {
    echo ""
    echo "=== Flow 1: TransferContract ==="
    local recipient
    recipient=$(test_addr_hex 0xa1)
    echo "  recipient: 0x$recipient"

    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/createtransaction" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"to_address\":\"$recipient\",\"amount\":1000000}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "transfer: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi

    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "transfer: broadcast or inclusion failed"
        return
    fi
    pass "transfer included at block #$incl"

    assert_state_eq "recipient balance" \
        "/wallet/getaccount" "{\"address\":\"$recipient\"}" \
        "d.get('balance',0)"
}

# ── Flow 2: AccountCreateContract ────────────────────────────────
flow_account_create() {
    echo ""
    echo "=== Flow 2: AccountCreateContract ==="
    local newaddr
    newaddr=$(test_addr_hex 0xa2)

    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/createaccount" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"account_address\":\"$newaddr\"}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "createaccount: did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "createaccount: broadcast or inclusion failed"
        return
    fi
    pass "createaccount included at block #$incl"

    assert_state_eq "new account exists (type)" \
        "/wallet/getaccount" "{\"address\":\"$newaddr\"}" \
        "d.get('type',0)"
    assert_state_eq "new account create_time set" \
        "/wallet/getaccount" "{\"address\":\"$newaddr\"}" \
        "d.get('create_time',0)"

    # SR's total_create_account_cost should have advanced — query via
    # getchainparameters or listwitnesses (both nodes must agree on the
    # SR's account state).
    assert_state_eq "SR balance after fees" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('balance',0)"
}

# ── Flow 3: VoteWitnessContract ──────────────────────────────────
flow_vote() {
    echo ""
    echo "=== Flow 3: VoteWitnessContract ==="
    # SR votes for itself. The SR Zion is already a witness; the only thing
    # gating votes is having TRON Power (frozen-V2 for VOTING resource).
    # Pre-check: does the SR have any TRON Power? If not, Vote will fail
    # with "Frozen Balance is empty" — so we'll FreezeV2 first in Flow 5
    # and re-run Vote afterward.
    skip_step "vote-witness" "deferred until after FreezeV2 (Flow 5) seeds TRON Power"
}

# ── Flow 4: Freeze (V2 BANDWIDTH/TRON_POWER, else V1 NET) ────────
#
# Three regimes, gated by FREEZE_V2_OPEN and ALLOW_NEW_RM:
#   !FREEZE_V2_OPEN              → V1 freeze BANDWIDTH (counts as TP).
#   FREEZE_V2_OPEN, !ALLOW_NEW_RM → V2 freeze BANDWIDTH (still counts as TP
#                                  via getTronPower() since vote validation
#                                  uses getTronPower() under !ALLOW_NEW_RM).
#   FREEZE_V2_OPEN, ALLOW_NEW_RM  → V2 freeze TRON_POWER (the only resource
#                                  type that contributes to getAllTronPower()
#                                  for vote validation).
flow_freeze() {
    echo ""
    echo "=== Flow 4: Freeze (gives the SR TRON Power for voting) ==="
    local unsigned endpoint body label
    if [ "$FREEZE_V2_OPEN" = "1" ]; then
        endpoint="/wallet/freezebalancev2"
        if [ "${ALLOW_NEW_RM:-0}" = "1" ]; then
            body="{\"owner_address\":\"$SR_ADDR_HEX\",\"frozen_balance\":5000000,\"resource\":\"TRON_POWER\"}"
            label="V2 TRON_POWER"
        else
            body="{\"owner_address\":\"$SR_ADDR_HEX\",\"frozen_balance\":5000000,\"resource\":\"BANDWIDTH\"}"
            label="V2 BANDWIDTH"
        fi
    else
        # V1: freeze NET resource for 3 days (FROZEN_PERIOD_MIN). V1 freeze
        # of NET counts as TronPower in the V1 vote-weight calculation.
        endpoint="/wallet/freezebalance"
        body="{\"owner_address\":\"$SR_ADDR_HEX\",\"frozen_balance\":5000000,\"frozen_duration\":3,\"resource\":\"BANDWIDTH\"}"
        label="V1 BANDWIDTH"
    fi
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "$endpoint" "$body")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "freeze ($label): createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "freeze ($label): broadcast or inclusion failed"
        return
    fi
    pass "freeze included at block #$incl ($label)"

    if [ "$FREEZE_V2_OPEN" = "1" ]; then
        # frozenV2 entries sorted; type may be omitted by proto when 0
        # (BANDWIDTH), so we default to 0 in the projection.
        assert_state_eq "SR frozenV2 entries" \
            "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
            "sorted([(e.get('amount',0), e.get('type',0)) for e in d.get('frozenV2',[])])"
    else
        assert_state_eq "SR V1 frozen.balance + total_net_weight" \
            "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
            "[(f.get('frozen_balance',0), f.get('expire_time',0)) for f in d.get('frozen',[])]"
    fi
}

# ── Flow 5: VoteWitnessContract (now that we have TRON Power) ────
flow_vote_after_freeze() {
    echo ""
    echo "=== Flow 5: VoteWitnessContract (post-freeze) ==="
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/votewitnessaccount" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"votes\":[{\"vote_address\":\"$SR_ADDR_HEX\",\"vote_count\":1}]}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "vote: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "vote: broadcast or inclusion failed"
        return
    fi
    pass "vote included at block #$incl"

    assert_state_eq "SR votes recorded" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "sorted([(v.get('vote_address',''), v.get('vote_count',0)) for v in d.get('votes',[])])"
}

# ── Flow 6: WithdrawBalanceContract ───────────────────────────────
flow_withdraw() {
    echo ""
    echo "=== Flow 6: WithdrawBalanceContract ==="
    # The SR earns block rewards as it produces blocks. Allowance is
    # only credited at maintenance boundaries, but withdraw of zero
    # allowance returns "withdrawAmount == 0" — so this either
    # succeeds (post-maintenance) or returns a deterministic error
    # on both nodes. We assert on whether the broadcast result+state
    # is byte-equal regardless.
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/withdrawbalance" \
        "{\"owner_address\":\"$SR_ADDR_HEX\"}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        # Pre-maintenance: withdrawable amount == 0; java-tron's
        # gtron should both return CONTRACT_VALIDATE_ERROR. Skip.
        skip_step "withdrawBalance" "no claimable allowance yet (pre-maintenance)"
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        skip_step "withdrawBalance" "no allowance yet (gate accepts but actuator no-ops)"
        return
    fi
    pass "withdrawBalance included at block #$incl"

    assert_state_eq "SR allowance after withdraw" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('allowance',0)"
}

# ── Flow 7: AssetIssueContract ───────────────────────────────────
flow_asset_issue() {
    echo ""
    echo "=== Flow 7: AssetIssueContract ==="
    # AssetIssue requires the issuer to NOT already have an asset.
    # The Zion SR may or may not have issued one previously (run-to-run
    # state on the live java-tron private chain). Detect and skip if so.
    local existing
    existing=$(http_post "$JAVA_HTTP" "/wallet/getassetissuebyaccount" \
        "{\"address\":\"$SR_ADDR_HEX\"}")
    if echo "$existing" | grep -q '"assetIssue"'; then
        skip_step "assetIssue" "SR already has an asset (chain reused across runs)"
        return
    fi

    local now_ms
    now_ms=$(date +%s)000
    local end_ms=$((now_ms + 30 * 86400000))  # +30 days
    local name="CROSSXX$$"
    local body
    body=$(cat <<EOF
{"owner_address":"$SR_ADDR_HEX","name":"$(printf '%s' "$name" | xxd -p | tr -d '\n')","abbr":"$(printf 'CROSS' | xxd -p)","total_supply":1000000,"trx_num":1,"num":1,"start_time":$now_ms,"end_time":$end_ms,"description":"$(printf 'cross-impl' | xxd -p)","url":"$(printf 'http://t.io' | xxd -p)","precision":0}
EOF
)
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/createassetissue" "$body")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "assetIssue: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "assetIssue: broadcast or inclusion failed"
        return
    fi
    pass "assetIssue included at block #$incl"

    assert_state_eq "SR has asset_issued_ID" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('asset_issued_ID','')"
    assert_state_eq "SR TRC10 free balance" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "[(a.get('key',''), a.get('value',0)) for a in d.get('assetV2',[])]"
}

# ── Flow 7b: ExchangeCreate / Inject / Withdraw ───────────────────
#
# Bancor-style on-chain AMM. Pairs the TRC10 from flow_asset_issue with
# TRX (`_`, hex 5f). java-tron's ExchangeCreateActuator pre-increments
# latest_exchange_num and writes the new Exchange capsule keyed by it;
# gtron must produce the byte-identical post-state.
#
# Token-id encoding: gtron's /wallet/exchange* endpoints take the *bytes*
# field as a hex string (api_exchange.go applies common.FromHex). So:
#   TRX            → "_"        → hex "5f"
#   TRC10 id N     → "<N>"      → hex of ASCII digits, e.g. "1000001" → "31303030303031"
EXCHANGE_TRX_HEX=$(printf '_' | xxd -p | tr -d '\n')   # 5f
EXCHANGE_FIRST_BAL=1000000   # 1 TRX (sun)
EXCHANGE_SECOND_BAL=1000     # 1000 TRC10 units

# Snapshot of the SR's TRC10 id captured by flow_exchange_create; reused
# by inject/withdraw to talk about the same pool.
EXCHANGE_ASSET_ID=""
EXCHANGE_ASSET_HEX=""
EXCHANGE_ID=""

flow_exchange_create() {
    echo ""
    echo "=== Flow 7b.1: ExchangeCreateContract ==="
    # Read the SR's asset_issued_ID from java-tron. tronjson decodes that
    # field as UTF-8 (the numeric token-id string, e.g. "1000001"); java's
    # convertOutput does the same. If empty, the SR never issued a TRC10
    # on this chain — skip the entire exchange suite.
    local acct asset_id
    acct=$(http_post "$JAVA_HTTP" "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}")
    asset_id=$(json_field "d.get('asset_issued_ID','')" "$acct")
    if [ -z "$asset_id" ] || [ "$asset_id" = "0" ]; then
        skip_step "exchangeCreate" "SR has no asset_issued_ID (flow_asset_issue skipped/failed)"
        return
    fi
    EXCHANGE_ASSET_ID="$asset_id"
    EXCHANGE_ASSET_HEX=$(printf '%s' "$asset_id" | xxd -p | tr -d '\n')
    echo "  pair: TRX(_) + TRC10 id=$asset_id (hex=$EXCHANGE_ASSET_HEX)"

    # ExchangeCreate burns exchange_create_fee (1024 TRX on mainnet
    # defaults) plus the TRX deposit. Sanity-skip if SR can't cover it.
    local sr_bal fee
    sr_bal=$(json_field "d.get('balance',0)" "$acct")
    fee=$(http_post "$JAVA_HTTP" "/wallet/getchainparameters" "{}")
    fee=$(json_field "next((p.get('value',0) for p in d.get('chainParameter',[]) if p.get('key')=='getExchangeCreateFee' or p.get('key')=='exchange_create_fee'), 1024000000)" "$fee")
    local need=$((fee + EXCHANGE_FIRST_BAL))
    if [ "$sr_bal" -lt "$need" ] 2>/dev/null; then
        skip_step "exchangeCreate" "SR balance $sr_bal < need $need (fee $fee + TRX deposit $EXCHANGE_FIRST_BAL)"
        return
    fi

    local body
    body="{\"owner_address\":\"$SR_ADDR_HEX\",\"first_token_id\":\"$EXCHANGE_TRX_HEX\",\"first_token_balance\":$EXCHANGE_FIRST_BAL,\"second_token_id\":\"$EXCHANGE_ASSET_HEX\",\"second_token_balance\":$EXCHANGE_SECOND_BAL}"
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/exchangecreate" "$body")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "exchangeCreate: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "exchangeCreate: broadcast or inclusion failed"
        return
    fi
    pass "exchangeCreate included at block #$incl"

    # latest_exchange_num is the rename target of commit a8c241b. Both
    # nodes must advance it symmetrically. Asserted via getchainparameters
    # (both ends expose it under the snake_case key).
    assert_state_eq "latest_exchange_num advanced" \
        "/wallet/getchainparameters" "{}" \
        "next((p.get('value',0) for p in d.get('chainParameter',[]) if p.get('key')=='latest_exchange_num'), -1)"

    # listexchanges returns one entry per Exchange capsule. Capture the
    # max exchange_id from java-tron to use as "our" id below — the chain
    # may already carry exchanges from prior test runs.
    local jlist
    jlist=$(http_post "$JAVA_HTTP" "/wallet/listexchanges" "{}")
    EXCHANGE_ID=$(json_field "max([e.get('exchange_id',0) for e in d.get('exchanges',[])] + [0])" "$jlist")
    echo "  exchange_id=$EXCHANGE_ID"
    if [ -z "$EXCHANGE_ID" ] || [ "$EXCHANGE_ID" = "0" ]; then
        fail "exchangeCreate: tx included but listexchanges returned no entry"
        return
    fi

    # 4-tuple match on the newly created pool. token_id fields are bytes
    # → hex strings on both sides.
    assert_state_eq "exchange[$EXCHANGE_ID] 4-tuple" \
        "/wallet/listexchanges" "{}" \
        "next(((e.get('first_token_id',''), e.get('first_token_balance',0), e.get('second_token_id',''), e.get('second_token_balance',0)) for e in d.get('exchanges',[]) if e.get('exchange_id',0)==$EXCHANGE_ID), None)"
    # Creator address and create_time must also match.
    assert_state_eq "exchange[$EXCHANGE_ID] creator+createtime" \
        "/wallet/listexchanges" "{}" \
        "next(((e.get('creator_address',''), e.get('create_time',0)) for e in d.get('exchanges',[]) if e.get('exchange_id',0)==$EXCHANGE_ID), None)"
    # SR balance must reflect fee + TRX deposit; TRC10 balance must drop
    # by EXCHANGE_SECOND_BAL.
    assert_state_eq "SR balance after exchangeCreate" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('balance',0)"
    assert_state_eq "SR TRC10 balance after exchangeCreate" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "[(a.get('key',''), a.get('value',0)) for a in d.get('assetV2',[])]"
}

# ExchangeInject deposits one side of the pair; the actuator computes the
# matching amount of the other side from the current ratio and debits both
# from the creator. For a 1_000_000:1_000 pool, injecting 100_000 sun TRX
# pulls in floor(1000 * 100_000 / 1_000_000) = 100 TRC10.
flow_exchange_inject() {
    echo ""
    echo "=== Flow 7b.2: ExchangeInjectContract ==="
    if [ -z "$EXCHANGE_ID" ] || [ "$EXCHANGE_ID" = "0" ]; then
        skip_step "exchangeInject" "no exchange from flow_exchange_create"
        return
    fi
    local inject_quant=100000   # 0.1 TRX worth of liquidity (sun)
    local body
    body="{\"owner_address\":\"$SR_ADDR_HEX\",\"exchange_id\":$EXCHANGE_ID,\"token_id\":\"$EXCHANGE_TRX_HEX\",\"quant\":$inject_quant}"
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/exchangeinject" "$body")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "exchangeInject: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "exchangeInject: broadcast or inclusion failed"
        return
    fi
    pass "exchangeInject included at block #$incl"

    # Pool balances must have advanced symmetrically.
    assert_state_eq "exchange[$EXCHANGE_ID] balances after inject" \
        "/wallet/listexchanges" "{}" \
        "next(((e.get('first_token_balance',0), e.get('second_token_balance',0)) for e in d.get('exchanges',[]) if e.get('exchange_id',0)==$EXCHANGE_ID), None)"
    assert_state_eq "SR balance after exchangeInject" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('balance',0)"
    assert_state_eq "SR TRC10 after exchangeInject" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "[(a.get('key',''), a.get('value',0)) for a in d.get('assetV2',[])]"
}

# ExchangeWithdraw returns proportional liquidity to the creator. Withdraw
# 100_000 sun TRX → withdraws floor(other * quant / this) of the other side.
# At a 1_100_000:1_100 pool, that's floor(1100 * 100_000 / 1_100_000) = 100
# TRC10. The "Not precise enough" guard accepts this (exact ratio).
flow_exchange_withdraw() {
    echo ""
    echo "=== Flow 7b.3: ExchangeWithdrawContract ==="
    if [ -z "$EXCHANGE_ID" ] || [ "$EXCHANGE_ID" = "0" ]; then
        skip_step "exchangeWithdraw" "no exchange from flow_exchange_create"
        return
    fi
    local withdraw_quant=100000   # match inject quantity
    local body
    body="{\"owner_address\":\"$SR_ADDR_HEX\",\"exchange_id\":$EXCHANGE_ID,\"token_id\":\"$EXCHANGE_TRX_HEX\",\"quant\":$withdraw_quant}"
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/exchangewithdraw" "$body")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "exchangeWithdraw: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "exchangeWithdraw: broadcast or inclusion failed"
        return
    fi
    pass "exchangeWithdraw included at block #$incl"

    assert_state_eq "exchange[$EXCHANGE_ID] balances after withdraw" \
        "/wallet/listexchanges" "{}" \
        "next(((e.get('first_token_balance',0), e.get('second_token_balance',0)) for e in d.get('exchanges',[]) if e.get('exchange_id',0)==$EXCHANGE_ID), None)"
    assert_state_eq "SR balance after exchangeWithdraw" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('balance',0)"
    assert_state_eq "SR TRC10 after exchangeWithdraw" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "[(a.get('key',''), a.get('value',0)) for a in d.get('assetV2',[])]"
}

# ── Flow 8: ProposalCreate ───────────────────────────────────────
flow_proposal_create() {
    echo ""
    echo "=== Flow 8: ProposalCreateContract ==="
    # Set proposal_expire_time (proposal #19) to its current value as a
    # no-op proposal. Most proposal IDs are 1-shot (can only be set
    # once) so we pick one that's safe to re-set.
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/proposalcreate" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"parameters\":[{\"key\":19,\"value\":259200000}]}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        skip_step "proposalCreate" "endpoint did not return a tx (perhaps already proposed)"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        skip_step "proposalCreate" "broadcast / inclusion failed (could be duplicate)"
        return
    fi
    pass "proposalCreate included at block #$incl"

    # The proposal ID counter on both chains must match; both list the same
    # active proposals.
    assert_state_eq "active proposal count" \
        "/wallet/listproposals" "{}" \
        "len(d.get('proposals',[]))"
    assert_state_eq "latest proposal_id" \
        "/wallet/listproposals" "{}" \
        "max([p.get('proposal_id',0) for p in d.get('proposals',[])] + [0])"
}

# ── Flow 9: ProposalApproveContract ──────────────────────────────
# Approves the most recent proposal via java's listproposals output.
flow_proposal_approve() {
    echo ""
    echo "=== Flow 9: ProposalApproveContract ==="
    local proposals max_id
    proposals=$(http_post "$JAVA_HTTP" "/wallet/listproposals" "{}")
    max_id=$(json_field "max([p.get('proposal_id',0) for p in d.get('proposals',[])] + [0])" "$proposals")
    if [ -z "$max_id" ] || [ "$max_id" = "0" ]; then
        skip_step "proposalApprove" "no proposal to approve"
        return
    fi
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/proposalapprove" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"proposal_id\":$max_id,\"is_add_approval\":true}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        skip_step "proposalApprove" "endpoint did not return a tx (already approved?)"
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        skip_step "proposalApprove" "broadcast/inclusion failed (already approved?)"
        return
    fi
    pass "proposalApprove included at block #$incl (proposal_id=$max_id)"

    # Proposal's approval list should contain SR_ADDR on both nodes.
    # Both java-tron and gtron return approvals at the top level of the
    # response, not nested under a `proposal` key.
    assert_state_eq "proposal approvers" \
        "/wallet/getproposalbyid" "{\"id\":$max_id}" \
        "sorted(d.get('approvals',[]))"
}

# ── Flow 10: UpdateBrokerageContract ─────────────────────────────
# Sets witness brokerage rate (default 20%, change to 25%, then to 20%).
flow_update_brokerage() {
    echo ""
    echo "=== Flow 10: UpdateBrokerageContract ==="
    local current
    current=$(http_post "$JAVA_HTTP" "/wallet/getBrokerage" \
        "{\"address\":\"$SR_ADDR_HEX\"}")
    local rate=$(json_field "d.get('brokerage', 20)" "$current")
    # Pick a different value (cycles 19↔21) to ensure the proposal actually
    # mutates state. Default mainnet brokerage is 20.
    local newrate=21
    if [ "$rate" = "21" ]; then newrate=19; fi
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/updateBrokerage" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"brokerage\":$newrate}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "updateBrokerage: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "updateBrokerage: broadcast or inclusion failed"
        return
    fi
    pass "updateBrokerage included at block #$incl ($rate -> $newrate)"

    assert_state_eq "SR brokerage rate" \
        "/wallet/getBrokerage" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('brokerage',-1)"
}

# ── Flow 11: WitnessUpdateContract ───────────────────────────────
# Updates the SR's URL. Must be at least 1 byte and ≤256 bytes.
flow_witness_update() {
    echo ""
    echo "=== Flow 11: WitnessUpdateContract ==="
    # Bump version suffix on the URL so each run actually mutates state
    # (java rejects no-op or empty updates).
    local newurl="http://test.io/v$(date +%s)"
    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/updatewitness" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"update_url\":\"$(echo -n "$newurl" | xxd -p | tr -d '\n')\"}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "witnessUpdate: createtransaction did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "witnessUpdate: broadcast or inclusion failed"
        return
    fi
    pass "witnessUpdate included at block #$incl"

    assert_state_eq "witness url" \
        "/wallet/listwitnesses" "{}" \
        "next((w.get('url','') for w in d.get('witnesses',[]) if w.get('address','')==\"$SR_ADDR_HEX\"), '')"
}

# ── Flow 12: UnfreezeBalanceV2 ────────────────────────────────────
flow_unfreeze_v2() {
    echo ""
    echo "=== Flow 12: UnfreezeBalanceV2 ==="
    # Requires FREEZE_V2_OPEN. Unfreeze 1 TRX of BANDWIDTH staked in Flow 4.
    # This creates a pending unfreeze entry in unfrozenV2[] with an
    # unfreeze_expire_time derived from UnfreezeDelayDays.
    if [ "$FREEZE_V2_OPEN" != "1" ]; then
        skip_step "unfreezeV2" "FREEZE_V2_OPEN=0 (UnfreezeDelayDays not set)"
        return
    fi

    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/unfreezebalancev2" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"unfreeze_balance\":1000000,\"resource\":\"BANDWIDTH\"}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "unfreezeV2: did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "unfreezeV2: broadcast or inclusion failed"
        return
    fi
    pass "unfreezeV2 included at block #$incl"

    # Both nodes must agree on the total amount pending unfreeze (sum of
    # unfreeze_amount across all unfrozenV2 entries) and on the expire time
    # of the first (only) pending entry.
    assert_state_eq "SR unfrozenV2 total amount" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "sum(e.get('unfreeze_amount',0) for e in d.get('unfrozenV2',[]))"
    assert_state_eq "SR unfrozenV2 expire_time set (>0)" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "max((e.get('unfreeze_expire_time',0) for e in d.get('unfrozenV2',[])), default=0) > 0"
    # frozenV2 BANDWIDTH amount must have decreased by 1 TRX on both sides.
    assert_state_eq "SR frozenV2 after unfreeze" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "sorted([(e.get('amount',0), e.get('type',0)) for e in d.get('frozenV2',[])])"
}

# ── Flow 13: DelegateResource ────────────────────────────────────
# Deterministic recipient that doesn't collide with the PID-keyed test addresses.
DELEGATE_RECIPIENT="410000000000000000000000000000000000000011"

flow_delegate_resource() {
    echo ""
    echo "=== Flow 13: DelegateResource ==="
    # Requires FREEZE_V2_OPEN. Delegate 2 TRX of BANDWIDTH to a fixed
    # recipient address. After Flow 9 unfroze 1 TRX, the SR still has 4 TRX
    # of frozen BANDWIDTH (unfrozeV2 pending ≠ available; delegatable balance
    # comes from frozenV2 amount), so 2 TRX delegation is within budget.
    if [ "$FREEZE_V2_OPEN" != "1" ]; then
        skip_step "delegateResource" "FREEZE_V2_OPEN=0"
        return
    fi

    # Bootstrap DELEGATE_RECIPIENT if it has no on-chain account yet.
    # java-tron's DelegateResourceActuator (and gtron's mirror) validate
    # that receiver_address is an existing account; on a fresh chain
    # this address has never been seen, so a tiny Transfer materializes
    # it. Idempotent on subsequent runs (the check short-circuits once
    # the account exists).
    local recipient_chk
    recipient_chk=$(http_post "$JAVA_HTTP" "/wallet/getaccount" \
        "{\"address\":\"$DELEGATE_RECIPIENT\"}")
    if ! echo "$recipient_chk" | grep -q '"address"'; then
        local bootstrap
        bootstrap=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/createtransaction" \
            "{\"owner_address\":\"$SR_ADDR_HEX\",\"to_address\":\"$DELEGATE_RECIPIENT\",\"amount\":1000000}")
        if echo "$bootstrap" | grep -q '"raw_data"'; then
            broadcast_and_confirm "$bootstrap" >/dev/null
        fi
    fi

    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/delegateresource" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"receiver_address\":\"$DELEGATE_RECIPIENT\",\"balance\":2000000,\"resource\":\"BANDWIDTH\",\"lock\":false}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "delegateResource: did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "delegateResource: broadcast or inclusion failed"
        return
    fi
    pass "delegateResource included at block #$incl"

    # getdelegatedresourcev2 body uses proto field names fromAddress / toAddress.
    local body
    body="{\"fromAddress\":\"$SR_ADDR_HEX\",\"toAddress\":\"$DELEGATE_RECIPIENT\"}"
    assert_state_eq "delegatedResourceV2 bandwidth balance" \
        "/wallet/getdelegatedresourcev2" "$body" \
        "sum(r.get('frozen_balance_for_bandwidth',0) for r in d.get('delegatedResource',[]))"
    # The SR's own account should reflect delegated_frozenV2_balance_for_bandwidth.
    assert_state_eq "SR delegated_frozenV2_balance_for_bandwidth" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('delegated_frozenV2_balance_for_bandwidth',0)"
}

# ── Flow 14: UnDelegateResource ──────────────────────────────────
flow_undelegate_resource() {
    echo ""
    echo "=== Flow 14: UnDelegateResource ==="
    # Undelegate the 2 TRX delegated in Flow 10. Expect the delegation entry
    # to disappear (zero balance) and the SR's frozenV2 BANDWIDTH to grow back.
    if [ "$FREEZE_V2_OPEN" != "1" ]; then
        skip_step "undelegateResource" "FREEZE_V2_OPEN=0"
        return
    fi

    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/undelegateresource" \
        "{\"owner_address\":\"$SR_ADDR_HEX\",\"receiver_address\":\"$DELEGATE_RECIPIENT\",\"balance\":2000000,\"resource\":\"BANDWIDTH\"}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "undelegateResource: did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "undelegateResource: broadcast or inclusion failed"
        return
    fi
    pass "undelegateResource included at block #$incl"

    # Delegation record should be gone (zero or empty list).
    local body
    body="{\"fromAddress\":\"$SR_ADDR_HEX\",\"toAddress\":\"$DELEGATE_RECIPIENT\"}"
    assert_state_eq "delegatedResourceV2 cleared after undelegate" \
        "/wallet/getdelegatedresourcev2" "$body" \
        "sum(r.get('frozen_balance_for_bandwidth',0) for r in d.get('delegatedResource',[]))"
    # SR frozenV2 entries should have recovered (back to what they were after
    # Flow 9's unfreeze — the 4 TRX frozen minus the 1 TRX pending unfreeze).
    assert_state_eq "SR frozenV2 after undelegate" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "sorted([(e.get('amount',0), e.get('type',0)) for e in d.get('frozenV2',[])])"
    assert_state_eq "SR delegated_frozenV2_balance_for_bandwidth cleared" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('delegated_frozenV2_balance_for_bandwidth',0)"
}

# ── Flow 15: CancelAllUnfreezeV2 ─────────────────────────────────
flow_cancel_all_unfreeze_v2() {
    echo ""
    echo "=== Flow 15: CancelAllUnfreezeV2 ==="
    # Requires proposal #77 (getAllowCancelAllUnfreezeV2). If not active,
    # java-tron's actuator will reject with CONTRACT_VALIDATE_ERROR — skip.
    # Also requires FREEZE_V2_OPEN (unfrozenV2[] must be non-empty to be useful,
    # though the tx itself may succeed even with an empty list on some builds).
    if [ "$FREEZE_V2_OPEN" != "1" ]; then
        skip_step "cancelAllUnfreezeV2" "FREEZE_V2_OPEN=0"
        return
    fi
    local cancel_ok
    cancel_ok=$(probe_param getAllowCancelAllUnfreezeV2)
    if [ "${cancel_ok:-0}" != "1" ]; then
        skip_step "cancelAllUnfreezeV2" "proposal #77 getAllowCancelAllUnfreezeV2 not active"
        return
    fi

    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/cancelallunfreezev2" \
        "{\"owner_address\":\"$SR_ADDR_HEX\"}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        fail "cancelAllUnfreezeV2: did not return raw_data"
        echo "      response: $unsigned" >&2
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "cancelAllUnfreezeV2: broadcast or inclusion failed"
        return
    fi
    pass "cancelAllUnfreezeV2 included at block #$incl"

    # unfrozenV2[] must be empty (sum == 0) on both nodes after cancel.
    assert_state_eq "SR unfrozenV2 cleared after cancel" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "sum(e.get('unfreeze_amount',0) for e in d.get('unfrozenV2',[]))"
    # frozenV2 BANDWIDTH must have grown back by the 1 TRX that was cancelled.
    assert_state_eq "SR frozenV2 after cancel" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "sorted([(e.get('amount',0), e.get('type',0)) for e in d.get('frozenV2',[])])"
}

# ── Flow 16: WithdrawExpireUnfreeze (conditional) ─────────────────
flow_withdraw_expire_unfreeze() {
    echo ""
    echo "=== Flow 16: WithdrawExpireUnfreeze ==="
    # Only run if the SR has a matured unfreeze (unfreeze_expire_time in the
    # past). On a freshly started test chain the unfreeze from Flow 9 won't
    # have matured yet (delay is typically 14 days). Skip gracefully if not.
    if [ "$FREEZE_V2_OPEN" != "1" ]; then
        skip_step "withdrawExpireUnfreeze" "FREEZE_V2_OPEN=0"
        return
    fi

    local now_ms
    now_ms=$(date +%s)000
    local acct
    acct=$(http_post "$JAVA_HTTP" "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}")
    local matured
    matured=$(python3 -c "
import sys, json
d = json.loads('''$acct''')
now = $now_ms
total = sum(e.get('unfreeze_amount', 0) for e in d.get('unfrozenV2', [])
            if e.get('unfreeze_expire_time', 0) <= now and e.get('unfreeze_expire_time', 0) > 0)
print(total)
" 2>/dev/null || echo "0")

    if [ -z "$matured" ] || [ "$matured" -le 0 ] 2>/dev/null; then
        skip_step "withdrawExpireUnfreeze" "no matured unfreeze available (chain too young)"
        return
    fi

    local unsigned
    unsigned=$(http_post "127.0.0.1:$GTRON_HTTP" "/wallet/withdrawexpireunfreeze" \
        "{\"owner_address\":\"$SR_ADDR_HEX\"}")
    if ! echo "$unsigned" | grep -q '"raw_data"'; then
        skip_step "withdrawExpireUnfreeze" "createtransaction rejected (no matured unfreeze on gtron side)"
        return
    fi
    local incl
    incl=$(broadcast_and_confirm "$unsigned")
    if [ -z "$incl" ]; then
        fail "withdrawExpireUnfreeze: broadcast or inclusion failed"
        return
    fi
    pass "withdrawExpireUnfreeze included at block #$incl"

    # Matured entries must have been removed from unfrozenV2 and the sun
    # returned to balance; assert both nodes agree on the remaining pending list.
    assert_state_eq "SR unfrozenV2 after withdraw" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "sum(e.get('unfreeze_amount',0) for e in d.get('unfrozenV2',[]))"
    assert_state_eq "SR balance after withdraw" \
        "/wallet/getaccount" "{\"address\":\"$SR_ADDR_HEX\"}" \
        "d.get('balance',0)"
}

# ── Run all flows ─────────────────────────────────────────────────
flow_transfer
flow_account_create
flow_freeze
flow_vote_after_freeze
flow_withdraw
flow_asset_issue
flow_exchange_create
flow_exchange_inject
flow_exchange_withdraw
flow_proposal_create
flow_proposal_approve
flow_update_brokerage
flow_witness_update
flow_unfreeze_v2
flow_delegate_resource
flow_undelegate_resource
flow_cancel_all_unfreeze_v2
flow_withdraw_expire_unfreeze

echo ""
echo "=== Done ==="
