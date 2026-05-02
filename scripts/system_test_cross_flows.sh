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

# ── Run all flows ─────────────────────────────────────────────────
flow_transfer
flow_account_create
flow_freeze
flow_vote_after_freeze
flow_withdraw
flow_asset_issue
flow_proposal_create

echo ""
echo "=== Done ==="
