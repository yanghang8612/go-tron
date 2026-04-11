#!/usr/bin/env bash
#
# Comprehensive multi-node system test for go-tron.
#
# Tests: consensus, P2P sync, transaction lifecycle (build→sign→broadcast→confirm),
# balance verification, transaction info queries, block queries, resource/chain
# parameter queries, smart contract deployment, and cross-node consistency.
#
set -euo pipefail

BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
GTRON="$BASEDIR/build/bin/gtron"
TXSIGN="$BASEDIR/build/bin/txsign"
TMPDIR=$(mktemp -d)
SR_DIR="$TMPDIR/sr"
NODE_DIR="$TMPDIR/node"
SR_HTTP=18090
NODE_HTTP=18091
SR_P2P=19888
NODE_P2P=19889
SR_PID=""
NODE_PID=""
PASS=0
FAIL=0
SKIP=0

# Fixed witness key for reproducibility
WITNESS_KEY="c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4"

# ── Build binaries ────────────────────────────────────────────────
echo "================================"
echo "  go-tron System Test"
echo "================================"
echo ""

if [ ! -f "$GTRON" ]; then
    echo "Building gtron..."
    (cd "$BASEDIR" && go build -o build/bin/gtron ./cmd/gtron/)
fi
if [ ! -f "$TXSIGN" ]; then
    echo "Building txsign..."
    (cd "$BASEDIR" && go build -o build/bin/txsign ./cmd/txsign/)
fi
echo "Binaries ready."
echo ""

# ── Helpers ───────────────────────────────────────────────────────
cleanup() {
    echo ""
    echo "=== Cleanup ==="
    [ -n "$SR_PID" ] && kill "$SR_PID" 2>/dev/null && wait "$SR_PID" 2>/dev/null || true
    [ -n "$NODE_PID" ] && kill "$NODE_PID" 2>/dev/null && wait "$NODE_PID" 2>/dev/null || true
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

check() {
    local desc="$1"
    local result="$2"
    local expected="$3"
    if echo "$result" | grep -q "$expected"; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc"
        echo "    expected to match: $expected"
        echo "    got: $(echo "$result" | head -c 300)"
        FAIL=$((FAIL + 1))
    fi
}

check_eq() {
    local desc="$1"
    local actual="$2"
    local expected="$3"
    if [ "$actual" = "$expected" ]; then
        echo "  PASS: $desc"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (expected=$expected, got=$actual)"
        FAIL=$((FAIL + 1))
    fi
}

check_gt() {
    local desc="$1"
    local actual="$2"
    local threshold="$3"
    if [ "$actual" -gt "$threshold" ] 2>/dev/null; then
        echo "  PASS: $desc ($actual > $threshold)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: $desc (expected > $threshold, got $actual)"
        FAIL=$((FAIL + 1))
    fi
}

skip() {
    local desc="$1"
    local reason="$2"
    echo "  SKIP: $desc ($reason)"
    SKIP=$((SKIP + 1))
}

http_get() {
    curl -sf --max-time 5 "http://localhost:$1$2" 2>/dev/null || echo "CURL_ERROR"
}

http_post() {
    curl -sf --max-time 5 -X POST -H "Content-Type: application/json" \
        -d "$3" "http://localhost:$1$2" 2>/dev/null || echo "CURL_ERROR"
}

# Extract JSON field using python3
json_field() {
    python3 -c "import sys,json; d=json.load(sys.stdin); print($1)" 2>/dev/null <<< "$2"
}

wait_for_http() {
    local port=$1
    local name=$2
    local tries=0
    while ! curl -sf --max-time 1 "http://localhost:$port/wallet/getnodeinfo" > /dev/null 2>&1; do
        tries=$((tries + 1))
        if [ $tries -ge 30 ]; then
            echo "ERROR: $name HTTP not ready after 30s"
            return 1
        fi
        sleep 1
    done
    echo "$name HTTP ready (port $port)"
}

wait_for_block() {
    local port=$1
    local target=$2
    local name=$3
    local tries=0
    while true; do
        local blk
        blk=$(http_get "$port" "/wallet/getnowblock")
        local num
        num=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$blk" || echo "0")
        if [ "$num" -ge "$target" ] 2>/dev/null; then
            return 0
        fi
        tries=$((tries + 1))
        if [ $tries -ge 40 ]; then
            echo "ERROR: $name did not reach block $target (at $num)"
            return 1
        fi
        sleep 1
    done
}

echo "Tmp dir: $TMPDIR"
echo "SR:   http=$SR_HTTP  p2p=$SR_P2P"
echo "Node: http=$NODE_HTTP p2p=$NODE_P2P"
echo ""

# ─────────────────────────────────────────────────────────────────
# SECTION 1: Start SR node (dev mode = single-witness chain)
# ─────────────────────────────────────────────────────────────────
echo "=== Starting SR node (dev mode) ==="
"$GTRON" --dev --witness \
    --witness.key "$WITNESS_KEY" \
    --datadir "$SR_DIR" \
    --p2p.port "$SR_P2P" \
    --http.port "$SR_HTTP" \
    > "$TMPDIR/sr.log" 2>&1 &
SR_PID=$!
echo "SR PID=$SR_PID"

wait_for_http $SR_HTTP "SR"
echo "Waiting for blocks..."
wait_for_block $SR_HTTP 2 "SR"

# ─────────────────────────────────────────────────────────────────
# SECTION 2: Consensus & Block Production
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 1: Consensus & Block Production ==="

RESULT=$(http_get $SR_HTTP "/wallet/getnodeinfo")
check "node info returns version" "$RESULT" '"version"'
check "node info returns currentBlock" "$RESULT" '"currentBlock"'

RESULT=$(http_get $SR_HTTP "/wallet/getnowblock")
check "getnowblock returns block_header" "$RESULT" '"block_header"'
check "getnowblock returns blockID" "$RESULT" '"blockID"'

BLOCK_NUM=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$RESULT" || echo "0")
check_gt "blocks are being produced" "$BLOCK_NUM" 0
BLOCK_ID=$(json_field "d.get('blockID','')" "$RESULT" || echo "")

# Get block by number
RESULT=$(http_post $SR_HTTP "/wallet/getblockbynum" '{"num": 1}')
check "getblockbynum(1) returns block_header" "$RESULT" '"block_header"'
BLOCK1_ID=$(json_field "d.get('blockID','')" "$RESULT" || echo "")

# Get block by ID (use the blockID from block 1)
if [ -n "$BLOCK1_ID" ] && [ "$BLOCK1_ID" != "" ]; then
    RESULT=$(http_post $SR_HTTP "/wallet/getblockbyid" "{\"value\": \"$BLOCK1_ID\"}")
    check "getblockbyid returns block" "$RESULT" '"block_header"'
else
    skip "getblockbyid" "no block ID available"
fi

# Get block range
RESULT=$(http_post $SR_HTTP "/wallet/getblockbylimitnext" '{"startNum": 1, "endNum": 3}')
check "getblockbylimitnext returns block array" "$RESULT" '"block"'
RANGE_COUNT=$(json_field "len(d.get('block',[]))" "$RESULT" || echo "0")
check_eq "block range returns 2 blocks" "$RANGE_COUNT" "2"

# ─────────────────────────────────────────────────────────────────
# SECTION 3: Witness Address & Account Query
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 2: Account & Witness Queries ==="

WITNESS_ADDR=$(http_get $SR_HTTP "/wallet/getnowblock" | python3 -c "
import sys, json
d = json.load(sys.stdin)
print(d.get('block_header',{}).get('raw_data',{}).get('witness_address',''))
" 2>/dev/null || echo "")
echo "  Witness address: $WITNESS_ADDR"

if [ -n "$WITNESS_ADDR" ] && [ "$WITNESS_ADDR" != "" ]; then
    RESULT=$(http_post $SR_HTTP "/wallet/getaccount" "{\"address\": \"$WITNESS_ADDR\"}")
    check "getaccount returns balance" "$RESULT" '"balance"'
    WITNESS_BALANCE=$(json_field "d.get('balance',0)" "$RESULT" || echo "0")
    check_gt "witness has large balance" "$WITNESS_BALANCE" 1000000
    echo "  Witness balance: $WITNESS_BALANCE sun"
else
    skip "account queries" "could not determine witness address"
fi

# List witnesses
RESULT=$(http_post $SR_HTTP "/wallet/listwitnesses" '{}')
check "listwitnesses returns witnesses array" "$RESULT" '"witnesses"'
W_COUNT=$(json_field "len(d.get('witnesses',[]))" "$RESULT" || echo "0")
check_gt "at least 1 witness listed" "$W_COUNT" 0

# Next maintenance time
RESULT=$(http_post $SR_HTTP "/wallet/getnextmaintenancetime" '{}')
check "getnextmaintenancetime returns num" "$RESULT" '"num"'

# ─────────────────────────────────────────────────────────────────
# SECTION 4: Resource & Chain Parameters
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 3: Resource & Chain Parameters ==="

RESULT=$(http_post $SR_HTTP "/wallet/getaccountresource" "{\"address\": \"$WITNESS_ADDR\"}")
check "getaccountresource returns freeNetLimit" "$RESULT" '"freeNetLimit"'
check "getaccountresource returns TotalNetLimit" "$RESULT" '"TotalNetLimit"'

RESULT=$(http_post $SR_HTTP "/wallet/getchainparameters" '{}')
check "getchainparameters returns chainParameter" "$RESULT" '"chainParameter"'
PARAM_COUNT=$(json_field "len(d.get('chainParameter',[]))" "$RESULT" || echo "0")
check_gt "chain parameters > 0" "$PARAM_COUNT" 0

# Transaction pool
RESULT=$(http_get $SR_HTTP "/wallet/gettransactioncountinpool")
check "tx pool count returns count" "$RESULT" '"count"'

# ─────────────────────────────────────────────────────────────────
# SECTION 5: Transaction Lifecycle (Build → Sign → Broadcast → Confirm)
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 4: Transaction Lifecycle ==="

# Generate a recipient address (deterministic from a known key for reproducibility)
RECIPIENT_KEY="a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a0a1"
# Derive recipient address using python3 + the signing tool's approach
RECIPIENT_ADDR=$(python3 -c "
import hashlib, binascii
# We know the witness address, just use a simple different address
# 41 prefix + 20 bytes
print('41' + 'aa' * 20)
" 2>/dev/null)
echo "  Recipient address: $RECIPIENT_ADDR"

# Step 1: Build unsigned transfer transaction via API
TRANSFER_AMOUNT=1000000
echo "  Building transfer: $TRANSFER_AMOUNT sun to $RECIPIENT_ADDR"
UNSIGNED_TX=$(http_post $SR_HTTP "/wallet/createtransaction" "{
    \"owner_address\": \"$WITNESS_ADDR\",
    \"to_address\": \"$RECIPIENT_ADDR\",
    \"amount\": $TRANSFER_AMOUNT
}")
check "createtransaction returns raw_data" "$UNSIGNED_TX" '"raw_data"'
check "createtransaction returns txID" "$UNSIGNED_TX" '"txID"'

TX_ID=$(json_field "d.get('txID','')" "$UNSIGNED_TX" || echo "")
echo "  Transaction ID: $TX_ID"

# Step 2: Sign the transaction
echo "  Signing transaction..."
SIGNED_TX=$(echo "$UNSIGNED_TX" | "$TXSIGN" "$WITNESS_KEY" 2>&1) || true
echo "  Signed TX (first 200): ${SIGNED_TX:0:200}"
check "txsign produces signed transaction" "$SIGNED_TX" '"signature"'

# Step 3: Broadcast the signed transaction
echo "  Broadcasting..."
BCAST_RESULT=$(http_post $SR_HTTP "/wallet/broadcasttransaction" "$SIGNED_TX")
echo "  Broadcast result: $BCAST_RESULT"
check "broadcast returns result=true" "$BCAST_RESULT" '"result":true'

# Step 4: Wait for the transaction to be included in a block
echo "  Waiting for tx to be mined..."
CURRENT_BLOCK=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$(http_get $SR_HTTP /wallet/getnowblock)" || echo "0")
TARGET_BLOCK=$((CURRENT_BLOCK + 2))
wait_for_block $SR_HTTP $TARGET_BLOCK "SR"

# Step 5: Verify transaction by ID
if [ -n "$TX_ID" ] && [ "$TX_ID" != "" ]; then
    RESULT=$(http_post $SR_HTTP "/wallet/gettransactionbyid" "{\"value\": \"$TX_ID\"}")
    check "gettransactionbyid finds the tx" "$RESULT" '"raw_data"'

    # Step 6: Verify transaction info
    RESULT=$(http_post $SR_HTTP "/wallet/gettransactioninfobyid" "{\"value\": \"$TX_ID\"}")
    check "gettransactioninfobyid returns info" "$RESULT" '"blockNumber"'
    TX_BLOCK=$(json_field "d.get('blockNumber',0)" "$RESULT" || echo "0")
    echo "  Tx included in block: $TX_BLOCK"

    # Step 7: Verify transaction info by block number
    if [ "$TX_BLOCK" -gt 0 ] 2>/dev/null; then
        RESULT=$(http_post $SR_HTTP "/wallet/gettransactioninfobyblocknum" "{\"num\": $TX_BLOCK}")
        check "gettransactioninfobyblocknum returns array" "$RESULT" '"blockNumber"'
    fi
else
    skip "tx queries" "no TX_ID"
fi

# Step 8: Verify recipient balance changed
RESULT=$(http_post $SR_HTTP "/wallet/getaccount" "{\"address\": \"$RECIPIENT_ADDR\"}")
RECV_BALANCE=$(json_field "d.get('balance',0)" "$RESULT" || echo "0")
echo "  Recipient balance: $RECV_BALANCE sun"
check_eq "recipient received correct amount" "$RECV_BALANCE" "$TRANSFER_AMOUNT"

# Step 9: Verify sender balance decreased
RESULT=$(http_post $SR_HTTP "/wallet/getaccount" "{\"address\": \"$WITNESS_ADDR\"}")
NEW_WITNESS_BALANCE=$(json_field "d.get('balance',0)" "$RESULT" || echo "0")
echo "  Witness balance after transfer: $NEW_WITNESS_BALANCE sun"
if [ "$NEW_WITNESS_BALANCE" -lt "$WITNESS_BALANCE" ] 2>/dev/null; then
    echo "  PASS: witness balance decreased after transfer"
    PASS=$((PASS + 1))
else
    echo "  FAIL: witness balance did not decrease (before=$WITNESS_BALANCE, after=$NEW_WITNESS_BALANCE)"
    FAIL=$((FAIL + 1))
fi

# ─────────────────────────────────────────────────────────────────
# SECTION 6: Smart Contract Deployment
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 5: Smart Contract Deployment ==="

# Simple contract: PUSH1 0x42 PUSH1 0 MSTORE PUSH1 32 PUSH1 0 RETURN
# Runtime bytecode (returns 0x42 padded to 32 bytes)
RUNTIME="60426000526020600060f3"  # deliberately wrong - use proper hex
# Correct runtime: PUSH1 0x42, PUSH1 0, MSTORE, PUSH1 0x20, PUSH1 0, RETURN
RUNTIME="604260005260206000f3"
RUNTIME_LEN=10  # 10 bytes

# Init code: PUSH1 <len> DUP1 PUSH1 <offset> PUSH1 0 CODECOPY PUSH1 <len> PUSH1 0 RETURN
# offset = length of init code = 13 bytes
INIT="600a80600d6000396006600af3"  # wrong, let's compute properly

# Init code that copies runtime to memory and returns it:
# PUSH1 runtimeLen (0x0a=10)
# DUP1
# PUSH1 initCodeLen (will be 0x0d=13)
# PUSH1 0x00
# CODECOPY
# PUSH1 runtimeLen (0x0a=10)
# PUSH1 0x00
# RETURN
INIT_CODE="600a80600d600039600a6000f3"
BYTECODE="${INIT_CODE}${RUNTIME}"

echo "  Deploying contract (bytecode=${#BYTECODE} hex chars)..."
DEPLOY_TX=$(http_post $SR_HTTP "/wallet/deploycontract" "{
    \"owner_address\": \"$WITNESS_ADDR\",
    \"bytecode\": \"$BYTECODE\",
    \"fee_limit\": 10000000,
    \"name\": \"TestStore42\",
    \"consume_user_resource_percent\": 100
}")
check "deploycontract returns raw_data" "$DEPLOY_TX" '"raw_data"'

DEPLOY_TX_ID=$(json_field "d.get('txID','')" "$DEPLOY_TX" || echo "")
echo "  Deploy TX ID: $DEPLOY_TX_ID"

# Sign and broadcast the deploy transaction
SIGNED_DEPLOY=$(echo "$DEPLOY_TX" | "$TXSIGN" "$WITNESS_KEY" 2>&1) || true
check "deploy tx signed" "$SIGNED_DEPLOY" '"signature"'

BCAST_RESULT=$(http_post $SR_HTTP "/wallet/broadcasttransaction" "$SIGNED_DEPLOY")
check "deploy broadcast succeeds" "$BCAST_RESULT" '"result":true'

# Wait for deployment to be mined
echo "  Waiting for deploy tx to be mined..."
CURRENT_BLOCK=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$(http_get $SR_HTTP /wallet/getnowblock)" || echo "0")
wait_for_block $SR_HTTP $((CURRENT_BLOCK + 2)) "SR"

# Check deploy transaction info for contract address
if [ -n "$DEPLOY_TX_ID" ] && [ "$DEPLOY_TX_ID" != "" ]; then
    RESULT=$(http_post $SR_HTTP "/wallet/gettransactioninfobyid" "{\"value\": \"$DEPLOY_TX_ID\"}")
    check "deploy tx info has blockNumber" "$RESULT" '"blockNumber"'
    CONTRACT_ADDR=$(json_field "d.get('contract_address','')" "$RESULT" || echo "")
    echo "  Contract address: $CONTRACT_ADDR"

    if [ -n "$CONTRACT_ADDR" ] && [ "$CONTRACT_ADDR" != "" ]; then
        RESULT=$(http_post $SR_HTTP "/wallet/getcontract" "{\"value\": \"$CONTRACT_ADDR\"}")
        echo "  Contract query: $(echo "$RESULT" | head -c 200)"
        check "getcontract returns bytecode" "$RESULT" '"bytecode"'
    else
        skip "getcontract" "no contract address in tx info"
    fi
else
    skip "deploy verification" "no deploy TX_ID"
fi

# ─────────────────────────────────────────────────────────────────
# SECTION 7: Start Regular Node & P2P Sync
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 6: P2P Sync & Cross-Node Consistency ==="

echo "Starting regular node (relay/sync only — no --witness flag)..."
"$GTRON" --dev \
    --witness.key "$WITNESS_KEY" \
    --datadir "$NODE_DIR" \
    --p2p.port "$NODE_P2P" \
    --http.port "$NODE_HTTP" \
    --seednode "localhost:$SR_P2P" \
    > "$TMPDIR/node.log" 2>&1 &
NODE_PID=$!
echo "Node PID=$NODE_PID"

wait_for_http $NODE_HTTP "Node"

# Wait for sync
echo "Waiting for node to sync..."
sleep 10

SR_BLOCK=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$(http_get $SR_HTTP /wallet/getnowblock)" || echo "0")
NODE_BLOCK=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$(http_get $NODE_HTTP /wallet/getnowblock)" || echo "0")
echo "  SR block:   $SR_BLOCK"
echo "  Node block: $NODE_BLOCK"

check_gt "node synced blocks via P2P" "$NODE_BLOCK" 0

# Check block heights are close
if [ "$SR_BLOCK" -gt 0 ] && [ "$NODE_BLOCK" -gt 0 ] 2>/dev/null; then
    DIFF=$((SR_BLOCK - NODE_BLOCK))
    if [ "$DIFF" -lt 0 ]; then DIFF=$((-DIFF)); fi
    if [ "$DIFF" -le 3 ]; then
        echo "  PASS: block heights within 3 (diff=$DIFF)"
        PASS=$((PASS + 1))
    else
        echo "  FAIL: block heights too far apart (diff=$DIFF)"
        FAIL=$((FAIL + 1))
    fi
fi

# Cross-node account consistency
echo ""
echo "--- Cross-node account consistency ---"

# Recipient account on node
RESULT=$(http_post $NODE_HTTP "/wallet/getaccount" "{\"address\": \"$RECIPIENT_ADDR\"}")
NODE_RECV_BALANCE=$(json_field "d.get('balance',0)" "$RESULT" || echo "0")
echo "  Recipient balance on node: $NODE_RECV_BALANCE"
check_eq "recipient balance consistent across nodes" "$NODE_RECV_BALANCE" "$TRANSFER_AMOUNT"

# Witness account on node
RESULT=$(http_post $NODE_HTTP "/wallet/getaccount" "{\"address\": \"$WITNESS_ADDR\"}")
NODE_WITNESS_BALANCE=$(json_field "d.get('balance',0)" "$RESULT" || echo "0")
echo "  Witness balance on node: $NODE_WITNESS_BALANCE"
if [ "$NODE_WITNESS_BALANCE" -gt 0 ] 2>/dev/null; then
    echo "  PASS: witness has balance on synced node"
    PASS=$((PASS + 1))
else
    echo "  FAIL: witness has no balance on synced node"
    FAIL=$((FAIL + 1))
fi

# Cross-node: same block at same height
echo ""
echo "--- Cross-node block consistency ---"
SR_BLOCK1=$(http_post $SR_HTTP "/wallet/getblockbynum" '{"num": 1}')
NODE_BLOCK1=$(http_post $NODE_HTTP "/wallet/getblockbynum" '{"num": 1}')
SR_B1_ID=$(json_field "d.get('blockID','')" "$SR_BLOCK1" || echo "")
NODE_B1_ID=$(json_field "d.get('blockID','')" "$NODE_BLOCK1" || echo "")
check_eq "block 1 ID matches across nodes" "$SR_B1_ID" "$NODE_B1_ID"

# Cross-node: transaction query on synced node
if [ -n "$TX_ID" ] && [ "$TX_ID" != "" ]; then
    RESULT=$(http_post $NODE_HTTP "/wallet/gettransactionbyid" "{\"value\": \"$TX_ID\"}")
    check "node can find transfer tx by ID" "$RESULT" '"raw_data"'

    RESULT=$(http_post $NODE_HTTP "/wallet/gettransactioninfobyid" "{\"value\": \"$TX_ID\"}")
    check "node has transfer tx info" "$RESULT" '"blockNumber"'
fi

# Cross-node: contract query
if [ -n "$CONTRACT_ADDR" ] && [ "$CONTRACT_ADDR" != "" ]; then
    RESULT=$(http_post $NODE_HTTP "/wallet/getcontract" "{\"value\": \"$CONTRACT_ADDR\"}")
    check "node getcontract returns bytecode" "$RESULT" '"bytecode"'
fi

# Cross-node: chain parameters should match
SR_PARAMS=$(http_post $SR_HTTP "/wallet/getchainparameters" '{}')
NODE_PARAMS=$(http_post $NODE_HTTP "/wallet/getchainparameters" '{}')
SR_PARAM_COUNT=$(json_field "len(d.get('chainParameter',[]))" "$SR_PARAMS" || echo "0")
NODE_PARAM_COUNT=$(json_field "len(d.get('chainParameter',[]))" "$NODE_PARAMS" || echo "0")
check_eq "chain parameter count matches across nodes" "$SR_PARAM_COUNT" "$NODE_PARAM_COUNT"

# Cross-node: witness list should match
SR_WITNESSES=$(http_post $SR_HTTP "/wallet/listwitnesses" '{}')
NODE_WITNESSES=$(http_post $NODE_HTTP "/wallet/listwitnesses" '{}')
SR_W_COUNT=$(json_field "len(d.get('witnesses',[]))" "$SR_WITNESSES" || echo "0")
NODE_W_COUNT=$(json_field "len(d.get('witnesses',[]))" "$NODE_WITNESSES" || echo "0")
check_eq "witness count matches across nodes" "$SR_W_COUNT" "$NODE_W_COUNT"

# ─────────────────────────────────────────────────────────────────
# SECTION 8: Second Transfer (from node perspective)
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 7: Transaction via Second Node ==="

# The regular node has no witness key and cannot produce blocks.
# Broadcasting a TX to it is the definitive test of P2P TX propagation:
# the TX must travel from NODE's pool to SR's pool via P2P, then be mined by SR.
RECIPIENT2_ADDR="41bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
TRANSFER_AMOUNT2=500000

UNSIGNED_TX2=$(http_post $NODE_HTTP "/wallet/createtransaction" "{
    \"owner_address\": \"$WITNESS_ADDR\",
    \"to_address\": \"$RECIPIENT2_ADDR\",
    \"amount\": $TRANSFER_AMOUNT2
}")
check "node: createtransaction works" "$UNSIGNED_TX2" '"raw_data"'

TX_ID2=$(json_field "d.get('txID','')" "$UNSIGNED_TX2" || echo "")
SIGNED_TX2=$(echo "$UNSIGNED_TX2" | "$TXSIGN" "$WITNESS_KEY" 2>&1) || true

# Broadcast ONLY to the non-producing node — SR must receive it via P2P to mine it
BCAST2=$(http_post $NODE_HTTP "/wallet/broadcasttransaction" "$SIGNED_TX2")
check "broadcast to relay node accepted" "$BCAST2" '"result":true'

echo "  Waiting for SR to mine the propagated tx..."
CURRENT_BLOCK=$(json_field "d.get('block_header',{}).get('raw_data',{}).get('number',0)" "$(http_get $SR_HTTP /wallet/getnowblock)" || echo "0")
wait_for_block $SR_HTTP $((CURRENT_BLOCK + 3)) "SR"
sleep 5  # let sync propagate back to node

# Verify SR mined TX2 (confirms P2P pool propagation worked)
RESULT2=$(http_post $SR_HTTP "/wallet/gettransactioninfobyid" "{\"value\": \"$TX_ID2\"}")
TX2_BLOCK=$(json_field "d.get('blockNumber',0)" "$RESULT2" || echo "0")
echo "  TX2 mined in block: $TX2_BLOCK"
if [ "$TX2_BLOCK" -gt 0 ] 2>/dev/null; then
    echo "  PASS: SR mined the tx received via P2P from relay node"
    PASS=$((PASS + 1))
else
    echo "  FAIL: SR did not mine tx (P2P propagation may have failed)"
    FAIL=$((FAIL + 1))
fi

# Verify balance on both nodes
RESULT=$(http_post $SR_HTTP "/wallet/getaccount" "{\"address\": \"$RECIPIENT2_ADDR\"}")
SR_R2_BAL=$(json_field "d.get('balance',0)" "$RESULT" || echo "0")
echo "  Recipient2 balance on SR: $SR_R2_BAL"
check_eq "recipient2 balance on SR" "$SR_R2_BAL" "$TRANSFER_AMOUNT2"

RESULT=$(http_post $NODE_HTTP "/wallet/getaccount" "{\"address\": \"$RECIPIENT2_ADDR\"}")
NODE_R2_BAL=$(json_field "d.get('balance',0)" "$RESULT" || echo "0")
echo "  Recipient2 balance on node: $NODE_R2_BAL"
check_eq "recipient2 balance synced to node" "$NODE_R2_BAL" "$TRANSFER_AMOUNT2"

# ─────────────────────────────────────────────────────────────────
# SECTION 9: Phase 10 Query APIs
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 9: Phase 10 Query APIs ==="

# 9.1 getdelegatedresourcev2 — no delegation in dev chain → empty list
RESULT=$(http_post $SR_HTTP "/wallet/getdelegatedresourcev2" \
    "{\"fromAddress\": \"$WITNESS_ADDR\", \"toAddress\": \"$WITNESS_ADDR\"}")
check "getdelegatedresourcev2 returns delegatedResource key" "$RESULT" '"delegatedResource"'

# 9.2 getdelegatedresourceaccountindexv2 — no delegation → empty toAddresses
RESULT=$(http_post $SR_HTTP "/wallet/getdelegatedresourceaccountindexv2" \
    "{\"value\": \"$WITNESS_ADDR\"}")
check "getdelegatedresourceaccountindexv2 returns toAddresses key" "$RESULT" '"toAddresses"'

# 9.3 candelegateresource — witness has no frozen → maxSize=0
RESULT=$(http_post $SR_HTTP "/wallet/candelegateresource" \
    "{\"owner_address\": \"$WITNESS_ADDR\", \"balance\": 0, \"type\": 0}")
check "candelegateresource returns maxSize key" "$RESULT" '"maxSize"'

# 9.4 getcanwithdrawunfreezeamount — no pending unfreezes → amount=0
RESULT=$(http_post $SR_HTTP "/wallet/getcanwithdrawunfreezeamount" \
    "{\"owner_address\": \"$WITNESS_ADDR\", \"timestamp\": 9999999999999}")
check "getcanwithdrawunfreezeamount returns amount key" "$RESULT" '"amount"'

# 9.5 getavailableunfreezecount — no pending unfreezes → count=32
RESULT=$(http_post $SR_HTTP "/wallet/getavailableunfreezecount" \
    "{\"owner_address\": \"$WITNESS_ADDR\"}")
check "getavailableunfreezecount returns count key" "$RESULT" '"count"'

# 9.6 getreward — witness earns allowance after block production
RESULT=$(http_post $SR_HTTP "/wallet/getreward" \
    "{\"address\": \"$WITNESS_ADDR\"}")
check "getreward returns reward key" "$RESULT" '"reward"'

# 9.7 gettransactionfrompending — tx not in pool → Error field
RESULT=$(http_post $SR_HTTP "/wallet/gettransactionfrompending" \
    '{"value":"0000000000000000000000000000000000000000000000000000000000000000"}')
check "gettransactionfrompending returns Error for unknown txid" "$RESULT" '"Error"'

# 9.8 gettransactionlistfrompending — pool should be empty between blocks
RESULT=$(http_post $SR_HTTP "/wallet/gettransactionlistfrompending" '{}')
check "gettransactionlistfrompending returns transaction key" "$RESULT" '"transaction"'

# 9.9 listnodes — SR should see at least 0 nodes (relay node connects to SR)
RESULT=$(http_get $SR_HTTP "/wallet/listnodes")
check "listnodes returns nodes key" "$RESULT" '"nodes"'

# ─────────────────────────────────────────────────────────────────
# Summary: print last few lines of logs
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== SR Log (last 10 lines) ==="
tail -10 "$TMPDIR/sr.log" | sed 's/^/  /'
echo ""
echo "=== Node Log (last 10 lines) ==="
tail -10 "$TMPDIR/node.log" | sed 's/^/  /'
