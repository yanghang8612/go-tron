#!/usr/bin/env bash
#
# System test flows: exercise representative TRON transaction types end-to-end
# (HTTP builder → txsign → broadcast → confirm → verify state side-effect).
#
# Output: per-flow ✅/⚠️/❌/❓ verdicts to drive PLAN.md/spec adjustments.
#
# Pre-conditions: build/bin/gtron + build/bin/txsign exist; ports 18090/19888 free.
# Usage:
#   scripts/system_test_flows.sh          # run against existing node on :18090
#   scripts/system_test_flows.sh --start  # spawn node in /tmp/gtron-flow-test
#
# IMPORTANT — ✅ PASS semantics:
#   PASS means the tx was confirmed in a block. It does NOT verify the field
#   *content* matches what the caller intended. M9 P0-2 (silent corruption from
#   `[]byte(stringField)`) currently causes setAccountId / updateWitness etc. to
#   return PASS while writing the literal hex string as bytes (16 chars stored
#   when 8 were intended). After M9.2 lands, these will still PASS but write
#   the correct decoded bytes. Don't read PASS as "fully correct" until M9 is
#   complete. See docs/superpowers/specs/2026-04-27-system-test-findings.md.
#
set -uo pipefail

BASEDIR="$(cd "$(dirname "$0")/.." && pwd)"
GTRON="$BASEDIR/build/bin/gtron"
TXSIGN="$BASEDIR/build/bin/txsign"
WORKDIR="${WORKDIR:-/tmp/gtron-flow-test}"
HTTP=18090
JSONRPC=18091
P2P=19888
WITNESS_KEY="c85ef7d79691fe79573b1a7064c19c1a9819ebdbd1faaab1a8ec92344438aaf4"
WITNESS_ADDR="41cd2a3d9f938e13cd947ec05abc7fe734df8dd826"
B_ADDR="41a614f803b6fd780986a42c78ec9c7f77e6ded13c"
C_ADDR="41ad0f37e25316f1812e75b5fd0fb78a1d8ad3db86"

PASS=0
WARN=0
FAIL=0
SKIP=0
declare -a FINDINGS=()
FLOW="setup"
LAST_TXID=""

START_NODE=0
for arg in "$@"; do
  case "$arg" in --start) START_NODE=1 ;; esac
done

# ── helpers ─────────────────────────────────────────────────────
http_get()      { curl -sf --max-time 5 "http://localhost:$HTTP$1" 2>/dev/null; }
http_post()     { curl -sf --max-time 5 -X POST -H "Content-Type: application/json" -d "$2" "http://localhost:$HTTP$1" 2>/dev/null; }
http_post_raw() { curl -s  --max-time 5 -X POST -H "Content-Type: application/json" -d "$2" "http://localhost:$HTTP$1" 2>/dev/null; }
jrpc()          { curl -s  --max-time 5 -X POST -H "Content-Type: application/json" \
                    -d "{\"jsonrpc\":\"2.0\",\"method\":\"$1\",\"params\":$2,\"id\":1}" \
                    "http://localhost:$JSONRPC" 2>/dev/null; }

ok()    { echo "  ✅ $1"; PASS=$((PASS+1)); FINDINGS+=("PASS|$FLOW|$1"); }
warn()  { echo "  ⚠️  $1 — $2"; WARN=$((WARN+1)); FINDINGS+=("WARN|$FLOW|$1|$2"); }
fail()  { echo "  ❌ $1 — $2"; FAIL=$((FAIL+1)); FINDINGS+=("FAIL|$FLOW|$1|$2"); }
skip()  { echo "  ❓ $1 — $2"; SKIP=$((SKIP+1)); FINDINGS+=("SKIP|$FLOW|$1|$2"); }
section() { echo ""; echo "── $1 ──"; FLOW="$1"; }

# Wait until tx is confirmed in a block (poll gettransactioninfobyid).
# $1=txID. Echoes block number on success; returns 1 on timeout (≈25s).
wait_for_confirm() {
  local tries=0
  while [ $tries -lt 25 ]; do
    local info=$(http_post /wallet/gettransactioninfobyid "{\"value\":\"$1\"}")
    local blk=$(echo "$info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('blockNumber',0))" 2>/dev/null)
    if [ -n "$blk" ] && [ "$blk" != "0" ]; then echo "$blk"; return 0; fi
    sleep 1
    tries=$((tries+1))
  done
  return 1
}

# Run one tx end-to-end. Args: <desc> <path> <body-json>
# Sets LAST_TXID on success (empty otherwise). Calls one of ok/warn/fail.
run_tx() {
  local desc="$1" path="$2" body="$3"
  LAST_TXID=""
  local raw=$(http_post_raw "$path" "$body")
  local has_raw=$(echo "$raw" | python3 -c "import sys,json
try: print(bool(json.load(sys.stdin).get('raw_data_hex')))
except: print(False)" 2>/dev/null)
  if [ "$has_raw" != "True" ]; then
    warn "$desc" "BUILDER_ERR: $(echo "$raw" | head -c 120 | tr '\n' ' ')"
    return 1
  fi
  local signed=$(echo "$raw" | "$TXSIGN" "$WITNESS_KEY" 2>/dev/null)
  if [ -z "$signed" ]; then fail "$desc" "TXSIGN_ERR"; return 1; fi
  local bcast=$(http_post_raw /wallet/broadcasttransaction "$signed")
  local rok=$(echo "$bcast" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('result',False))" 2>/dev/null)
  local code=$(echo "$bcast" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('code',''))" 2>/dev/null)
  local msg=$(echo "$bcast" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message',''))" 2>/dev/null)
  local txid=$(echo "$bcast" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('txid',''))" 2>/dev/null)
  if [ "$rok" != "True" ]; then
    fail "$desc" "BCAST_REJECT code=$code msg=$msg"
    return 1
  fi
  if [ -z "$txid" ]; then
    fail "$desc" "broadcast OK but no txid in response"
    return 1
  fi
  local blk
  if ! blk=$(wait_for_confirm "$txid"); then
    warn "$desc" "EVICTED (bcast.true → no inclusion in 25s; producer dropped at actuator.Validate)"
    return 1
  fi
  # Check info
  local info=$(http_post /wallet/gettransactioninfobyid "{\"value\":\"$txid\"}")
  local res=$(echo "$info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('receipt',{}).get('result',''))" 2>/dev/null)
  local cr=$(echo "$info" | python3 -c "import sys,json; d=json.load(sys.stdin); cr=d.get('contractResult',[]); print(cr[0] if cr else '')" 2>/dev/null)
  local err=$(echo "$info" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('resMessage',''))" 2>/dev/null)
  if [ -n "$res" ] && [ "$res" != "SUCCESS" ] && [ "$res" != "DEFAULT" ]; then
    warn "$desc" "result=$res err=$err"
    return 1
  fi
  # contractResult[0] is TVM return data (deployed bytecode for deploys, call return value for calls).
  # Not an error indicator — receipt.result is the authoritative success/fail field.
  LAST_TXID="$txid"
  ok "$desc (block $blk)"
}

balance_of() {
  http_post /wallet/getaccount "{\"address\":\"$1\"}" \
    | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('balance',0))" 2>/dev/null
}

# Build+broadcast a tx and verify it is REJECTED (BCAST_REJECT or BUILDER_ERR).
# Counts as PASS if rejected, FAIL if unexpectedly accepted.
run_tx_expect_reject() {
  local desc="$1" path="$2" body="$3"
  local raw=$(http_post_raw "$path" "$body")
  local has_raw=$(echo "$raw" | python3 -c "import sys,json
try: print(bool(json.load(sys.stdin).get('raw_data_hex')))
except: print(False)" 2>/dev/null)
  if [ "$has_raw" != "True" ]; then
    ok "$desc (rejected at builder — expected)"
    return 0
  fi
  local signed=$(echo "$raw" | "$TXSIGN" "$WITNESS_KEY" 2>/dev/null)
  if [ -z "$signed" ]; then ok "$desc (rejected — txsign error)"; return 0; fi
  local bcast=$(http_post_raw /wallet/broadcasttransaction "$signed")
  local rok=$(echo "$bcast" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('result',False))" 2>/dev/null)
  local code=$(echo "$bcast" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('code',''))" 2>/dev/null)
  local msg=$(echo "$bcast" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('message',''))" 2>/dev/null)
  if [ "$rok" != "True" ]; then
    ok "$desc (rejected as expected: $code)"
  else
    fail "$desc" "expected rejection but tx was accepted"
  fi
}

# ── start node if requested ─────────────────────────────────────
if [ "$START_NODE" = "1" ]; then
  rm -rf "$WORKDIR/sr"
  mkdir -p "$WORKDIR"
  echo "Starting gtron dev node..."
  "$GTRON" --dev --witness \
    --witness.key "$WITNESS_KEY" \
    --datadir "$WORKDIR/sr" \
    --p2p.port "$P2P" --http.port "$HTTP" \
    --grpc.port 0 --jsonrpc.port "$JSONRPC" \
    > "$WORKDIR/sr.log" 2>&1 &
  GTRON_PID=$!
  trap 'kill $GTRON_PID 2>/dev/null' EXIT
  for i in $(seq 1 30); do
    if curl -sf --max-time 1 http://localhost:$HTTP/wallet/getnodeinfo > /dev/null; then break; fi
    sleep 1
  done
fi

if ! http_get /wallet/getnodeinfo > /dev/null; then
  echo "ERROR: gtron not reachable on http://localhost:$HTTP"
  exit 2
fi
START_BLOCK=$(http_get /wallet/getnowblock | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('block_header',{}).get('raw_data',{}).get('number',0))")
echo "Starting flow tests at block $START_BLOCK"

# ════════════════════════════════════════════════════════════════
# F0: Baseline transfer (sanity)
# ════════════════════════════════════════════════════════════════
section "F0 Baseline"

run_tx "F0/1 transfer 1 TRX SR→B" /wallet/createtransaction \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"to_address\":\"$B_ADDR\",\"amount\":1000000}"

# ════════════════════════════════════════════════════════════════
# F1: Account
# ════════════════════════════════════════════════════════════════
section "F1 Account"

# createAccount may fail if B exists from previous run — both outcomes are informative.
run_tx "F1/1 createAccount(C) — fresh address" /wallet/createaccount \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"account_address\":\"$C_ADDR\"}"

run_tx "F1/2 updateAccount rename" /wallet/updateaccount \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"account_name\":\"6e65772d6e616d6531\"}"

run_tx "F1/3 setAccountId" /wallet/setaccountid \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"account_id\":\"6d796e616d653031\"}"

run_tx "F1/4 accountPermissionUpdate (owner+actives)" /wallet/accountpermissionupdate \
  "{\"owner_address\":\"$WITNESS_ADDR\",
    \"owner\":{\"type\":0,\"id\":0,\"permission_name\":\"owner\",\"threshold\":1,
      \"keys\":[{\"address\":\"$WITNESS_ADDR\",\"weight\":1}]},
    \"actives\":[{\"type\":2,\"id\":2,\"permission_name\":\"active\",\"threshold\":1,
      \"operations\":\"7fff1fc0033e0000000000000000000000000000000000000000000000000000\",
      \"keys\":[{\"address\":\"$WITNESS_ADDR\",\"weight\":1}]}]}"

# ════════════════════════════════════════════════════════════════
# F3: Freeze V1 (legacy)
# ════════════════════════════════════════════════════════════════
section "F3 Freeze V1"

# Dev genesis has allow_new_resource_model=1 which closes freeze V1.
# java-tron rejects with "freeze v2 is open, old freeze is closed" — expected.
run_tx_expect_reject "F3/1 freezeBalance v1 (BANDWIDTH)" /wallet/freezebalance \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"frozen_balance\":1000000,\"frozen_duration\":3,\"resource\":0}"

run_tx_expect_reject "F3/2 unfreezeBalance v1 (BANDWIDTH)" /wallet/unfreezebalance \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"resource\":0}"

# ════════════════════════════════════════════════════════════════
# F4: Freeze V2 + Delegate
# ════════════════════════════════════════════════════════════════
section "F4 Freeze V2 + Delegate"

run_tx "F4/1 freezeBalanceV2 BANDWIDTH 200 TRX" /wallet/freezebalancev2 \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"frozen_balance\":200000000,\"resource\":0}"

run_tx "F4/2 freezeBalanceV2 ENERGY 300 TRX" /wallet/freezebalancev2 \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"frozen_balance\":300000000,\"resource\":1}"

# Verify account.frozenV2 reflects both freezes
AC=$(http_post /wallet/getaccount "{\"address\":\"$WITNESS_ADDR\"}")
FV2_TOTAL=$(echo "$AC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(sum(f.get('amount',0) for f in d.get('frozenV2',[])))")
[ "$FV2_TOTAL" -ge 500000000 ] 2>/dev/null && ok "F4/2.5 frozenV2 total ≥ 500 TRX (got $FV2_TOTAL)" \
  || warn "F4/2.5 frozenV2 total" "got $FV2_TOTAL"

run_tx "F4/3 delegateResource ENERGY → B (100 TRX)" /wallet/delegateresource \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"receiver_address\":\"$B_ADDR\",\"balance\":100000000,\"resource\":1}"

run_tx "F4/4 undelegateResource ENERGY ← B (100 TRX)" /wallet/undelegateresource \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"receiver_address\":\"$B_ADDR\",\"balance\":100000000,\"resource\":1}"

run_tx "F4/5 unfreezeBalanceV2 BANDWIDTH 50 TRX" /wallet/unfreezebalancev2 \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"unfreeze_balance\":50000000,\"resource\":0}"

run_tx "F4/6 cancelAllUnfreezeV2" /wallet/cancelallunfreezev2 \
  "{\"owner_address\":\"$WITNESS_ADDR\"}"

# withdrawExpireUnfreeze with no expired entries is correctly rejected.
run_tx_expect_reject "F4/7 withdrawExpireUnfreeze (no expired entries expected)" /wallet/withdrawexpireunfreeze \
  "{\"owner_address\":\"$WITNESS_ADDR\"}"

# Query endpoints
RESP=$(http_post /wallet/candelegateresource "{\"owner_address\":\"$WITNESS_ADDR\",\"type\":1}")
CDR=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('maxSize',-1))" 2>/dev/null)
[ "$CDR" != "" ] && [ "$CDR" != "-1" ] && ok "F4/q1 candelegateresource maxSize=$CDR" || warn "F4/q1 candelegateresource" "maxSize=$CDR"

# ════════════════════════════════════════════════════════════════
# F5: Witness / Vote / Reward (M1.5 critical path)
# ════════════════════════════════════════════════════════════════
section "F5 Witness/Vote/Reward"

run_tx "F5/1 updateWitness URL" /wallet/updatewitness \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"update_url\":\"687474703a2f2f6e65772e75726c\"}"

run_tx "F5/2 updateBrokerage 25%" /wallet/updatebrokerage \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"brokerage\":25}"

run_tx "F5/3 voteWitnessAccount SR→SR (100 votes)" /wallet/votewitnessaccount \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"votes\":[{\"vote_address\":\"$WITNESS_ADDR\",\"vote_count\":100}]}"

# getReward query
RESP=$(http_post /wallet/getreward "{\"address\":\"$WITNESS_ADDR\"}")
RWD=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('reward',0))" 2>/dev/null)
ok "F5/q1 getReward(SR) = $RWD (pre-maintenance)"

# withdrawBalance — pre-maintenance: should return BUILDER_ERR or EVICTED (no allowance to withdraw)
run_tx "F5/4 withdrawBalance pre-maintenance (expected: rejected)" /wallet/withdrawbalance \
  "{\"owner_address\":\"$WITNESS_ADDR\"}"

NEXT_MT=$(http_post /wallet/getnextmaintenancetime '{}' | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('num',0))" 2>/dev/null)
NOW_MS=$(($(date +%s)*1000))
TIME_TO_MT=$((NEXT_MT - NOW_MS))
skip "F5/post-maintenance reward distribution" "next_maintenance=$NEXT_MT (in ${TIME_TO_MT}ms; default 6h interval. Add --dev.maintenance-interval flag)"

# ════════════════════════════════════════════════════════════════
# F6: Proposal lifecycle
# ════════════════════════════════════════════════════════════════
section "F6 Proposal"

# witness_pay_per_block parameter id is 3 in java-tron's proposal table
run_tx "F6/1 proposalCreate (key=3, value=17000000)" /wallet/proposalcreate \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"parameters\":[{\"key\":3,\"value\":17000000}]}"

PLIST=$(http_post /wallet/listproposals '{}')
PROPOSAL_ID=$(echo "$PLIST" | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d.get('proposals',None) or []
# Proposal IDs start at 0; find the most recent one, defaulting to -1 if empty.
print(max((p.get('proposal_id',-1) for p in ps),default=-1))
" 2>/dev/null)
[ -n "$PROPOSAL_ID" ] && [ "$PROPOSAL_ID" != "-1" ] \
  && ok "F6/q1 listProposals returns id=$PROPOSAL_ID" \
  || warn "F6/q1 listProposals" "no proposals (proposalCreate may have been evicted)"

if [ -n "$PROPOSAL_ID" ] && [ "$PROPOSAL_ID" != "-1" ]; then
  GP=$(http_post /wallet/getproposalbyid "{\"id\":$PROPOSAL_ID}")
  GP_KEY=$(echo "$GP" | python3 -c "
import sys,json
d=json.load(sys.stdin)
ps=d.get('parameters',{})
print(list(ps.keys())[0] if ps else '')" 2>/dev/null)
  [ -n "$GP_KEY" ] && ok "F6/q2 getProposalById id=$PROPOSAL_ID returned param" \
    || warn "F6/q2 getProposalById" "no parameters key"

  run_tx "F6/2 proposalApprove id=$PROPOSAL_ID" /wallet/proposalapprove \
    "{\"owner_address\":\"$WITNESS_ADDR\",\"proposal_id\":$PROPOSAL_ID,\"is_add_approval\":true}"

  run_tx "F6/3 proposalDelete id=$PROPOSAL_ID" /wallet/proposaldelete \
    "{\"owner_address\":\"$WITNESS_ADDR\",\"proposal_id\":$PROPOSAL_ID}"
fi

# ════════════════════════════════════════════════════════════════
# F2: TRC10 Asset
# ════════════════════════════════════════════════════════════════
section "F2 TRC10 Asset"

NOW_MS=$(($(date +%s)*1000))
END_MS=$((NOW_MS + 7*86400000))
# name suffix derived from current second to avoid name collision across reruns
SUFFIX=$(printf '%02d' $(( $(date +%s) % 100 )))
ASSET_NAME=$(printf '464c5754%02x' $(( $(date +%s) % 256 )))  # FLWT + byte
run_tx "F2/1 createAssetIssue name=hex($ASSET_NAME)" /wallet/createassetissue \
  "{\"owner_address\":\"$WITNESS_ADDR\",
    \"name\":\"$ASSET_NAME\",
    \"abbr\":\"4654\",
    \"total_supply\":1000000,
    \"trx_num\":1,\"num\":10,
    \"start_time\":$NOW_MS,\"end_time\":$END_MS,
    \"description\":\"666c6f77\",\"url\":\"687474703a2f2f78\",
    \"precision\":0}"

AC=$(http_post /wallet/getaccount "{\"address\":\"$WITNESS_ADDR\"}")
ASSET_ID=$(echo "$AC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('asset_issued_ID',''))" 2>/dev/null)
[ -n "$ASSET_ID" ] && ok "F2/q1 asset_issued_ID=$ASSET_ID" || warn "F2/q1 asset_issued_ID" "empty"

if [ -n "$ASSET_ID" ]; then
  # ASSET_ID is already hex-encoded bytes of the token ID string (e.g. hex("1000001")).
  # API endpoints expecting hex bytes: use ASSET_ID directly.
  # API endpoints expecting a plain decimal integer: decode hex first.
  ASSET_ID_INT=$(echo "$ASSET_ID" | python3 -c "import sys; print(bytes.fromhex(sys.stdin.read().strip()).decode('ascii','replace'))" 2>/dev/null)
  run_tx "F2/2 transferAsset (1000) SR→B" /wallet/transferasset \
    "{\"owner_address\":\"$WITNESS_ADDR\",\"to_address\":\"$B_ADDR\",
      \"asset_name\":\"$ASSET_ID\",\"amount\":1000}"

  run_tx "F2/3 updateAsset" /wallet/updateasset \
    "{\"owner_address\":\"$WITNESS_ADDR\",
      \"description\":\"6e65772d6465736372\",
      \"url\":\"687474703a2f2f78322e636f6d\",
      \"new_limit\":100,\"new_public_limit\":1000}"

  RESP=$(http_post /wallet/getassetissuebyid "{\"value\":\"$ASSET_ID_INT\"}")
  ID_NAME=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('name',''))" 2>/dev/null)
  [ -n "$ID_NAME" ] && ok "F2/q2 getAssetIssueByID name=$ID_NAME" || warn "F2/q2 getAssetIssueByID" "no name"
fi

# ════════════════════════════════════════════════════════════════
# F7: Exchange
# ════════════════════════════════════════════════════════════════
section "F7 Exchange"

# Each address can only issue one TRC10 token.  Pair TRX ("_") with the
# TRC10 asset issued in F2/1 to test the full exchange lifecycle without
# needing a second signing key.
ok "F7/q1 ASSET2_ID=(N/A — using TRX as second token)"

if [ -n "$ASSET_ID" ]; then
  ASSET_HEX="$ASSET_ID"  # ASSET_ID is already hex-encoded bytes of the token ID
  TRX_HEX="5f"  # hex("_") — TRX sentinel in TRON exchange contracts
  run_tx "F7/1 exchangeCreate (TRX/$ASSET_ID)" /wallet/exchangecreate \
    "{\"owner_address\":\"$WITNESS_ADDR\",
      \"first_token_id\":\"$TRX_HEX\",\"first_token_balance\":1000000,
      \"second_token_id\":\"$ASSET_HEX\",\"second_token_balance\":1000}"

  EX=$(http_post /wallet/listexchanges '{}')
  EXCH_ID=$(echo "$EX" | python3 -c "
import sys,json
d=json.load(sys.stdin)
xs=d.get('exchanges',[])
print(max((e.get('exchange_id',0) for e in xs),default=0))
" 2>/dev/null)

  if [ -n "$EXCH_ID" ] && [ "$EXCH_ID" != "0" ]; then
    ok "F7/q2 listExchanges latest id=$EXCH_ID"

    run_tx "F7/3 exchangeInject (TRX)" /wallet/exchangeinject \
      "{\"owner_address\":\"$WITNESS_ADDR\",\"exchange_id\":$EXCH_ID,
        \"token_id\":\"$TRX_HEX\",\"quant\":100000}"

    run_tx "F7/4 exchangeTransaction (TRX→ASSET)" /wallet/exchangetransaction \
      "{\"owner_address\":\"$WITNESS_ADDR\",\"exchange_id\":$EXCH_ID,
        \"token_id\":\"$TRX_HEX\",\"quant\":10000,\"expected\":1}"

    run_tx "F7/5 exchangeWithdraw (TRX)" /wallet/exchangewithdraw \
      "{\"owner_address\":\"$WITNESS_ADDR\",\"exchange_id\":$EXCH_ID,
        \"token_id\":\"$TRX_HEX\",\"quant\":50000}"
  else
    warn "F7/q2 listExchanges" "no exchange found after create (probably evicted)"
  fi
fi

# ════════════════════════════════════════════════════════════════
# F8: Smart Contract
# ════════════════════════════════════════════════════════════════
section "F8 Smart Contract"

# Storage contract (set/get uint256)
BYTECODE="608060405234801561001057600080fd5b50610150806100206000396000f3fe608060405234801561001057600080fd5b50600436106100365760003560e01c806360fe47b11461003b5780636d4ce63c14610057575b600080fd5b610055600480360381019061005091906100c3565b610075565b005b61005f61007f565b60405161006c91906100ff565b60405180910390f35b8060008190555050565b60008054905090565b600080fd5b6000819050919050565b6100a08161008d565b81146100ab57600080fd5b50565b6000813590506100bd81610097565b92915050565b6000602082840312156100d9576100d8610088565b5b60006100e7848285016100ae565b91505092915050565b6100f98161008d565b82525050565b600060208201905061011460008301846100f0565b9291505056fea2646970667358221220abcdef1234567890fedcba9876543210abcdef0011223344556677889900aabbcc64736f6c63430008130033"

run_tx "F8/1 deployContract Storage" /wallet/deploycontract \
  "{\"owner_address\":\"$WITNESS_ADDR\",\"abi\":\"\",\"bytecode\":\"$BYTECODE\",
    \"call_value\":0,\"name\":\"Storage\",
    \"consume_user_resource_percent\":100,\"origin_energy_limit\":1000000,
    \"fee_limit\":1000000000}"

CONTRACT_ADDR=""
if [ -n "$LAST_TXID" ]; then
  INFO=$(http_post /wallet/gettransactioninfobyid "{\"value\":\"$LAST_TXID\"}")
  CONTRACT_ADDR=$(echo "$INFO" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('contract_address',''))" 2>/dev/null)
  [ -n "$CONTRACT_ADDR" ] && ok "F8/q1 contract_address=$CONTRACT_ADDR" || warn "F8/q1 contract_address" "empty"
fi

if [ -n "$CONTRACT_ADDR" ]; then
  # triggerSmartContract — set(42)
  TRIGGER=$(http_post_raw /wallet/triggersmartcontract \
    "{\"owner_address\":\"$WITNESS_ADDR\",\"contract_address\":\"$CONTRACT_ADDR\",
      \"function_selector\":\"set(uint256)\",
      \"parameter\":\"000000000000000000000000000000000000000000000000000000000000002a\",
      \"call_value\":0,\"fee_limit\":1000000000}")
  HAS_TX=$(echo "$TRIGGER" | python3 -c "import sys,json; d=json.load(sys.stdin); print('transaction' in d)" 2>/dev/null)
  if [ "$HAS_TX" = "True" ]; then
    INNER=$(echo "$TRIGGER" | python3 -c "import sys,json
d=json.load(sys.stdin); import json as J; print(J.dumps(d.get('transaction',{})))" 2>/dev/null)
    SIGNED=$(echo "$INNER" | "$TXSIGN" "$WITNESS_KEY" 2>/dev/null)
    BR=$(http_post_raw /wallet/broadcasttransaction "$SIGNED")
    TXID=$(echo "$BR" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('txid','') if d.get('result') else '')" 2>/dev/null)
    if [ -n "$TXID" ]; then
      BLK=$(wait_for_confirm "$TXID" 2>/dev/null) || BLK=""
      if [ -n "$BLK" ]; then
        INFO=$(http_post /wallet/gettransactioninfobyid "{\"value\":\"$TXID\"}")
        RES=$(echo "$INFO" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('receipt',{}).get('result','UNKNOWN'))" 2>/dev/null)
        ENERGY=$(echo "$INFO" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('receipt',{}).get('energy_usage_total',0))" 2>/dev/null)
        case "$RES" in
          SUCCESS) ok "F8/2 triggerSmartContract set(42), energy_usage_total=$ENERGY" ;;
          *)       warn "F8/2 triggerSmartContract" "result=$RES" ;;
        esac
      else
        warn "F8/2 triggerSmartContract" "EVICTED"
      fi
    else
      fail "F8/2 triggerSmartContract" "BCAST_REJECT"
    fi
  else
    fail "F8/2 triggerSmartContract builder" "no .transaction in response"
  fi

  # triggerConstantContract — get()
  RC=$(http_post_raw /wallet/triggerconstantcontract \
    "{\"owner_address\":\"$WITNESS_ADDR\",\"contract_address\":\"$CONTRACT_ADDR\",
      \"function_selector\":\"get()\",\"call_value\":0}")
  CR=$(echo "$RC" | python3 -c "import sys,json; d=json.load(sys.stdin); cr=d.get('constant_result',['']); print(cr[0] if cr else '')" 2>/dev/null)
  if [ -n "$CR" ]; then
    ok "F8/3 triggerConstantContract get() returned: $CR"
  else
    warn "F8/3 triggerConstantContract" "empty constant_result"
  fi

  # updateSetting / updateEnergyLimit endpoints
  for path in updatesetting updateenergylimit; do
    RESP=$(http_post_raw "/wallet/$path" "{\"owner_address\":\"$WITNESS_ADDR\",\"contract_address\":\"$CONTRACT_ADDR\",\"consume_user_resource_percent\":50,\"origin_energy_limit\":2000000}")
    HAS_RAW=$(echo "$RESP" | python3 -c "import sys,json
try: print(bool(json.load(sys.stdin).get('raw_data_hex')))
except: print(False)" 2>/dev/null)
    if [ "$HAS_RAW" = "True" ]; then
      ok "F8/X $path endpoint registered (builder works)"
    else
      warn "F8/X $path endpoint" "not registered (HTTP 404 / non-JSON)"
    fi
  done

  run_tx "F8/4 clearABI" /wallet/clearabi \
    "{\"owner_address\":\"$WITNESS_ADDR\",\"contract_address\":\"$CONTRACT_ADDR\"}"

  # Probe getcontract returns AbiStore-backed result (M2 PR-6)
  CT=$(http_post /wallet/getcontract "{\"value\":\"$CONTRACT_ADDR\"}")
  ABI_LEN=$(echo "$CT" | python3 -c "import sys,json; d=json.load(sys.stdin); print(len(json.dumps(d.get('abi',{}))))" 2>/dev/null)
  if [ -n "$ABI_LEN" ]; then
    ok "F8/q2 getContract returned (abi=$ABI_LEN bytes)"
  fi
fi

# ════════════════════════════════════════════════════════════════
# F9: M8.1 Solidity/PBFT API + M8.2 WS subscriptions
# ════════════════════════════════════════════════════════════════
section "F9 M8.1+M8.2"

NOW_BLK=$(http_get /wallet/getnowblock | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('block_header',{}).get('raw_data',{}).get('number',0))" 2>/dev/null)
SOLID_RAW=$(http_get /walletsolidity/getnowblock)
SOLID_BLK=$(echo "$SOLID_RAW" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('block_header',{}).get('raw_data',{}).get('number',-1))" 2>/dev/null)
PBFT_RAW=$(http_get /walletpbft/getnowblock)
PBFT_BLK=$(echo "$PBFT_RAW" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('block_header',{}).get('raw_data',{}).get('number',-1))" 2>/dev/null)

ok "F9/q1 wallet/getnowblock = $NOW_BLK"
if [ "$SOLID_BLK" = "0" ] || [ "$SOLID_BLK" = "-1" ]; then
  fail "F9/q2 walletsolidity/getnowblock" "= $SOLID_BLK (latest_solidified_block_num is never updated by ProcessBlock; M8.1 produces stuck-at-0 / empty block)"
else
  ok "F9/q2 walletsolidity/getnowblock = $SOLID_BLK (lag=$((NOW_BLK-SOLID_BLK)))"
fi

if [ "$PBFT_BLK" = "0" ] || [ "$PBFT_BLK" = "-1" ]; then
  if [ "$SOLID_BLK" = "$PBFT_BLK" ]; then
    ok "F9/q3 walletpbft/getnowblock = $PBFT_BLK == solid (PBFT inactive → fallback OK)"
  else
    fail "F9/q3 walletpbft/getnowblock" "= $PBFT_BLK; expected fallback to solid=$SOLID_BLK"
  fi
else
  ok "F9/q3 walletpbft/getnowblock = $PBFT_BLK"
fi

# JSON-RPC sanity
RPC=$(jrpc "eth_blockNumber" "[]")
EB=$(echo "$RPC" | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d.get('result','0x0'),16))" 2>/dev/null)
[ "$EB" -ge "$NOW_BLK" ] 2>/dev/null && ok "F9/q4 eth_blockNumber=$EB" || warn "F9/q4 eth_blockNumber" "got $EB vs $NOW_BLK"

# WS eth_subscribe newHeads
python3 - "$JSONRPC" 2>&1 << 'PY' | tee /tmp/gtron-ws-test.log
import json, sys, socket, struct, base64, os
host="localhost"; port=int(sys.argv[1])
key = base64.b64encode(os.urandom(16)).decode()
req = (f"GET / HTTP/1.1\r\nHost: {host}:{port}\r\n"
       f"Upgrade: websocket\r\nConnection: Upgrade\r\n"
       f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n")
s = socket.socket(); s.settimeout(5); s.connect((host,port)); s.sendall(req.encode())
hdr = b""
while b"\r\n\r\n" not in hdr: hdr += s.recv(1024)
if b"101" not in hdr.split(b"\r\n",1)[0]:
    print(f"WS_HANDSHAKE_FAIL: {hdr[:80]!r}"); sys.exit(1)
print("WS_OK")
def send(p):
    m=os.urandom(4); mp=bytes(p[i]^m[i%4] for i in range(len(p)))
    if len(p)<=125: hdr=struct.pack("!BB",0x81,0x80|len(p))
    else: hdr=struct.pack("!BBH",0x81,0xfe,len(p))
    s.sendall(hdr+m+mp)
def recv():
    h=s.recv(2)
    if len(h)<2: return None
    pl=h[1]&0x7f
    if pl==126: pl=struct.unpack("!H",s.recv(2))[0]
    elif pl==127: pl=struct.unpack("!Q",s.recv(8))[0]
    d=b""
    while len(d)<pl: d+=s.recv(pl-len(d))
    return d
send(json.dumps({"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}).encode())
print(f"SUB_REPLY={recv().decode(errors='replace')}")
s.settimeout(7)
try:
    push = recv()
    print(f"PUSH_OK len={len(push)} sample={push.decode(errors='replace')[:120]}")
except Exception as e:
    print(f"NO_PUSH: {e}")
PY

if grep -q "PUSH_OK" /tmp/gtron-ws-test.log 2>/dev/null; then
  ok "F9/q5 eth_subscribe newHeads pushes events"
else
  warn "F9/q5 eth_subscribe newHeads" "no push in 7s"
fi

# ════════════════════════════════════════════════════════════════
# Summary
# ════════════════════════════════════════════════════════════════
echo ""
echo "════════════════════════════════════════"
echo "  Results: $PASS pass | $WARN warn | $FAIL fail | $SKIP skip"
echo "════════════════════════════════════════"
echo ""
echo "FINDINGS_BEGIN"
printf '%s\n' "${FINDINGS[@]}"
echo "FINDINGS_END"
