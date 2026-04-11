# Phase 12: TRC10 Asset System — Design Spec

**Date:** 2026-04-11

## Goal

Implement the complete TRC10 native token lifecycle: 6 actuators for issuing, transferring, selling, updating, and unfreezing tokens; asset metadata stored in raw KV; per-account TRC10 balances stored as storage trie slots (contributing to stateRoot); and 5 HTTP query endpoints.

## Context

Phases 1–11 built a working go-tron node with block production, P2P, TVM/EVM, all governance/delegation contract types, smart contract persistence, full TRON HTTP API, and an Ethereum-compatible JSON-RPC API at `:8545`. Phase 12 adds TRC10 — TRON's native token standard predating TRC20.

## Architecture Principles

- **Prefix+key rawdb pattern**: No Store structs. Read*/Write* accessor functions in `core/rawdb/asset.go`. New prefixes for asset metadata, name index, owner index, and issue timestamp.
- **geth-compatible account model**: TRC10 balances live in the account's storage trie as keccak256-keyed slots, contributing to the account's `storageRoot` and hence the block's `stateRoot`.
- **No new external dependencies**: Standard library + already-imported packages only.

## Files

| File | Change |
|---|---|
| `core/rawdb/schema.go` | Add 4 new prefixes: `ast-`, `astn-`, `asto-`, `asti-` |
| `core/rawdb/asset.go` | New: all asset Read/Write/List functions |
| `core/state/slots.go` | New: `trc10BalanceSlot`, `trc10FrozenClaimedSlot` key derivation |
| `core/state/statedb.go` | Add: 6 TRC10 state methods |
| `core/state/dynamic_properties.go` | Add: `next_token_id` default + typed getter/setter |
| `actuator/asset_issue.go` | New: `AssetIssueActuator` (type 6) |
| `actuator/transfer_asset.go` | New: `TransferAssetActuator` (type 2) |
| `actuator/update_asset.go` | New: `UpdateAssetActuator` (type 15) |
| `actuator/participate_asset_issue.go` | New: `ParticipateAssetIssueActuator` (type 9) |
| `actuator/vote_asset.go` | New: `VoteAssetActuator` (type 3, stub) |
| `actuator/unfreeze_asset.go` | New: `UnfreezeAssetActuator` (type 14) |
| `actuator/actuator.go` | Register 6 new types |
| `internal/tronapi/backend.go` | Add 5 asset query methods and response type |
| `internal/tronapi/api.go` | Add 5 route handlers |
| `core/tron_backend.go` | Implement 5 new asset Backend methods |
| `scripts/system_test.sh` | Add Section 11: asset query endpoint checks |

## Storage Design

### New Raw KV Prefixes (core/rawdb/schema.go)

```
ast-  + big_endian_int64(tokenID)          → proto AssetIssueContract bytes  (metadata)
astn- + <name bytes>                        → big_endian_int64(tokenID)        (name → ID)
asto- + <owner 20-byte address>             → big_endian_int64(tokenID)        (owner → ID)
asti- + big_endian_int64(tokenID)          → big_endian_int64(issueTimeMs)    (issue timestamp)
```

Each address can issue at most one TRC10 token. The `asto-` index enables UpdateAsset and UnfreezeAsset (which only carry `owner_address`, no token ID) to find the asset.

### DynamicProperties New Key

Add `"next_token_id": 1_000_001` to `defaultProps`. Token IDs start at 1,000,001 and increment by 1 for each new asset. The `LoadDynamicProperties` loop already handles arbitrary keys in `defaultProps`, so this is the only change needed.

```go
// New typed getter/setter in dynamic_properties.go
func (dp *DynamicProperties) NextTokenID() int64  { return dp.props["next_token_id"] }
func (dp *DynamicProperties) SetNextTokenID(id int64) { dp.Set("next_token_id", id) }
func (dp *DynamicProperties) AssetIssueFee() int64 { return dp.props["asset_issue_fee"] }
```

### Account Storage Trie Slots (core/state/slots.go)

TRC10 balances and frozen-claim state are stored as standard contract storage slots in the account's storage trie. This means they contribute to `storageRoot` and therefore to the block's `stateRoot`.

```go
package state

import (
    "encoding/binary"
    "github.com/ethereum/go-ethereum/crypto"
    tcommon "github.com/tronprotocol/go-tron/common"
)

// trc10BalanceSlot returns the storage slot key for account addr's TRC10 token balance.
// Key = keccak256("trc10_balance" || big_endian_int64(tokenID))
func trc10BalanceSlot(tokenID int64) tcommon.Hash {
    buf := make([]byte, len("trc10_balance")+8)
    copy(buf, "trc10_balance")
    binary.BigEndian.PutUint64(buf[len("trc10_balance"):], uint64(tokenID))
    return tcommon.Hash(crypto.Keccak256Hash(buf))
}

// trc10FrozenClaimedSlot returns the storage slot key for whether frozen entry index
// has been claimed by the asset issuer.
// Key = keccak256("trc10_frozen_claimed" || big_endian_int64(tokenID) || big_endian_uint32(index))
func trc10FrozenClaimedSlot(tokenID int64, index uint32) tcommon.Hash {
    buf := make([]byte, len("trc10_frozen_claimed")+8+4)
    copy(buf, "trc10_frozen_claimed")
    binary.BigEndian.PutUint64(buf[len("trc10_frozen_claimed"):], uint64(tokenID))
    binary.BigEndian.PutUint32(buf[len("trc10_frozen_claimed")+8:], index)
    return tcommon.Hash(crypto.Keccak256Hash(buf))
}
```

**Encoding int64 balance in a 32-byte slot**: write the int64 value as big-endian uint64 in the last 8 bytes (bytes 24–31) of the 32-byte hash. Read by extracting bytes 24–31 and interpreting as big-endian uint64, cast to int64.

```go
func int64ToSlot(v int64) tcommon.Hash {
    var h tcommon.Hash
    binary.BigEndian.PutUint64(h[24:], uint64(v))
    return h
}

func slotToInt64(h tcommon.Hash) int64 {
    return int64(binary.BigEndian.Uint64(h[24:]))
}
```

**Encoding bool (claimed) in a 32-byte slot**: store `0x01` in byte 31 when claimed; zero hash = not claimed.

### New StateDB Methods (core/state/statedb.go)

```go
// GetTRC10Balance returns the TRC10 token balance of addr for the given tokenID.
func (s *StateDB) GetTRC10Balance(addr tcommon.Address, tokenID int64) int64 {
    slot := trc10BalanceSlot(tokenID)
    return slotToInt64(s.GetState(addr, slot))
}

// SetTRC10Balance sets the TRC10 token balance. Used for initial minting during AssetIssue.
func (s *StateDB) SetTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
    slot := trc10BalanceSlot(tokenID)
    s.SetState(addr, slot, int64ToSlot(amount))
}

// AddTRC10Balance credits amount tokens to addr. Creates the account if needed
// (SetState calls GetOrCreateAccount internally).
func (s *StateDB) AddTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) {
    current := s.GetTRC10Balance(addr, tokenID)
    s.SetTRC10Balance(addr, tokenID, current+amount)
}

// SubTRC10Balance debits amount tokens from addr. Returns ErrInsufficientBalance if addr
// has fewer than amount tokens.
func (s *StateDB) SubTRC10Balance(addr tcommon.Address, tokenID int64, amount int64) error {
    current := s.GetTRC10Balance(addr, tokenID)
    if current < amount {
        return ErrInsufficientBalance
    }
    s.SetTRC10Balance(addr, tokenID, current-amount)
    return nil
}

// IsFrozenClaimed returns whether frozen_supply entry at index has been claimed.
func (s *StateDB) IsFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) bool {
    slot := trc10FrozenClaimedSlot(tokenID, index)
    v := s.GetState(addr, slot)
    return v[31] != 0
}

// SetFrozenClaimed marks frozen_supply entry at index as claimed.
func (s *StateDB) SetFrozenClaimed(addr tcommon.Address, tokenID int64, index uint32) {
    slot := trc10FrozenClaimedSlot(tokenID, index)
    var v tcommon.Hash
    v[31] = 0x01
    s.SetState(addr, slot, v)
}
```

### New Rawdb Functions (core/rawdb/asset.go)

```go
package rawdb

import (
    "encoding/binary"
    contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
    "github.com/ethereum/go-ethereum/ethdb"
    "google.golang.org/protobuf/proto"
)

func WriteAssetIssue(db ethdb.KeyValueWriter, tokenID int64, c *contractpb.AssetIssueContract)
func ReadAssetIssue(db ethdb.KeyValueReader, tokenID int64) *contractpb.AssetIssueContract
func WriteAssetNameIndex(db ethdb.KeyValueWriter, name []byte, tokenID int64)
func ReadAssetNameIndex(db ethdb.KeyValueReader, name []byte) (int64, bool)
func WriteAssetOwnerIndex(db ethdb.KeyValueWriter, ownerAddr []byte, tokenID int64)
func ReadAssetOwnerIndex(db ethdb.KeyValueReader, ownerAddr []byte) (int64, bool)
func WriteAssetIssueTime(db ethdb.KeyValueWriter, tokenID int64, issueTimeMs int64)
func ReadAssetIssueTime(db ethdb.KeyValueReader, tokenID int64) int64
// ListAllAssets iterates the `ast-` prefix (sorted by tokenID ascending) and returns all assets.
func ListAllAssets(db ethdb.Iteratee) []*contractpb.AssetIssueContract
// ListAssetsPaginated returns up to `limit` assets starting at position `offset`.
func ListAssetsPaginated(db ethdb.Iteratee, offset, limit int) []*contractpb.AssetIssueContract
```

Schema additions:
```go
var (
    assetPrefix         = []byte("ast-")   // tokenID (8 bytes big-endian)
    assetNamePrefix     = []byte("astn-")  // token name bytes
    assetOwnerPrefix    = []byte("asto-")  // owner address bytes (20 bytes)
    assetIssueTimePrefix = []byte("asti-") // tokenID (8 bytes big-endian)
)

func assetKey(tokenID int64) []byte {
    k := make([]byte, len(assetPrefix)+8)
    copy(k, assetPrefix)
    binary.BigEndian.PutUint64(k[len(assetPrefix):], uint64(tokenID))
    return k
}
// assetNameKey, assetOwnerKey, assetIssueTimeKey follow the same pattern.
```

## The 6 Actuators

### 1. AssetIssueActuator (type 6) — `actuator/asset_issue.go`

**Validate:**
- Decode `contractpb.AssetIssueContract` from tx
- owner exists and is `AccountType_Normal`
- `name` non-empty
- `abbr` non-empty
- `total_supply > 0`
- `trx_num > 0`, `num > 0`
- `start_time < end_time`
- `precision` in [0, 6]
- `sum(frozen_supply.frozen_amount) <= total_supply`
- `rawdb.ReadAssetNameIndex(db, name)` returns not-found (name uniqueness)
- `rawdb.ReadAssetOwnerIndex(db, owner)` returns not-found (one token per address)
- owner TRX balance `>= dynProps.AssetIssueFee()`

**Execute:**
```
tokenID = dynProps.NextTokenID()           // e.g. 1_000_001
dynProps.SetNextTokenID(tokenID + 1)
contract.Id = strconv.FormatInt(tokenID, 10)
rawdb.WriteAssetIssue(db, tokenID, contract)
rawdb.WriteAssetNameIndex(db, contract.Name, tokenID)
rawdb.WriteAssetOwnerIndex(db, owner[:], tokenID)
rawdb.WriteAssetIssueTime(db, tokenID, ctx.BlockTime)
freeAmount = total_supply - sum(frozen_supply[i].frozen_amount)
state.SetTRC10Balance(owner, tokenID, freeAmount)
// frozen_supply entries are NOT credited yet — they wait for UnfreezeAsset
state.SubBalance(owner, dynProps.AssetIssueFee())
// fee is burned (no recipient), matching TRON's fee burning for asset issuance
```

Result: `Fee = dynProps.AssetIssueFee()`

### 2. TransferAssetActuator (type 2) — `actuator/transfer_asset.go`

The `asset_name` field contains the token ID as a decimal string (post-ALLOW_SAME_TOKEN_NAME; we always operate in ID mode).

**Validate:**
- Decode `contractpb.TransferAssetContract`
- `tokenID, err = strconv.ParseInt(string(contract.AssetName), 10, 64)` — must succeed
- `rawdb.ReadAssetIssue(db, tokenID)` not nil (token exists)
- `contract.Amount > 0`
- `from != to`
- `state.GetTRC10Balance(from, tokenID) >= contract.Amount`

**Execute:**
```
if !state.AccountExists(to):
    state.CreateAccount(to, AccountType_Normal)
    state.SubBalance(from, dynProps.CreateAccountFee())
state.SubTRC10Balance(from, tokenID, contract.Amount)   // error impossible (validated above)
state.AddTRC10Balance(to, tokenID, contract.Amount)
```

Result: `Fee = 0` (unless account created: `Fee = CreateAccountFee`)

### 3. ParticipateAssetIssueActuator (type 9) — `actuator/participate_asset_issue.go`

The buyer (`owner_address`) purchases tokens from the issuer (`to_address`) during the ICO window.

**Validate:**
- Decode `contractpb.ParticipateAssetIssueContract`
- `tokenID = strconv.ParseInt(string(contract.AssetName), 10, 64)` — must succeed
- asset = `rawdb.ReadAssetIssue(db, tokenID)` not nil
- `contract.Amount > 0` (TRX drops to spend)
- `ctx.BlockTime >= asset.StartTime && ctx.BlockTime <= asset.EndTime` (ICO window active)
- `asset.TrxNum > 0`, `asset.Num > 0`
- `tokenAmount = contract.Amount * int64(asset.Num) / int64(asset.TrxNum)` — `tokenAmount > 0`
- `state.GetTRC10Balance(issuer, tokenID) >= tokenAmount` (issuer has tokens available)
- `state.GetBalance(buyer) >= contract.Amount` (buyer has TRX)
- `buyer != issuer`

**Execute:**
```
state.SubBalance(buyer, contract.Amount)
state.AddBalance(issuer, contract.Amount)
state.SubTRC10Balance(issuer, tokenID, tokenAmount)
state.AddTRC10Balance(buyer, tokenID, tokenAmount)
```

Result: `Fee = 0`

### 4. UpdateAssetActuator (type 15) — `actuator/update_asset.go`

Only the issuer can update their token's description, URL, and bandwidth limits.

**Validate:**
- Decode `contractpb.UpdateAssetContract`
- `tokenID, ok = rawdb.ReadAssetOwnerIndex(db, owner[:])` — ok must be true
- `asset = rawdb.ReadAssetIssue(db, tokenID)` not nil

**Execute:**
```
asset.Description = contract.Description
asset.Url = contract.Url
asset.FreeAssetNetLimit = contract.NewLimit
asset.PublicFreeAssetNetLimit = contract.NewPublicLimit
rawdb.WriteAssetIssue(db, tokenID, asset)
```

Result: `Fee = 0`

### 5. VoteAssetActuator (type 3) — `actuator/vote_asset.go`

VoteAsset was deprecated in modern TRON — it had no lasting on-chain effect. Implemented as a validated no-op.

**Validate:**
- Decode `contractpb.VoteAssetContract`
- owner account exists

**Execute:** no state changes.

Result: `Fee = 0`

### 6. UnfreezeAssetActuator (type 14) — `actuator/unfreeze_asset.go`

The token issuer claims pre-frozen supply entries after their lock-up periods expire.

**Validate:**
- Decode `contractpb.UnfreezeAssetContract`
- `tokenID, ok = rawdb.ReadAssetOwnerIndex(db, owner[:])` — ok must be true
- `asset = rawdb.ReadAssetIssue(db, tokenID)` not nil; `len(asset.FrozenSupply) > 0`
- `issueTime = rawdb.ReadAssetIssueTime(db, tokenID)`
- At least one entry where `issueTime + entry.FrozenDays*86_400_000 <= ctx.BlockTime` and `!state.IsFrozenClaimed(owner, tokenID, i)` must exist

**Execute:**
```
const DayMs = 86_400_000
credited := int64(0)
for i, entry := range asset.FrozenSupply:
    if issueTime + entry.FrozenDays*DayMs > ctx.BlockTime: continue
    if state.IsFrozenClaimed(owner, tokenID, uint32(i)): continue
    state.AddTRC10Balance(owner, tokenID, entry.FrozenAmount)
    state.SetFrozenClaimed(owner, tokenID, uint32(i))
    credited += entry.FrozenAmount
if credited == 0: return error "no unfrozen asset"
```

Result: `Fee = 0`

## HTTP Query Endpoints

### New Backend Methods (internal/tronapi/backend.go)

```go
// AssetIssueInfo is the JSON response for a single asset.
// We return the raw proto as JSON (via protojson) for full field coverage.

GetAssetIssueByID(id int64) *contractpb.AssetIssueContract
GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract
GetAssetIssueList() []*contractpb.AssetIssueContract
GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract
GetAssetIssueByAccount(addr common.Address) *contractpb.AssetIssueContract
```

### Route Registration (internal/tronapi/api.go)

```
POST /wallet/getassetissuebyid          body: {"value":"1000001"}
POST /wallet/getassetissuebyname        body: {"value":"TRX"}
GET  /wallet/getassetissuelist
POST /wallet/getpaginatedassetissuelist body: {"offset":0,"limit":20}
POST /wallet/getassetissuebyaccount     body: {"address":"..."}
```

Response format: `protojson.Marshal(contract)` for single; `{"assetIssue":[...]}` wrapper for list endpoints (matching java-tron API shape).

### TronBackend Implementation (core/tron_backend.go)

```go
func (b *TronBackend) GetAssetIssueByID(id int64) *contractpb.AssetIssueContract {
    return rawdb.ReadAssetIssue(b.chain.DB(), id)
}
func (b *TronBackend) GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
    id, ok := rawdb.ReadAssetNameIndex(b.chain.DB(), name)
    if !ok { return nil }
    return rawdb.ReadAssetIssue(b.chain.DB(), id)
}
func (b *TronBackend) GetAssetIssueByAccount(addr common.Address) *contractpb.AssetIssueContract {
    id, ok := rawdb.ReadAssetOwnerIndex(b.chain.DB(), addr[:])
    if !ok { return nil }
    return rawdb.ReadAssetIssue(b.chain.DB(), id)
}
func (b *TronBackend) GetAssetIssueList() []*contractpb.AssetIssueContract {
    return rawdb.ListAllAssets(b.chain.DB())
}
func (b *TronBackend) GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract {
    return rawdb.ListAssetsPaginated(b.chain.DB(), offset, limit)
}
```

## Address Handling

All address fields in TRC10 protos are 21-byte TRON addresses (with `0x41` prefix byte). go-tron's native `common.Address` is also 21 bytes (`AddressLength = 21`), so conversion is straightforward:

```go
ownerAddr := common.BytesToAddress(contract.OwnerAddress)  // common.Address (21 bytes)
```

`common.BytesToAddress` takes the last 21 bytes of the input, so it handles proto byte slices of exactly 21 bytes correctly.

The 21-byte `common.Address` is used for both StateDB calls and rawdb index keys. The `asto-` rawdb key uses `ownerAddr[:]` (21 bytes).

## Testing

### Per-Actuator Tests

**`actuator/asset_issue_test.go`** — tests using `newTestContext()` from existing actuator tests:
- Happy path: issue valid token, verify rawdb entries exist, verify TRC10 balance = freeAmount, verify TRX fee deducted
- Duplicate name: second issue with same name → error
- Name already used: ReadAssetOwnerIndex finds existing → error
- Insufficient fee: owner TRX balance < asset_issue_fee → error
- Invalid supply: total_supply = 0 → error

**`actuator/transfer_asset_test.go`**:
- Happy path: issue token first, then transfer, verify sender balance decremented, receiver credited
- Insufficient TRC10 balance: transfer more than held → error
- Unknown token: random token ID → error
- Account creation fee: transfer to new address, verify fee deducted from sender

**`actuator/participate_asset_issue_test.go`**:
- Happy path: issue token with ICO window, participate, verify buyer gets tokens, issuer gets TRX
- ICO not started: blockTime < start_time → error
- ICO ended: blockTime > end_time → error
- Insufficient TRX: buyer has less than amount → error
- Zero token amount: amount/trx_num rounds to 0 → error

**`actuator/update_asset_test.go`**:
- Happy path: issue then update description/url, verify rawdb reflects changes
- Not owner: different address attempts update → error (ReadAssetOwnerIndex returns not-found)

**`actuator/vote_asset_test.go`**:
- Valid: owner exists → success, no state changes

**`actuator/unfreeze_asset_test.go`**:
- Happy path: issue token with frozen_supply, set blockTime past freeze period, unfreeze → balance credited
- Nothing to unfreeze: freeze period not elapsed → error
- Already claimed: second unfreeze call for same entry → skipped (no double-credit)
- No frozen supply: token with empty frozen_supply list → error

### StateDB TRC10 Tests — `core/state/statedb_trc10_test.go`

- `GetTRC10Balance` on non-existent account returns 0
- `SetTRC10Balance` / `GetTRC10Balance` roundtrip
- `AddTRC10Balance` / `SubTRC10Balance` roundtrip
- `SubTRC10Balance` with insufficient balance returns `ErrInsufficientBalance`
- `IsFrozenClaimed` returns false before `SetFrozenClaimed`; true after
- Two different token IDs have independent slots (no collision)

### System Test Section 11 — `scripts/system_test.sh`

Since system_test.sh operates against a running node with no pre-issued tokens, all asset query endpoints return empty/null results — we verify they return HTTP 200 without error:

```bash
# Section 11: TRC10 asset query endpoints
check "getassetissuebyid (unknown)"      "$(curl -s -X POST http://localhost:8090/wallet/getassetissuebyid -d '{"value":"1000001"}')" "" ""
check "getassetissuelist (empty)"        "$(curl -s http://localhost:8090/wallet/getassetissuelist)" "" ""
check "getpaginatedassetissuelist"       "$(curl -s -X POST http://localhost:8090/wallet/getpaginatedassetissuelist -d '{"offset":0,"limit":10}')" "" ""
```

(The `check` helper verifies HTTP 200 and no `"Error"` key in the response body.)

## Error Cases

- Unknown token ID: `rawdb.ReadAssetIssue` returns nil → `"token not found"`
- Insufficient TRC10 balance: `SubTRC10Balance` returns `ErrInsufficientBalance`
- ICO window checks: `blockTime < start_time` → `"ico not started"`, `blockTime > end_time` → `"ico ended"`
- Name collision: `ReadAssetNameIndex` returns found → `"token name already exists"`
- Duplicate issuance: `ReadAssetOwnerIndex` returns found → `"address already issued a token"`

## No New External Dependencies

Uses only already-imported packages: `encoding/binary`, `strconv`, `errors`, `google.golang.org/protobuf/proto`, `github.com/ethereum/go-ethereum/crypto` (already used in state/statedb.go).
