# Phase 10: HTTP API Query Completion — Design Spec

**Date:** 2026-04-11

## Goal

Add 9 missing HTTP query endpoints to the go-tron node so that clients can interrogate delegation state, pending unfreeze windows, unclaimed rewards, the transaction pool, and connected peers without needing a full java-tron node.

## Context

Phases 1–9 built a working go-tron node with block production, P2P, TVM/EVM, transaction lifecycle, governance, and smart contract persistence. The HTTP API at `:8090` already has ~25 endpoints. This phase adds the remaining read-only query layer to reach feature parity with the most commonly used java-tron wallet API calls.

## Architecture

Single-file approach: all changes fit inside four existing files — no new files created. The Backend interface gains 9 new methods and the corresponding implementations are added to TronBackend. A new `PeerLister` injection pattern (matching the existing `TxBroadcaster`) provides access to P2P peer state without importing the `net` package from `core`.

## Files Modified

| File | Change |
|---|---|
| `internal/tronapi/backend.go` | Add `PeerInfo` struct; add 9 new methods to `Backend` interface |
| `internal/tronapi/api.go` | Register 9 new route handlers; implement handler functions |
| `core/tron_backend.go` | Add `PeerLister` interface + `peersFunc` field + setter; implement 9 Backend methods |
| `cmd/gtron/main.go` | Wire `backend.SetPeerLister(handler.ConnectedPeers)` |

## Data Sources

| Endpoint | Source |
|---|---|
| `getdelegatedresourcev2` | `rawdb.ReadDelegatedResource(db, from, to)` |
| `getdelegatedresourceaccountindexv2` | `rawdb.ReadDelegationIndex(db, from)` |
| `candelegateresource` | `statedb.GetFrozenV2Amount(addr, resource)` |
| `getcanwithdrawunfreezeamount` | `statedb.UnfrozenV2()` filtered by `now >= expireTime` |
| `getavailableunfreezecount` | `32 - statedb.UnfreezeV2Count(addr)` (TRON max = 32) |
| `getreward` | `statedb.GetAllowance(addr)` |
| `gettransactionfrompending` | `pool.Get(hash)` |
| `gettransactionlistfrompending` | `pool.Pending()` |
| `listnodes` | `peersFunc()` — injected from `p2p.Server` via `main.go` |

## New Response Types (in `backend.go`)

```go
type PeerInfo struct {
    Address string `json:"address"`
    Host    string `json:"host"`
    Port    int    `json:"port"`
}

type DelegatedResourceInfo struct {
    FromAddress               string `json:"fromAddress"`
    ToAddress                 string `json:"toAddress"`
    FrozenBalanceForBandwidth int64  `json:"frozenBalanceForBandwidth"`
    FrozenBalanceForEnergy    int64  `json:"frozenBalanceForEnergy"`
    ExpireTimeForBandwidth    int64  `json:"expireTimeForBandwidth"`
    ExpireTimeForEnergy       int64  `json:"expireTimeForEnergy"`
}

type DelegationIndexInfo struct {
    Account    string   `json:"account"`
    ToAddresses []string `json:"toAddresses"`
}

type CanDelegateInfo struct {
    MaxSize        int64 `json:"maxSize"`
    CanDelegateSize int64 `json:"canDelegateSize"`
    Balance        int64 `json:"balance"`
}

type CanWithdrawUnfreezeInfo struct {
    Amount int64 `json:"amount"`
}

type AvailableUnfreezeCountInfo struct {
    Count int64 `json:"count"`
}

type RewardInfo struct {
    Reward int64 `json:"reward"`
}
```

## New Backend Interface Methods

```go
// Resource/delegation queries
GetDelegatedResourceV2(from, to common.Address) (*DelegatedResourceInfo, error)
GetDelegatedResourceAccountIndexV2(addr common.Address) (*DelegationIndexInfo, error)
CanDelegateResource(addr common.Address, amount int64, resource int32) (*CanDelegateInfo, error)
GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*CanWithdrawUnfreezeInfo, error)
GetAvailableUnfreezeCount(addr common.Address) (*AvailableUnfreezeCountInfo, error)

// Rewards
GetReward(addr common.Address) (*RewardInfo, error)

// TX pool queries
GetTransactionFromPending(txID string) (*corepb.Transaction, error)
GetTransactionListFromPending() ([]*corepb.Transaction, error)

// Network
ListNodes() ([]*PeerInfo, error)
```

## PeerLister Injection

To avoid import cycle (`core` → `net` is forbidden), `core/tron_backend.go` defines:

```go
// PeerLister returns currently connected P2P peers.
// Implemented by net.TronHandler; defined here to avoid an import cycle.
type PeerLister interface {
    ConnectedPeers() []p2p.PeerInfo
}
```

Actually, to avoid any dependency on the `p2p` package types in `core`, we use a simpler function-based approach:

```go
// peersFunc is a late-bound function that returns connected peer addresses.
// Wired from main.go to avoid core→net→p2p import cycle.
type peersFunc func() []tronapi.PeerInfo
```

`TronBackend` stores `peersFunc` and exposes:
```go
func (b *TronBackend) SetPeerLister(fn func() []tronapi.PeerInfo) {
    b.peersFunc = fn
}
```

`main.go` wires it:
```go
backend.SetPeerLister(func() []tronapi.PeerInfo {
    peers := handler.HandshakedPeers()
    result := make([]tronapi.PeerInfo, len(peers))
    for i, p := range peers {
        result[i] = tronapi.PeerInfo{Address: p.ID, Host: p.Host, Port: p.Port}
    }
    return result
})
```

This depends on `p2p.PeerConn` having `Host`/`Port` fields (or whatever the existing peer struct exposes). We'll use whatever fields are already on the connected peer type from the `p2p` package.

## Endpoint Specifications

### 1. `POST /wallet/getdelegatedresourcev2`

Request: `{"fromAddress": "<hex>", "toAddress": "<hex>"}`

Response:
```json
{
  "delegatedResource": [{
    "fromAddress": "...",
    "toAddress": "...",
    "frozenBalanceForBandwidth": 0,
    "frozenBalanceForEnergy": 1000000,
    "expireTimeForBandwidth": 0,
    "expireTimeForEnergy": 1712345678000
  }]
}
```

Returns empty `delegatedResource: []` if no delegation exists.

### 2. `POST /wallet/getdelegatedresourceaccountindexv2`

Request: `{"value": "<hex address>"}`

Response:
```json
{
  "account": "...",
  "toAddresses": ["...", "..."]
}
```

`toAddresses` lists all addresses that `account` has delegated resources to.

### 3. `POST /wallet/candelegateresource`

Request: `{"ownerAddress": "<hex>", "balance": <int64>, "type": <0=BW|1=Energy>}`

Response:
```json
{
  "maxSize": 1000000,
  "canDelegateSize": 800000,
  "balance": 500000
}
```

`canDelegateSize` = available frozen balance of the requested resource type. `maxSize` = total frozen. `balance` = requested amount, echoed back.

### 4. `POST /wallet/getcanwithdrawunfreezeamount`

Request: `{"ownerAddress": "<hex>", "timestamp": <unix_ms>}`

Response:
```json
{"amount": 5000000}
```

Sum of all `unfrozenV2` entries where `unfreezeExpireTime <= timestamp`.

### 5. `POST /wallet/getavailableunfreezecount`

Request: `{"ownerAddress": "<hex>"}`

Response:
```json
{"count": 30}
```

`count` = `32 - len(account.unfrozenV2)`.

### 6. `POST /wallet/getreward`

Request: `{"address": "<hex>"}`

Response:
```json
{"reward": 123456}
```

Returns the `allowance` field = unclaimed witness reward balance.

### 7. `POST /wallet/gettransactionfrompending`

Request: `{"value": "<tx_id_hex>"}`

Response: full `Transaction` protobuf as JSON, or `{"Error": "transaction not found"}`.

### 8. `POST /wallet/gettransactionlistfrompending`

Request: `{}` (no parameters)

Response:
```json
{
  "transaction": [<Transaction>, ...]
}
```

### 9. `GET /wallet/listnodes`

Request: no parameters

Response:
```json
{
  "nodes": [
    {"address": {"host": "127.0.0.1", "port": 18888}}
  ]
}
```

## TX Pool `Get` Method

If `pool.Get(hash)` does not exist today, it needs to be added to `core/txpool/txpool.go`. It returns `*types.Transaction` or nil.

## Error Handling

All endpoints return HTTP 400 for bad input, HTTP 404 for not-found resources, HTTP 500 for internal errors. Response format for errors: `{"Error": "<message>"}`.

## Testing

Each new Backend method gets a unit test in `core/tron_backend_test.go` using an in-memory state. The 9 handler functions are tested via `httptest` in `internal/tronapi/api_test.go`. The system test (`scripts/system_test.sh`) gets a new section (Section 9) that calls all 9 endpoints and checks for non-error responses.

## No New Files

All changes are in-place additions to existing files. No new packages, no new files.
