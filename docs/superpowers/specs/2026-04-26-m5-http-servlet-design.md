# M5.1 HTTP Servlet 补齐 — Design

**Date**: 2026-04-26  
**Milestone**: M5.1  
**Status**: Active  
**Spec basis**: `TODO.md §4.2`, java-tron `framework/src/main/java/org/tron/core/services/http/`

## Goal

Add the ~30 missing HTTP servlet clusters so that standard TRON wallets and SDKs can call go-tron's HTTP API directly. All handlers are thin adapters over `tronapi.Backend` — no new domain logic.

## File structure

`internal/tronapi/api.go` (1137 lines) will exceed 1500 with PR-1 alone. Following java-tron's pattern of one file per cluster, split new handlers into cluster files:

```
internal/tronapi/
  api.go              # RegisterRoutes + shared helpers (unchanged, routes added)
  api_account.go      # PR-1: account/permission endpoints (new)
  api_tx.go           # PR-2: transaction builders (new)
  api_trc10.go        # PR-3: TRC10 asset endpoints (new)
  api_contract.go     # PR-4: smart contract extras (new)
  api_stake.go        # PR-5: stake/delegation/freeze endpoints (new)
  api_exchange.go     # PR-6: exchange/market endpoints (new)
  api_proposal.go     # PR-7: proposal/monitoring extras (new)
  api_misc.go         # PR-8: tx-meta helpers (new)
```

`api.go` retains all existing handlers (they won't be moved — no churn). New handlers go in their cluster file.

## PR sequencing

### PR-1: Account / Permission / ID (6 endpoints)

`getaccountbalance` is deferred (requires `AccountTraceStore` / `btrace-` historical store, not yet implemented).

| Route | Method | Contract/Proto | Notes |
|-------|--------|----------------|-------|
| `/wallet/createaccount` | POST | `AccountCreateContract` | tx builder |
| `/wallet/updateaccount` | POST | `AccountUpdateContract` | tx builder |
| `/wallet/setaccountid` | POST | `SetAccountIdContract` | tx builder |
| `/wallet/accountpermissionupdate` | POST | `AccountPermissionUpdateContract` | tx builder |
| `/wallet/getaccountbyid` | GET+POST | `Account{account_id}` → `Account` | read query |
| `/wallet/getaccountnet` | GET+POST | `address` → `AccountNetMessage` | read query |

**New Backend methods** (add to interface + implement in `core/tron_backend.go`):
```go
BuildCreateAccountTransaction(owner, account common.Address) (*corepb.Transaction, error)
BuildUpdateAccountTransaction(owner common.Address, name []byte) (*corepb.Transaction, error)
BuildSetAccountIdTransaction(owner common.Address, accountID []byte) (*corepb.Transaction, error)
BuildAccountPermissionUpdateTransaction(c *contractpb.AccountPermissionUpdateContract) (*corepb.Transaction, error)
GetAccountById(accountID []byte) (*types.Account, error)
GetAccountNet(addr common.Address) (*apipb.AccountNetMessage, error)
```

`GetAccountNet` returns bandwidth info (free net used/limit, staked net used/limit, TRC10 asset net maps) — same data as `getaccountresource` but only the bandwidth fields, serialized as `AccountNetMessage` proto (already exists in `proto/api/api.pb.go`).

**Handler pattern** (tx builders):
```go
func (api *API) createAccount(w http.ResponseWriter, r *http.Request) {
    body := readBody(r)
    var c contractpb.AccountCreateContract
    if err := protojson.Unmarshal(body, &c); err != nil {
        http.Error(w, err.Error(), http.StatusBadRequest); return
    }
    tx, err := api.backend.BuildCreateAccountTransaction(owner, account)
    writeProtoJSON(w, tx, err)
}
```

**Handler pattern** (read queries):
```go
func (api *API) getAccountById(w http.ResponseWriter, r *http.Request) {
    body := readBody(r)
    var req corepb.Account
    protojson.Unmarshal(body, &req)
    acct, err := api.backend.GetAccountById(req.GetAccountId())
    writeAccountJSON(w, acct, err)
}
```

### PR-2: Transaction builders (~16 endpoints)

| Route | Contract | Notes |
|-------|----------|-------|
| `/wallet/createcommon transaction` | via `contract_type` param | dispatches to existing builders |
| `/wallet/transferasset` | `TransferAssetContract` | TRC10 transfer |
| `/wallet/participateassetissue` | `ParticipateAssetIssueContract` | TRC10 buy |
| `/wallet/createwitness` | `WitnessCreateContract` | become SR candidate |
| `/wallet/votewitnessaccount` | `VoteWitnessContract` | vote for witnesses |
| `/wallet/updatewitness` | `WitnessUpdateContract` | update SR URL |
| `/wallet/withdrawbalance` | `WithdrawBalanceContract` | claim staking rewards |
| `/wallet/updatebrokerage` | `UpdateBrokerageContract` | update commission rate |
| `/wallet/freezebalance` | `FreezeBalanceContract` (V1) | legacy freeze |
| `/wallet/unfreezebalance` | `UnfreezeBalanceContract` (V1) | legacy unfreeze |
| `/wallet/freezebalancev2` | `FreezeBalanceV2Contract` | already has backend method |
| `/wallet/unfreezebalancev2` | `UnfreezeBalanceV2Contract` | already has backend method |
| `/wallet/cancelallunfreezev2` | `CancelAllUnfreezeV2Contract` | already has backend method |
| `/wallet/delegateresource` | `DelegateResourceContract` | already has backend method |
| `/wallet/undelegateresource` | `UnDelegateResourceContract` | already has backend method |
| `/wallet/withdrawexpireunfreeze` | `WithdrawExpireUnfreezeContract` | already has backend method |

New backend methods needed: `BuildTransferAssetTransaction`, `BuildParticipateAssetIssueTransaction`, `BuildCreateWitnessTransaction`, `BuildVoteWitnessTransaction` (already exists), `BuildUpdateWitnessTransaction`, `BuildWithdrawBalanceTransaction`, `BuildUpdateBrokerageTransaction`, `BuildFreezeBalanceV1Transaction`, `BuildUnfreezeBalanceV1Transaction`.

`createcommontransaction` is a meta-endpoint that accepts a `contract_type` field and dispatches to the appropriate builder — implement last in PR-2.

### PR-3: TRC10 asset (~4 endpoints)

| Route | Notes |
|-------|-------|
| `/wallet/createassetissue` | `AssetIssueContract` tx builder |
| `/wallet/updateasset` | `UpdateAssetContract` tx builder |
| `/wallet/getpaginatedassetissuelist` | already implemented! skip |
| `/wallet/getassetissuelistbyname` | multi-name variant |

### PR-4: Smart contract extras (1 endpoint)

| Route | Notes |
|-------|-------|
| `/wallet/clearabi` | `ClearABIContract` tx builder |

### PR-5: Exchange / Market (~6 endpoints)

| Route | Contract | Notes |
|-------|----------|-------|
| `/wallet/exchangecreate` | `ExchangeCreateContract` | tx builder |
| `/wallet/exchangeinject` | `ExchangeInjectContract` | tx builder |
| `/wallet/exchangetransaction` | `ExchangeTransactionContract` | tx builder |
| `/wallet/exchangewithdraw` | `ExchangeWithdrawContract` | tx builder |
| `/wallet/marketcancelorder` | `MarketCancelOrderContract` | tx builder |
| `/wallet/marketsellasset` | `MarketSellAssetContract` | tx builder |

### PR-6: Proposal / Monitoring extras (3 endpoints)

| Route | Notes |
|-------|-------|
| `/wallet/getproposalbyid` | `Proposal` by ID — needs backend method |
| `/wallet/getpaginatedproposallist` | already in gRPC; add HTTP |
| `/wallet/metrics` | stub JSON `{"ok": true}` (no Prometheus yet) |

### PR-7: Transaction meta (4 endpoints)

| Route | Notes |
|-------|-------|
| `/wallet/gettransactionreceiptbyid` | alias for `gettransactioninfobyid` |
| `/wallet/gettransactionapprovedlist` | multi-sig approval list |
| `/wallet/gettransactionsignweight` | sign-weight info (already in gRPC) |
| `/wallet/validateaddress` | validate a TRON address |

### Deferred

- `getaccountbalance` — requires `AccountTraceStore` (btrace history store)
- Shielded endpoints (`PR-5` in PLAN.md) — requires M2 PR-5 shielded storage and Zcash/Sapling crypto

## Wire type mapping

All HTTP handlers follow the same pattern as existing ones in `api.go`:
1. `readBody(r)` → raw bytes
2. `protojson.Unmarshal(body, &msg)` for proto inputs
3. Delegate to `api.backend.*` method
4. `protojson.Marshal` output → write response

Helpers already in `api.go`: `readBody`, `writeProtoJSON`, `writeAccountJSON`.

## `GetAccountNet` implementation

`GetAccountNet` computes bandwidth stats for an address. The data is a subset of `GetAccountResource` — same fields the bandwidth processor fills in. Backend implementation in `core/tron_backend.go`:

```go
func (b *TronBackend) GetAccountNet(addr common.Address) (*apipb.AccountNetMessage, error) {
    acct := b.chain.StateDB().GetAccount(addr)
    if acct == nil { return nil, nil }
    dp := b.chain.StateDB().DynamicProperties()
    msg := &apipb.AccountNetMessage{
        FreeNetUsed:    acct.FreeNetUsage,
        FreeNetLimit:   dp.GetFreeNetLimit(),
        NetUsed:        acct.NetUsage,
        NetLimit:       computeNetLimit(acct, dp),
        TotalNetLimit:  dp.GetTotalNetLimit(),
        TotalNetWeight: dp.GetTotalNetWeight(),
    }
    return msg, nil
}
```

TRC10 asset net maps are omitted for now (empty maps, same as `GetAccountResource` current stub behavior).

## Testing

- Each new handler gets an entry in `internal/tronapi/api_test.go` (same HTTP test style)
- Backend mock in test file already exists; add methods to the mock struct
- Tests use `httptest.NewRecorder` — no server port needed

## java-tron reference

Servlet files in `framework/src/main/java/org/tron/core/services/http/`:
- `CreateAccountServlet.java`, `UpdateAccountServlet.java`, `SetAccountIdServlet.java`
- `AccountPermissionUpdateServlet.java`
- `GetAccountByIdServlet.java`, `GetAccountNetServlet.java`
