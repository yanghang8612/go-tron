# M5.1 HTTP Servlet 补齐 — Plan

**Date**: 2026-04-26  
**Spec**: `docs/superpowers/specs/2026-04-26-m5-http-servlet-design.md`

## PR-1: Account / Permission / ID ✅ 完成 2026-04-26

- [x] Add 6 Backend interface methods to `internal/tronapi/backend.go`:
  `BuildCreateAccountTransaction`, `BuildUpdateAccountTransaction`,
  `BuildSetAccountIdTransaction`, `BuildAccountPermissionUpdateTransaction`,
  `GetAccountById`, `GetAccountNet`
- [x] Implement 6 methods in `core/tron_backend.go`
- [x] Create `internal/tronapi/api_account.go` with 6 handler functions:
  `createAccount`, `updateAccount`, `setAccountId`, `accountPermissionUpdate`,
  `getAccountById`, `getAccountNet`
- [x] Register 6 routes in `api.go` `RegisterRoutes`
- [x] Add 6 handler tests in `internal/tronapi/api_test.go` (7 tests total)
- [x] `go test ./...` green (25 packages)

## PR-2: Transaction builders (~16 endpoints)

- [ ] Add 9 new Backend methods: `BuildTransferAssetTransaction`,
  `BuildParticipateAssetIssueTransaction`, `BuildCreateWitnessTransaction`,
  `BuildUpdateWitnessTransaction`, `BuildWithdrawBalanceTransaction`,
  `BuildUpdateBrokerageTransaction`, `BuildFreezeBalanceV1Transaction`,
  `BuildUnfreezeBalanceV1Transaction`, `BuildCreateCommonTransaction`
- [ ] Implement 9 methods in `core/tron_backend.go`
- [ ] Create `internal/tronapi/api_tx.go` with 16 handlers
- [ ] Register 16 routes in `api.go`
- [ ] Add 16 tests
- [ ] `go test ./...` green

## PR-3: TRC10 asset extras

- [ ] Add `BuildCreateAssetIssueTransaction`, `BuildUpdateAssetTransaction` to Backend
- [ ] Implement in `core/tron_backend.go`
- [ ] Create `internal/tronapi/api_trc10.go` with:
  `createAssetIssue`, `updateAsset`, `getAssetIssueListByName`
- [ ] Register 3 routes in `api.go`
- [ ] Add 3 tests
- [ ] `go test ./...` green

## PR-4: Smart contract extras

- [ ] Add `BuildClearABITransaction` to Backend
- [ ] Implement in `core/tron_backend.go`
- [ ] Create `internal/tronapi/api_contract.go` with `clearABI`
- [ ] Register 1 route in `api.go`
- [ ] Add 1 test
- [ ] `go test ./...` green

## PR-5: Exchange / Market

- [ ] Add 6 Backend methods: `BuildExchangeCreateTransaction`, `BuildExchangeInjectTransaction`,
  `BuildExchangeTransactionTransaction`, `BuildExchangeWithdrawTransaction`,
  `BuildMarketCancelOrderTransaction`, `BuildMarketSellAssetTransaction`
- [ ] Implement 6 methods in `core/tron_backend.go`
- [ ] Create `internal/tronapi/api_exchange.go` with 6 handlers
- [ ] Register 6 routes in `api.go`
- [ ] Add 6 tests
- [ ] `go test ./...` green

## PR-6: Proposal / Monitoring extras

- [ ] Add `GetProposalByID`, `GetBandwidthPrices`, `GetEnergyPrices` (already exist) to usable list
- [ ] Create `internal/tronapi/api_proposal.go` with:
  `getProposalByID`, `getPaginatedProposalList`, `metricsStub`
- [ ] Register 3 routes in `api.go`
- [ ] Add 3 tests
- [ ] `go test ./...` green

## PR-7: Transaction meta

- [ ] Add `GetTransactionApprovedList`, `ValidateAddress` to Backend
- [ ] Create `internal/tronapi/api_misc.go` with:
  `getTransactionReceiptByID`, `getTransactionApprovedList`,
  `getTransactionSignWeight`, `validateAddress`
- [ ] Register 4 routes in `api.go`
- [ ] Add 4 tests
- [ ] `go test ./...` green
