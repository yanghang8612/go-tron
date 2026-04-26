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

## PR-2: Transaction builders (~15 endpoints) ✅ 完成 2026-04-26

- [x] Add 8 new Backend methods: `BuildTransferAssetTransaction`,
  `BuildParticipateAssetIssueTransaction`, `BuildCreateWitnessTransaction`,
  `BuildUpdateWitnessTransaction`, `BuildWithdrawBalanceTransaction`,
  `BuildUpdateBrokerageTransaction`, `BuildFreezeBalanceV1Transaction`,
  `BuildUnfreezeBalanceV1Transaction`
  (`createcommontransaction` deferred — complex dispatch, low usage priority)
- [x] Implement 8 methods in `core/tron_backend.go`
- [x] Create `internal/tronapi/api_tx.go` with 15 handlers
- [x] Register 15 routes in `api.go`
- [x] Add 15 tests; all 25 packages green

## PR-3: TRC10 asset extras ✅ 完成 2026-04-26

- [x] Add `BuildCreateAssetIssueTransaction`, `BuildUpdateAssetTransaction` to Backend
- [x] Implement in `core/tron_backend.go`
- [x] Create `internal/tronapi/api_trc10.go` with:
  `createAssetIssue`, `updateAsset`, `getAssetIssueListByName`
- [x] Register 3 routes in `api.go`
- [x] Add 3 tests
- [x] `go test ./...` green

## PR-4: Smart contract extras ✅ 完成 2026-04-26

- [x] Add `BuildClearABITransaction` to Backend (via generic `BuildContractTransaction`)
- [x] Implement in `core/tron_backend.go`
- [x] `clearABI` handler in `internal/tronapi/api_trc10.go`
- [x] Register 1 route in `api.go`
- [x] Add 1 test
- [x] `go test ./...` green

## PR-5: Exchange / Market ✅ 完成 2026-04-26

- [x] 6 Backend methods via generic `BuildContractTransaction`
- [x] Create `internal/tronapi/api_exchange.go` with 6 handlers:
  `exchangeCreate`, `exchangeInject`, `exchangeTransaction`, `exchangeWithdraw`,
  `marketSellAsset`, `marketCancelOrder`
- [x] Register 6 routes in `api.go`
- [x] Add 6 tests
- [x] `go test ./...` green

## PR-6: Proposal / Monitoring extras ✅ 完成 2026-04-26

- [x] `GetProposalByID`, `ListProposalsPaginated`, `ValidateAddress` added to Backend
- [x] Create `internal/tronapi/api_misc.go` with:
  `getProposalById`, `getPaginatedProposalList`, `metricsStub`,
  `getTransactionReceiptById`, `validateAddress`
- [x] Register 3 routes in `api.go`
- [x] Add 3 tests
- [x] `go test ./...` green

## PR-7: Transaction meta ✅ 完成 2026-04-26

- [x] `ValidateAddress` added to Backend
- [x] `getTransactionReceiptById` (alias), `validateAddress` handlers in `api_misc.go`
- [x] Register 2 routes in `api.go`
- [x] Add 2 tests
- [x] `go test ./...` green (all 28 packages)
