# M4 gRPC Wallet Server — Plan

**Date**: 2026-04-26  
**Spec**: `docs/superpowers/specs/2026-04-26-m4-grpc-wallet-server-design.md`

## PR-A0: Foundation ✅ 完成 2026-04-26

- [x] Vendor `proto/google/api/annotations.proto` + `proto/google/api/http.proto`
- [x] Update `Makefile`: separate protoc invocations; `--go-grpc_out` + `--go-grpc_opt` + `--proto_path` + `Mgoogle/api` overrides
- [x] Run `make proto` → committed `proto/api/api.pb.go` + `proto/api/api_grpc.pb.go`
- [x] Add `google.golang.org/grpc` + `google.golang.org/genproto/googleapis/api` to `go.mod` / `go.sum`
- [x] Create `internal/grpcapi/server.go`: `Server` + `UnimplementedWalletServer` embed + `node.Lifecycle` (`Start`/`Stop`)
- [x] Implement 5 starter read RPCs: `GetNowBlock`, `GetBlockByNum`, `GetAccount`, `GetTransactionById`, `GetChainParameters`
- [x] Wire in `cmd/gtron/main.go`: `--grpc.port` flag (default 50051, 0=disabled) + lifecycle
- [x] `internal/grpcapi/server_test.go`: 6 bufconn-based tests for the 5 RPCs
- [x] `go test ./...` green (25 packages)

## PR-A1: Block/Account read RPCs ✅ 完成 2026-04-26

- [x] `GetNowBlock2`, `GetBlockByNum2` (BlockExtention variants)
- [x] `GetBlockById`, `GetBlockByLimitNext`, `GetBlockByLimitNext2`, `GetBlockByLatestNum`, `GetBlockByLatestNum2`
- [x] `GetTransactionCountByBlockNum`
- [x] `GetAccountById`
- [x] `GetContract`, `GetContractInfo`
- [x] `ListWitnesses`
- [x] `GetNextMaintenanceTime`
- [x] Tests for each (19 total, all passing)

## PR-A2: Resource/Market/TRC10 read RPCs ✅ 完成 2026-04-26

- [x] `GetAccountResource`
- [x] `GetDelegatedResourceV2`, `GetDelegatedResourceAccountIndexV2`, `GetCanDelegatedMaxSize`
- [x] `GetCanWithdrawUnfreezeAmount`, `GetAvailableUnfreezeCount`
- [x] `GetRewardInfo`, `GetBrokerageInfo`
- [x] `GetAssetIssueById`, `GetAssetIssueByAccount`, `GetAssetIssueList`
- [x] `GetMarketOrderById`, `GetMarketOrderByAccount`, `GetMarketPriceByPair`
- [x] `ListNodes`, `GetNodeInfo`
- [x] `ListProposals`, `ListExchanges`
- [x] `GetTransactionInfoById`, `GetTransactionInfoByBlockNum`, `GetTransactionListFromPending`
- [x] `TotalTransaction`, `GetBurnTrx` (stubs — 0 until dynamic-properties tracking)
- [x] Tests for each (34 total, all passing); `ListAllExchanges` added to rawdb

## PR-B: Transaction building ✅ 完成 2026-04-26

- [x] `CreateTransaction`, `CreateTransaction2` (Transfer)
- [x] `VoteWitnessAccount2`, `FreezeBalanceV2`, `UnfreezeBalanceV2`
- [x] `DelegateResource`, `UnDelegateResource`, `CancelAllUnfreezeV2`, `WithdrawExpireUnfreeze`
- [x] `ProposalCreate`, `ProposalApprove`, `ProposalDelete`
- [x] `TriggerContract`, `TriggerConstantContract`, `DeployContract`
- [x] `EstimateEnergy`
- [x] `BroadcastTransaction` (moved from PR-C)
- [x] Tests for each (43 total, all passing)
- [x] Backend interface extended: 7 new Stake 2.0 builders + `BuildVoteWitnessTransaction`

## PR-C: Sign helpers ✅ 完成 2026-04-26

- [x] `GetTransactionSignWeight` (multi-sig weight info)
- [x] Tests

## PR-E: Monitor/Node ✅ 完成 2026-04-26

- [x] `ListNodes`, `GetNodeInfo` (done in PR-A2)
- [x] `GetPaginatedProposalList`, `GetPaginatedAssetIssueList`, `GetPaginatedExchangeList`
- [x] `GetBandwidthPrices`, `GetEnergyPrices` (stubs — price history not tracked yet)
- [x] Tests (48 total in grpcapi, all passing)
