# M5.2 JSON-RPC 写路径 + Filter 系统 — Plan

**Date**: 2026-04-26  
**Spec**: `docs/superpowers/specs/2026-04-26-m5-jsonrpc-write-design.md`

## PR-1: Utility methods (eth_gasPrice, web3_sha3, net_listening, net_peerCount)

- [ ] Add `GasPrice() int64` and `PeerCount() int` to `jsonrpc.Backend`
- [ ] Implement in `core/tron_backend.go`:
  - `GasPrice()` → `state.LoadDynamicProperties(b.chain.db).EnergyFee()`
  - `PeerCount()` → `b.p2pServer.PeerCount()` (needs `p2p.Server` reference on TronBackend)
- [ ] Add handlers in `api.go`:
  - `eth_gasPrice` → `hexUint64(backend.GasPrice())`
  - `web3_sha3` → decode hex param, return keccak256
  - `net_listening` → `backend.PeerCount() >= 1`
  - `net_peerCount` → `hexUint64(uint64(backend.PeerCount()))`
- [ ] Add stub methods to `api_test.go` stubBackend + 4 tests
- [ ] Update grpcapi stubBackend if needed
- [ ] `go test ./...` green

## PR-2: Write stubs + eth_accounts

- [ ] Add 5 case entries to `dispatch` in `api.go`:
  - `eth_sendRawTransaction`, `eth_sendTransaction`, `eth_sign`, `eth_signTransaction` → return error -32601
  - `eth_accounts` → return `[]string{}`
- [ ] Add 5 tests to `api_test.go`
- [ ] `go test ./...` green

## PR-3: eth_estimateGas

- [ ] Add `EstimateGas(from, to *common.Address, data []byte, value int64) (uint64, error)` to `jsonrpc.Backend`
- [ ] Implement in `core/tron_backend.go`:
  - If `to != nil && len(data) == 0`: return 0 (plain transfer, no energy)
  - Else: run `b.Call(from, to, data, value)` and return energy from result
- [ ] Add `eth_estimateGas` handler in `api.go`
- [ ] Add stub + test
- [ ] `go test ./...` green

## PR-4: Filter subsystem

- [ ] Create `internal/jsonrpc/filter.go`:
  - `FilterManager` struct with mutex-guarded `map[string]*filter`
  - `filter` struct: kind (log/block), `LogFilter`, `lastPoll time.Time`, `pendingLogs`, `pendingHashes`
  - `generateFilterID()` → 32 random bytes → hex
  - `NewFilter`, `NewBlockFilter`, `UninstallFilter`, `GetFilterChanges`, `GetFilterLogs` methods
  - `Start()`/`Stop()` lifecycle with background GC goroutine (5 min idle expiry, 30s tick)
- [ ] Add `SubscribeBlocks(chan<- *types.Block)` and `UnsubscribeBlocks(chan<- *types.Block)` to `jsonrpc.Backend`
- [ ] Implement subscription in `core/tron_backend.go`:
  - Slice of subscriber channels; non-blocking fan-out called from `BlockChain` after insert
- [ ] Wire `FilterManager` into `API` struct; call `fm.Start()` in `NewAPI` or separate `Start()` call
- [ ] Add 5 dispatch cases to `api.go`
- [ ] Add tests: newFilter, newBlockFilter, getFilterChanges, getFilterLogs, uninstallFilter
- [ ] `go test ./...` green
