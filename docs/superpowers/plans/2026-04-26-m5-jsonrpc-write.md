# M5.2 JSON-RPC 写路径 + Filter 系统 — Plan

**Date**: 2026-04-26  
**Spec**: `docs/superpowers/specs/2026-04-26-m5-jsonrpc-write-design.md`

## PR-1: Utility methods (eth_gasPrice, web3_sha3, net_listening, net_peerCount) ✅ 完成 2026-04-26

- [x] Add `GasPrice() int64` and `PeerCount() int` to `jsonrpc.Backend`
- [x] Implement in `core/tron_backend.go`:
  - `GasPrice()` → `state.LoadDynamicProperties(b.chain.db).EnergyFee()`
  - `PeerCount()` → `len(b.peersFunc())` (reuses existing peersFunc)
- [x] Add handlers in `api.go`: eth_gasPrice, web3_sha3, net_listening, net_peerCount
- [x] Tests pass; also fixed flaky p2p TestServerMaintainReconnectsToSeed

## PR-2: Write stubs + eth_accounts ✅ 完成 2026-04-26

- [x] eth_sendRawTransaction, eth_sendTransaction, eth_sign, eth_signTransaction → -32601
- [x] eth_accounts → []
- [x] 5 tests pass

## PR-3: eth_estimateGas ✅ 完成 2026-04-26

- [x] Add `EstimateGas` to `jsonrpc.Backend`; implement via TriggerConstantContract
- [x] Handler + test; 28 packages green

## PR-4: Filter subsystem ✅ 完成 2026-04-26

- [x] `internal/jsonrpc/filter.go`: FilterManager, NewLogFilter, NewBlockFilter,
  UninstallFilter, GetFilterChanges, GetFilterLogs; 5min GC, 30s tick
- [x] `SubscribeBlocks`/`UnsubscribeBlocks` on Backend + TronBackend via AddBlockHook
- [x] `blockchain.go`: AddBlockHook + fanOut in InsertBlock
- [x] FilterManager wired into API struct; started in NewAPI
- [x] 5 dispatch cases added; 7 filter tests pass
- [x] 28 packages green
