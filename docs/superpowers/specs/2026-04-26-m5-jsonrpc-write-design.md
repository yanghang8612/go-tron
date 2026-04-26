# M5.2 JSON-RPC 写路径 + Filter 系统设计

**Date**: 2026-04-26  
**Author**: Claude Sonnet 4.6  
**Plan**: `docs/superpowers/plans/2026-04-26-m5-jsonrpc-write.md`

## 1. Background

The existing `internal/jsonrpc/` package implements 15 read-only Ethereum JSON-RPC methods.
M5.2 fills three remaining gaps:

| Group | Methods | Notes |
|-------|---------|-------|
| Utility | `eth_gasPrice`, `web3_sha3`, `net_listening`, `net_peerCount` | Stateless one-liners |
| Write stubs | `eth_sendRawTransaction`, `eth_sendTransaction`, `eth_sign`, `eth_signTransaction`, `eth_accounts` | java-tron returns -32601 for all write methods; we match exactly |
| Filter subsystem | `eth_newFilter`, `eth_newBlockFilter`, `eth_uninstallFilter`, `eth_getFilterChanges`, `eth_getFilterLogs` | Stateful; new file |
| eth_estimateGas | `eth_estimateGas` | Reuses backend `Call`; returns energy units as hex |

## 2. java-tron Reference Behavior

From `TronJsonRpcImpl.java`:

| Method | java-tron behavior |
|--------|-------------------|
| `eth_gasPrice` | `toJsonHex(wallet.getEnergyFee())` — dynamic property `energy_fee` |
| `net_listening` | `activeConnectCount >= 1` (boolean) |
| `net_peerCount` | `toJsonHex(nodeInfoService.getNodeInfo().getPeerList().size())` |
| `eth_accounts` | Returns `new String[0]` — empty array |
| `eth_sendRawTransaction` | Throws "method not found" (-32601) |
| `eth_sendTransaction` | Throws "method not found" (-32601) |
| `eth_sign` | Throws "method not found" (-32601) |
| `eth_signTransaction` | Throws "method not found" (-32601) |
| `eth_estimateGas` | Runs TVM simulation; returns `"0x0"` for plain transfers |
| Filter methods | Full in-memory filter map, disabled in PBFT mode |

## 3. Backend Interface Additions

New methods to add to `jsonrpc.Backend`:

```go
// GasPrice returns the current energy fee (SUN per energy unit).
GasPrice() int64

// PeerCount returns the number of connected peers.
PeerCount() int

// EstimateGas simulates contract execution and returns energy used.
EstimateGas(from, to *common.Address, data []byte, value int64) (uint64, error)

// SubscribeBlocks sends new blocks to ch; caller must call UnsubscribeBlocks to stop.
SubscribeBlocks(ch chan<- *types.Block)
UnsubscribeBlocks(ch chan<- *types.Block)
```

## 4. Utility Methods (PR-1)

### eth_gasPrice
```
returns hexUint64(backend.GasPrice())
```
Backend implementation: `state.LoadDynamicProperties(db).EnergyFee()`

### web3_sha3
Params: `[hexData]` — compute keccak256.
```
input := common.FromHex(params[0])
return hexBytes(crypto.Keccak256(input))
```

### net_listening
```
returns backend.PeerCount() >= 1  // JSON boolean
```

### net_peerCount
```
returns hexUint64(uint64(backend.PeerCount()))
```

## 5. Write Stubs (PR-2)

All write methods return JSON-RPC error code -32601 ("method not found") matching java-tron.
`eth_accounts` returns `[]string{}`.
No backend changes needed.

## 6. eth_estimateGas (PR-3)

Params: `[{from, to, data, value, gas}, blockTag]`

java-tron behavior:
- For `TransferContract`: returns `"0x0"` (no energy cost for plain TRX transfer)
- For `TriggerSmartContract`/`CreateSmartContract`: runs TVM simulation, returns energy used

Simplified implementation:
- Reuse `backend.Call(from, to, data, value)` path — gas returned is energy used
- For plain transfer (no data, non-zero value): return `"0x0"`
- Otherwise: call EstimateGas → return hex

Backend `EstimateGas` delegates to the same TVM simulation as `Call` but returns energy accounting.

## 7. Filter Subsystem (PR-4)

### File: `internal/jsonrpc/filter.go`

```go
type filterKind int
const (
    filterLog filterKind = iota
    filterBlock
)

type filter struct {
    kind      filterKind
    logFilter *LogFilter      // non-nil for filterLog
    lastPoll  time.Time
    pendingLogs []*RPCLog     // accumulated since last getFilterChanges
    pendingHashes []string    // for filterBlock
}

type FilterManager struct {
    mu      sync.Mutex
    filters map[string]*filter
    backend Backend
    quit    chan struct{}
}
```

### Filter ID
`generateFilterID()` → 32 random bytes → hex string.

### Block subscriptions
`FilterManager.Start()` calls `backend.SubscribeBlocks(ch)` and runs a goroutine:
- On new block: fan-out to all `filterBlock` filters (append hash) and `filterLog` filters (match logs, append matching)
- On ticker (every 30s): GC filters idle >5 min (matching java-tron)

### Methods

| Method | Behavior |
|--------|---------|
| `eth_newFilter` | Create filterLog, store LogFilter, return hex ID |
| `eth_newBlockFilter` | Create filterBlock, return hex ID |
| `eth_uninstallFilter` | Remove filter or return error -32000 |
| `eth_getFilterChanges` | Pop accumulated hashes/logs, reset pendingXxx, update lastPoll |
| `eth_getFilterLogs` | Re-query all logs for filter's LogFilter (ignores accumulated) |

### Backend block subscription
`TronBackend` implements `SubscribeBlocks`/`UnsubscribeBlocks` using a slice of subscriber channels,
fed from `BlockChain.InsertBlock` via a non-blocking send.

## 8. Dispatch

Add all new methods to the `dispatch` switch in `api.go`. Pass `FilterManager` to `API` struct.

## 9. PR Sequencing

| PR | Scope | New Backend methods |
|----|-------|---------------------|
| PR-1 | Utility (gasPrice, sha3, listening, peerCount) | `GasPrice()`, `PeerCount()` |
| PR-2 | Write stubs + eth_accounts | None |
| PR-3 | eth_estimateGas | `EstimateGas()` |
| PR-4 | Filter subsystem | `SubscribeBlocks()`, `UnsubscribeBlocks()` |

## 10. Exit Criteria

- All 28 packages green with `go test ./...`
- Manual smoke (deferred, requires running node): `curl` to gtron JSON-RPC port
- MetaMask / Hardhat integration validation deferred — not a CI gate for M5.2 completion
