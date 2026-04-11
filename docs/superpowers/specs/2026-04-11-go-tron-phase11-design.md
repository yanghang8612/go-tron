# Phase 11: Ethereum-Compatible JSON-RPC API — Design Spec

**Date:** 2026-04-11

## Goal

Add an Ethereum-compatible JSON-RPC HTTP server at `:8545` so that EVM-aware clients (ethers.js, web3.py, contract SDKs, block explorers) can query the go-tron node using standard `eth_*` methods.

## Context

Phases 1–10 built a working go-tron node with block production, P2P, TVM/EVM, transaction lifecycle, governance, smart contract persistence, and a full TRON HTTP API at `:8090`. Port 8545 is already wired as a config flag (`--jsonrpc.port`) but has no server behind it. Phase 11 adds the server.

Scope: **read-only HTTP JSON-RPC only** (no WebSocket, no write operations like `eth_sendRawTransaction`).

## Architecture

**New package:** `internal/jsonrpc/` — three files, mirrors `internal/tronapi/` exactly.

**Files modified:** `core/tron_backend.go` adds ~10 new methods implementing the JSON-RPC Backend interface. `cmd/gtron/main.go` wires and starts the new server.

**No new packages** outside `internal/jsonrpc/`. No new dependencies.

## Files

| File | Change |
|---|---|
| `internal/jsonrpc/backend.go` | Backend interface + 8 response/filter types |
| `internal/jsonrpc/api.go` | JSON-RPC dispatcher + 15 method handlers |
| `internal/jsonrpc/server.go` | HTTP server (lifecycle: Start/Stop) |
| `internal/jsonrpc/api_test.go` | Unit tests with stub backend |
| `core/tron_backend.go` | Implement JSON-RPC Backend interface (new methods) |
| `cmd/gtron/main.go` | Instantiate + start JSON-RPC server |

## JSON-RPC Protocol

Standard JSON-RPC 2.0 over HTTP POST to `/`.

Request:
```json
{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}
```

Success response:
```json
{"jsonrpc":"2.0","result":"0x1a4","id":1}
```

Error response:
```json
{"jsonrpc":"2.0","error":{"code":-32601,"message":"method not found"},"id":1}
```

Standard error codes: `-32700` parse error, `-32600` invalid request, `-32601` method not found, `-32602` invalid params, `-32603` internal error.

Batch requests (array of requests) are supported — iterate and respond with array of responses.

## The 15 Methods

### Infrastructure

| Method | Return |
|---|---|
| `net_version` | Chain ID as decimal string (e.g. `"1"` or the configured chain ID) |
| `web3_clientVersion` | `"go-tron/v0.3.0-dev"` |
| `eth_chainId` | Chain ID as hex (e.g. `"0x2b6653dc"` for mainnet) |
| `eth_blockNumber` | Current block number as hex |
| `eth_syncing` | Always `false` |

### Account state

| Method | Params | Return |
|---|---|---|
| `eth_getBalance` | `[address, block]` | TRX balance in wei-equivalent (SUN × 10¹²) as hex |
| `eth_getTransactionCount` | `[address, block]` | Always `"0x0"` (TRON has no nonces) |
| `eth_getCode` | `[address, block]` | Contract bytecode as hex, `"0x"` for EOAs |
| `eth_getStorageAt` | `[address, slot, block]` | 32-byte storage value as hex |

### Execution

| Method | Params | Return |
|---|---|---|
| `eth_call` | `[txObject, block]` | Call result as hex (uses TVM simulation) |

### Block queries

| Method | Params | Return |
|---|---|---|
| `eth_getBlockByNumber` | `[blockNum, fullTx]` | Block object or `null` |
| `eth_getBlockByHash` | `[hash, fullTx]` | Block object or `null` |

### Transaction queries

| Method | Params | Return |
|---|---|---|
| `eth_getTransactionByHash` | `[hash]` | Transaction object or `null` |
| `eth_getTransactionReceipt` | `[hash]` | Receipt object or `null` |

### Log queries

| Method | Params | Return |
|---|---|---|
| `eth_getLogs` | `[filterObject]` | Array of log objects |

## Data Mapping: TRON → Ethereum

### Addresses

TRON addresses are 21 bytes with a `0x41` prefix byte. Ethereum addresses are 20 bytes.

- TRON → Ethereum: strip the first byte: `tronAddr[1:21]`
- Ethereum → TRON: prepend `0x41`: `[0x41] + ethAddr`
- The `common.Address` type in go-tron is already 20 bytes (EVM-native)

### Balance

TRON stores TRX in SUN (1 TRX = 10⁶ SUN). Ethereum uses wei (1 ETH = 10¹⁸ wei).

`eth_getBalance` returns: `balanceSUN × 10¹²` as a hex-encoded `*big.Int` (makes 1 TRX appear as 1 ETH to tooling — matching java-tron's JSON-RPC implementation).

**Important:** The multiplication must use `*big.Int` — `int64 × 10¹²` overflows for accounts holding >9.2 million TRX (which includes genesis accounts). Implementation: `new(big.Int).Mul(big.NewInt(balanceSUN), big.NewInt(1_000_000_000_000))`.

### Block Object

```go
type RPCBlock struct {
    Hash             string        `json:"hash"`
    ParentHash       string        `json:"parentHash"`
    Number           string        `json:"number"`           // hex
    Timestamp        string        `json:"timestamp"`        // hex seconds
    Miner            string        `json:"miner"`            // witness address
    Difficulty       string        `json:"difficulty"`       // "0x0"
    TotalDifficulty  string        `json:"totalDifficulty"`  // "0x0"
    ExtraData        string        `json:"extraData"`        // "0x"
    Size             string        `json:"size"`             // hex
    GasLimit         string        `json:"gasLimit"`         // energy limit as hex
    GasUsed          string        `json:"gasUsed"`          // energy used as hex
    Nonce            string        `json:"nonce"`            // "0x0000000000000000"
    Sha3Uncles       string        `json:"sha3Uncles"`       // zero hash
    LogsBloom        string        `json:"logsBloom"`        // 256-byte zero bloom
    TransactionsRoot string        `json:"transactionsRoot"` // "0x" + block tx root
    StateRoot        string        `json:"stateRoot"`        // "0x" + account state root
    ReceiptsRoot     string        `json:"receiptsRoot"`     // "0x"
    Uncles           []string      `json:"uncles"`           // []
    Transactions     interface{}   `json:"transactions"`     // []string hashes or []RPCTransaction
}
```

### Transaction Object

```go
type RPCTransaction struct {
    Hash             string `json:"hash"`
    BlockHash        string `json:"blockHash"`
    BlockNumber      string `json:"blockNumber"`      // hex
    TransactionIndex string `json:"transactionIndex"` // hex
    From             string `json:"from"`             // 20-byte hex
    To               string `json:"to"`               // 20-byte hex, null for deploy
    Value            string `json:"value"`            // TRX amount in wei-equiv hex
    Gas              string `json:"gas"`              // fee limit as hex
    GasPrice         string `json:"gasPrice"`         // "0x1"
    Input            string `json:"input"`            // call data hex
    Nonce            string `json:"nonce"`            // "0x0"
    Type             string `json:"type"`             // "0x0"
    V                string `json:"v"`                // "0x0"
    R                string `json:"r"`                // "0x0"
    S                string `json:"s"`                // "0x0"
}
```

For non-EVM transactions (transfers, governance, etc.), `to` is the recipient, `input` is `"0x"`, `value` is the amount.
For `TriggerSmartContract`: `to` is contract address, `input` is call data.
For `CreateSmartContract`: `to` is `null`, `input` is deploy bytecode.

### Receipt Object

```go
type RPCReceipt struct {
    TransactionHash   string      `json:"transactionHash"`
    TransactionIndex  string      `json:"transactionIndex"` // hex
    BlockHash         string      `json:"blockHash"`
    BlockNumber       string      `json:"blockNumber"`      // hex
    From              string      `json:"from"`
    To                string      `json:"to"`               // null for deploy
    CumulativeGasUsed string      `json:"cumulativeGasUsed"` // hex
    GasUsed           string      `json:"gasUsed"`           // hex energy used
    ContractAddress   string      `json:"contractAddress"`   // null or deployed addr
    Logs              []*RPCLog   `json:"logs"`
    LogsBloom         string      `json:"logsBloom"`         // "0x" + 256 zero bytes
    Status            string      `json:"status"`            // "0x1" success, "0x0" fail
    Type              string      `json:"type"`              // "0x0"
}
```

### Log Object

```go
type RPCLog struct {
    Address          string   `json:"address"`         // contract address, 20-byte hex
    Topics           []string `json:"topics"`          // []32-byte hex
    Data             string   `json:"data"`            // hex
    BlockNumber      string   `json:"blockNumber"`     // hex
    TransactionHash  string   `json:"transactionHash"`
    TransactionIndex string   `json:"transactionIndex"` // hex
    BlockHash        string   `json:"blockHash"`
    LogIndex         string   `json:"logIndex"`        // hex
    Removed          bool     `json:"removed"`         // always false
}
```

## Backend Interface

```go
// In internal/jsonrpc/backend.go

type LogFilter struct {
    FromBlock *uint64
    ToBlock   *uint64
    BlockHash *common.Hash
    Addresses []common.Address
    Topics    [][]common.Hash // topics[i] = required values for position i; nil = any
}

type Backend interface {
    // Chain info
    ChainID() uint64
    BlockNumber() uint64

    // Block queries
    GetBlockByNumber(num uint64) (*types.Block, error)
    GetBlockByHash(hash common.Hash) (*types.Block, error)

    // Account state (always uses current/latest block — block param is parsed by handler but ignored)
    GetBalance(addr common.Address) int64          // returns SUN; handler multiplies by 1e12 using big.Int
    GetCode(addr common.Address) []byte
    GetStorageAt(addr common.Address, slot common.Hash) common.Hash

    // Transaction queries
    GetTransactionByHash(hash common.Hash) (*corepb.Transaction, *types.Block, int, error)
    GetTransactionInfo(hash common.Hash) (*corepb.TransactionInfo, error)

    // EVM execution
    Call(from, to *common.Address, data []byte, value int64) ([]byte, error)

    // Log queries
    GetLogs(filter LogFilter) ([]*RPCLog, error)
}
```

## JSON-RPC Backend Implementation (core/tron_backend.go)

New methods on `TronBackend`:

- `ChainID()` — returns `b.chain.Config().ChainID`
- `BlockNumber()` — returns `b.chain.CurrentBlock().Number()`
- `GetBlockByNumber(num)` — `b.chain.GetBlockByNumber(num)`
- `GetBlockByHash(hash)` — `b.chain.GetBlockByHash(hash)`
- `GetBalance(addr)` — open state, call `statedb.GetBalance(addr)` (returns int64 SUN)
- `GetCode(addr)` — open state, call `statedb.GetCode(addr)`
- `GetStorageAt(addr, slot)` — open state, call `statedb.GetState(addr, slot)`
- `GetTransactionByHash(hash)` — scan recent blocks for tx; returns tx + block + index
- `GetTransactionInfo(hash)` — `rawdb.ReadTransactionInfo(db, hash)`
- `Call(from, to, data, value)` — reuse TVM simulation path (same as `TriggerConstantContract`)
- `GetLogs(filter)` — scan blocks in range, collect logs from TransactionInfos

## GetTransactionByHash Implementation

TRON does not store a tx→block index. The current implementation in the TRON HTTP API uses a block scan approach or relies on TransactionInfo (which stores the block number). Strategy:

1. Try `rawdb.ReadTransactionInfo(db, hash)` to get the block number
2. Load the block at that number
3. Find the transaction in the block by scanning tx IDs
4. Return tx + block + index

If TransactionInfo is not found, return nil (tx not found).

## eth_getLogs Implementation

```
filter = {fromBlock, toBlock, blockHash, addresses, topics}
```

- If `blockHash` is set: scan only that block
- Otherwise: scan `[fromBlock, toBlock]`, capped at 2000 blocks max (return error if range > 2000)
- For each block, get all TransactionInfos (via rawdb)
- Extract logs from each TransactionInfo, convert to RPCLog
- Filter by address (if filter.Addresses non-empty)
- Filter by topics (if filter.Topics non-empty, each position must match)

## eth_call Implementation

Reuse the existing `TriggerConstantContract` simulation logic, which already invokes the TVM with a read-only call. The `eth_call` request object maps to:
- `from` → owner address
- `to` → contract address
- `data` → call data (function selector + ABI-encoded args)
- `value` → TRX amount

Returns the raw output bytes as hex.

## Server Structure

`internal/jsonrpc/server.go` — same structure as `internal/tronapi/server.go`:

```go
type Server struct {
    backend  Backend
    port     int
    httpSrv  *http.Server
}

func NewServer(backend Backend, port int) *Server
func (s *Server) Start() error
func (s *Server) Stop() error
```

`TronBackend` (already serving the TRON HTTP API) implements both the TRON HTTP Backend interface and the JSON-RPC Backend interface. Adding the JSON-RPC methods directly to `TronBackend` keeps all state-access in one place and avoids a wrapper type.

`main.go` wiring:
```go
jrpcServer := jsonrpc.NewServer(backend, cfg.JSONRPCPort)
stack.RegisterLifecycle(jrpcServer)
```

Same `backend` object serves both APIs (`:8090` TRON HTTP and `:8545` JSON-RPC).

## API Dispatcher Structure

```go
// internal/jsonrpc/api.go

type API struct {
    backend Backend
}

type rpcRequest struct {
    JSONRPC string          `json:"jsonrpc"`
    Method  string          `json:"method"`
    Params  json.RawMessage `json:"params"`
    ID      json.RawMessage `json:"id"`
}

type rpcResponse struct {
    JSONRPC string          `json:"jsonrpc"`
    Result  interface{}     `json:"result,omitempty"`
    Error   *rpcError       `json:"error,omitempty"`
    ID      json.RawMessage `json:"id"`
}

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request)
func (api *API) dispatch(req rpcRequest) rpcResponse
```

## block param parsing

Many methods accept a "block parameter": a block number as hex, or `"latest"`, `"earliest"`, `"pending"`. Implementation:
- `"latest"` or omitted → current block
- `"earliest"` → block 0
- `"pending"` → current block (TRON doesn't have a pending block concept)
- `"0x1a4"` → block 420

## Testing

`internal/jsonrpc/api_test.go` — stub backend, one test per method. Tests use `httptest.NewServer` with the API handler. Each test:
1. Sends a JSON-RPC POST
2. Asserts the response has `"result"` (not `"error"`)
3. Checks the result shape

`core/tron_backend_test.go` — unit tests for the new Backend methods (GetBalance, GetCode, GetStorageAt, GetLogs).

System test: Section 10 in `scripts/system_test.sh` — curl-based JSON-RPC calls against a running node, checking for non-error responses.

## No New External Dependencies

Uses only Go standard library: `encoding/json`, `net/http`, `fmt`, `strconv`, `strings`. No third-party JSON-RPC library.

## Error Handling

- Parse errors → code `-32700`
- Method not found → code `-32601`  
- Invalid params (missing required param, wrong type) → code `-32602`
- Internal error (state open failed, etc.) → code `-32603`
- `null` result for not-found resources (tx/block) is a success, not an error
