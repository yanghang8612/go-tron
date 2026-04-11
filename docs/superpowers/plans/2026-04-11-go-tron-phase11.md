# Phase 11: Ethereum-Compatible JSON-RPC API — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add an Ethereum-compatible JSON-RPC HTTP server at `:8545` exposing 15 read-only `eth_*`/`net_*`/`web3_*` methods so contract SDKs, ethers.js, and block explorers can query the go-tron node.

**Architecture:** New `internal/jsonrpc/` package (server.go, backend.go, api.go, api_test.go) mirroring `internal/tronapi/`. `TronBackend` in `core/tron_backend.go` implements the new JSON-RPC Backend interface by adding 9 new methods. `cmd/gtron/main.go` starts the second server on `--jsonrpc.port` (default 8545) alongside the existing TRON HTTP server.

**Tech Stack:** Go standard library only — `encoding/json`, `net/http`, `math/big`, `fmt`, `strconv`. No third-party JSON-RPC library.

---

## File Map

| File | What changes |
|---|---|
| `internal/jsonrpc/backend.go` | **New** — Backend interface + LogFilter + RPCLog types |
| `internal/jsonrpc/api.go` | **New** — JSON-RPC dispatcher + 15 method handlers + conversion helpers |
| `internal/jsonrpc/server.go` | **New** — HTTP server lifecycle (Start/Stop), mirrors tronapi/server.go |
| `internal/jsonrpc/api_test.go` | **New** — stub backend + 16 test functions |
| `core/tron_backend.go` | Add 9 new methods implementing `jsonrpc.Backend` |
| `cmd/gtron/main.go` | Instantiate + start JSON-RPC server |

---

## Task 1: Package Scaffold — Three New Files, TronBackend Stubs, main.go Wiring

**Files:**
- Create: `internal/jsonrpc/server.go`
- Create: `internal/jsonrpc/backend.go`
- Create: `internal/jsonrpc/api.go`
- Modify: `core/tron_backend.go`
- Modify: `cmd/gtron/main.go`

- [ ] **Step 1: Create `internal/jsonrpc/server.go`**

```go
package jsonrpc

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// Server runs the Ethereum-compatible JSON-RPC HTTP server.
type Server struct {
	httpServer *http.Server
	api        *API
}

// NewServer creates a JSON-RPC server on the given port.
func NewServer(backend Backend, port int) *Server {
	api := &API{backend: backend}
	return &Server{
		httpServer: &http.Server{
			Addr:    fmt.Sprintf(":%d", port),
			Handler: api,
		},
		api: api,
	}
}

func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.httpServer.Addr)
	if err != nil {
		return fmt.Errorf("jsonrpc listen: %w", err)
	}
	go s.httpServer.Serve(ln)
	return nil
}

func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.httpServer.Shutdown(ctx)
}
```

- [ ] **Step 2: Create `internal/jsonrpc/backend.go`**

```go
package jsonrpc

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// LogFilter selects logs across a block range with optional address/topic constraints.
type LogFilter struct {
	FromBlock *uint64
	ToBlock   *uint64
	BlockHash *common.Hash
	Addresses []common.Address
	Topics    [][]common.Hash // Topics[i] = required hashes for position i; nil = any
}

// RPCLog is an Ethereum-format event log entry.
type RPCLog struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	BlockHash        string   `json:"blockHash"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// Backend is the data-access interface for the JSON-RPC API.
// Implemented by core.TronBackend.
type Backend interface {
	// Chain metadata
	ChainID() int64
	BlockNumber() uint64

	// Block queries — same signatures as tronapi.Backend, already on TronBackend
	GetBlockByNumber(num uint64) (*types.Block, error)
	GetBlockByHash(hash common.Hash) (*types.Block, error)

	// Account state (always reads latest/current state)
	GetBalance(addr common.Address) int64 // returns SUN; handler multiplies by 1e12
	GetCode(addr common.Address) []byte
	GetStorageAt(addr common.Address, slot common.Hash) common.Hash

	// Transaction queries
	// GetTransactionByHash returns the raw proto tx, its containing block, and its index within the block.
	// Returns (nil, nil, 0, nil) when not found.
	GetTransactionByHash(hash common.Hash) (*corepb.Transaction, *types.Block, int, error)
	// GetTransactionInfo returns the execution receipt stored by the blockchain.
	// Returns (nil, nil) when not found.
	GetTransactionInfo(hash common.Hash) (*corepb.TransactionInfo, error)

	// EVM execution (read-only simulation)
	// from and to may be nil; nil from uses zero address; nil to returns error.
	Call(from, to *common.Address, data []byte, value int64) ([]byte, error)

	// Log queries
	GetLogs(filter LogFilter) ([]*RPCLog, error)
}
```

- [ ] **Step 3: Create `internal/jsonrpc/api.go` — dispatcher + 15 stub handlers + hex helpers**

```go
package jsonrpc

import (
	"encoding/json"
	"fmt"
	"net/http"
)

// API implements http.Handler and dispatches JSON-RPC 2.0 requests.
type API struct {
	backend Backend
}

// ── JSON-RPC protocol types ────────────────────────────────────────────────

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

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternal       = -32603
)

// ── HTTP handler ────────────────────────────────────────────────────────────

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		json.NewEncoder(w).Encode(errResp(nil, codeParseError, "parse error"))
		return
	}

	// Batch vs. single request
	if len(raw) > 0 && raw[0] == '[' {
		var reqs []rpcRequest
		if err := json.Unmarshal(raw, &reqs); err != nil {
			json.NewEncoder(w).Encode(errResp(nil, codeParseError, "parse error"))
			return
		}
		responses := make([]rpcResponse, len(reqs))
		for i, req := range reqs {
			responses[i] = api.dispatch(req)
		}
		json.NewEncoder(w).Encode(responses)
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		json.NewEncoder(w).Encode(errResp(nil, codeParseError, "parse error"))
		return
	}
	json.NewEncoder(w).Encode(api.dispatch(req))
}

func errResp(id json.RawMessage, code int, msg string) rpcResponse {
	if id == nil {
		id = json.RawMessage("null")
	}
	return rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: code, Message: msg}, ID: id}
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

func (api *API) dispatch(req rpcRequest) rpcResponse {
	id := req.ID
	if id == nil {
		id = json.RawMessage("null")
	}

	var (
		result interface{}
		err    error
	)
	switch req.Method {
	case "net_version":
		result, err = api.netVersion(req.Params)
	case "web3_clientVersion":
		result, err = api.web3ClientVersion(req.Params)
	case "eth_chainId":
		result, err = api.ethChainId(req.Params)
	case "eth_blockNumber":
		result, err = api.ethBlockNumber(req.Params)
	case "eth_syncing":
		result, err = api.ethSyncing(req.Params)
	case "eth_getBalance":
		result, err = api.ethGetBalance(req.Params)
	case "eth_getTransactionCount":
		result, err = api.ethGetTransactionCount(req.Params)
	case "eth_getCode":
		result, err = api.ethGetCode(req.Params)
	case "eth_getStorageAt":
		result, err = api.ethGetStorageAt(req.Params)
	case "eth_call":
		result, err = api.ethCall(req.Params)
	case "eth_getBlockByNumber":
		result, err = api.ethGetBlockByNumber(req.Params)
	case "eth_getBlockByHash":
		result, err = api.ethGetBlockByHash(req.Params)
	case "eth_getTransactionByHash":
		result, err = api.ethGetTransactionByHash(req.Params)
	case "eth_getTransactionReceipt":
		result, err = api.ethGetTransactionReceipt(req.Params)
	case "eth_getLogs":
		result, err = api.ethGetLogs(req.Params)
	default:
		return errResp(id, codeMethodNotFound, "method not found")
	}

	if err != nil {
		return errResp(id, codeInternal, err.Error())
	}
	return rpcResponse{JSONRPC: "2.0", Result: result, ID: id}
}

// ── Hex helpers ──────────────────────────────────────────────────────────────

// hexUint64 formats n as "0x<hex>".
func hexUint64(n uint64) string { return fmt.Sprintf("0x%x", n) }

// hexBytes formats b as "0x<hex>". Returns "0x" for nil/empty.
func hexBytes(b []byte) string {
	if len(b) == 0 {
		return "0x"
	}
	return fmt.Sprintf("0x%x", b)
}

// hex20 formats a 20-byte address slice as "0x<40 hex chars>".
func hex20(b []byte) string {
	if len(b) < 20 {
		return "0x0000000000000000000000000000000000000000"
	}
	return fmt.Sprintf("0x%x", b[len(b)-20:])
}

// hex32 formats a 32-byte hash as "0x<64 hex chars>".
func hex32(b []byte) string {
	if len(b) == 0 {
		return "0x0000000000000000000000000000000000000000000000000000000000000000"
	}
	return fmt.Sprintf("0x%064x", b)
}

// parseBlockParam converts a block tag ("latest", "earliest", "pending", "0x1") to uint64.
// Returns math.MaxUint64 as sentinel for "latest"/"pending".
func parseBlockParam(s string) uint64 {
	switch s {
	case "", "latest", "pending":
		return ^uint64(0)
	case "earliest":
		return 0
	default:
		if len(s) > 2 && s[:2] == "0x" {
			n, _ := strconv.ParseUint(s[2:], 16, 64)
			return n
		}
		n, _ := strconv.ParseUint(s, 10, 64)
		return n
	}
}

// ── Stub handlers (replaced task by task) ───────────────────────────────────

func (api *API) netVersion(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) web3ClientVersion(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethChainId(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethBlockNumber(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethSyncing(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetBalance(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetTransactionCount(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetCode(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetStorageAt(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethCall(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetBlockByNumber(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetBlockByHash(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetTransactionByHash(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetTransactionReceipt(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetLogs(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
```

**Note:** `api.go` is missing `"strconv"` import — add it to the import block. The full import block for `api.go`:

```go
import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
)
```

- [ ] **Step 4: Add 9 stub methods to `core/tron_backend.go`**

Add `"github.com/tronprotocol/go-tron/internal/jsonrpc"` to the imports in `core/tron_backend.go`.

Then add these stubs at the end of the file:

```go
// ── JSON-RPC Backend implementation (Phase 11) ────────────────────────────

func (b *TronBackend) ChainID() int64 {
	return 0 // stub
}

func (b *TronBackend) BlockNumber() uint64 {
	return 0 // stub
}

func (b *TronBackend) GetBalance(addr tcommon.Address) int64 {
	return 0 // stub
}

func (b *TronBackend) GetCode(addr tcommon.Address) []byte {
	return nil // stub
}

func (b *TronBackend) GetStorageAt(addr tcommon.Address, slot tcommon.Hash) tcommon.Hash {
	return tcommon.Hash{} // stub
}

func (b *TronBackend) GetTransactionByHash(hash tcommon.Hash) (*corepb.Transaction, *types.Block, int, error) {
	return nil, nil, 0, nil // stub: not found
}

func (b *TronBackend) GetTransactionInfo(hash tcommon.Hash) (*corepb.TransactionInfo, error) {
	return nil, nil // stub: not found
}

func (b *TronBackend) Call(from, to *tcommon.Address, data []byte, value int64) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetLogs(filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, error) {
	return nil, fmt.Errorf("not implemented")
}
```

**Note:** `GetBlockByNumber` and `GetBlockByHash` are already implemented on `TronBackend` (lines 53, 177) — they satisfy the `jsonrpc.Backend` interface automatically. Do NOT add duplicate implementations.

- [ ] **Step 5: Wire the JSON-RPC server in `cmd/gtron/main.go`**

Add the import:
```go
"github.com/tronprotocol/go-tron/internal/jsonrpc"
```

After the line `apiServer := tronapi.NewServer(backend, cfg.HTTPPort)`, add:
```go
jrpcServer := jsonrpc.NewServer(backend, cfg.JSONRPCPort)
```

After `stack.RegisterLifecycle(apiServer)`, add:
```go
stack.RegisterLifecycle(jrpcServer)
```

The config already has a `JSONRPCPort` field in `params.ChainConfig` (default 8545) — no new flag needed.

- [ ] **Step 6: Verify compilation**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go build ./...
```

Expected: exits 0, no errors.

- [ ] **Step 7: Commit**

```bash
git add internal/jsonrpc/server.go internal/jsonrpc/backend.go internal/jsonrpc/api.go \
    core/tron_backend.go cmd/gtron/main.go
git commit -m "feat(phase11): add JSON-RPC package scaffold with dispatcher and 15 stub handlers"
```

---

## Task 2: Create Test File with Stub Backend and 16 Failing Tests

**Files:**
- Create: `internal/jsonrpc/api_test.go`

- [ ] **Step 1: Create `internal/jsonrpc/api_test.go`**

```go
package jsonrpc_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// stubBackend is a test double implementing jsonrpc.Backend.
type stubBackend struct {
	chainID     int64
	blockNumber uint64
	block       *types.Block
	balance     int64
	code        []byte
	storage     common.Hash
	tx          *corepb.Transaction
	txBlock     *types.Block
	txIndex     int
	txInfo      *corepb.TransactionInfo
	callResult  []byte
	logs        []*jsonrpc.RPCLog
}

func (s *stubBackend) ChainID() int64       { return s.chainID }
func (s *stubBackend) BlockNumber() uint64   { return s.blockNumber }
func (s *stubBackend) GetBalance(addr common.Address) int64 { return s.balance }
func (s *stubBackend) GetCode(addr common.Address) []byte   { return s.code }
func (s *stubBackend) GetStorageAt(addr common.Address, slot common.Hash) common.Hash {
	return s.storage
}
func (s *stubBackend) GetBlockByNumber(num uint64) (*types.Block, error) { return s.block, nil }
func (s *stubBackend) GetBlockByHash(hash common.Hash) (*types.Block, error) { return s.block, nil }
func (s *stubBackend) GetTransactionByHash(hash common.Hash) (*corepb.Transaction, *types.Block, int, error) {
	return s.tx, s.txBlock, s.txIndex, nil
}
func (s *stubBackend) GetTransactionInfo(hash common.Hash) (*corepb.TransactionInfo, error) {
	return s.txInfo, nil
}
func (s *stubBackend) Call(from, to *common.Address, data []byte, value int64) ([]byte, error) {
	return s.callResult, nil
}
func (s *stubBackend) GetLogs(filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, error) {
	return s.logs, nil
}

// ── Test helpers ─────────────────────────────────────────────────────────────

func newTestServer(t *testing.T, backend jsonrpc.Backend) *httptest.Server {
	t.Helper()
	api := &jsonrpc.API{}
	// API is not exported with a constructor in the scaffold; use NewServer's handler approach.
	// We expose the API via the server's handler directly.
	srv := jsonrpc.NewServer(backend, 0)
	return httptest.NewServer(srv.Handler())
}

func rpcCall(t *testing.T, srv *httptest.Server, method string, params interface{}) map[string]interface{} {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", method, err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode response for %s: %v", method, err)
	}
	if errField, hasErr := result["error"]; hasErr {
		t.Fatalf("%s returned error: %v", method, errField)
	}
	return result
}

// ── Tests ─────────────────────────────────────────────────────────────────────

func TestNetVersion(t *testing.T) {
	srv := newTestServer(t, &stubBackend{chainID: 1})
	defer srv.Close()
	resp := rpcCall(t, srv, "net_version", []interface{}{})
	if _, ok := resp["result"].(string); !ok {
		t.Fatalf("net_version result should be a string, got %v", resp["result"])
	}
}

func TestWeb3ClientVersion(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	resp := rpcCall(t, srv, "web3_clientVersion", []interface{}{})
	if _, ok := resp["result"].(string); !ok {
		t.Fatalf("web3_clientVersion result should be a string, got %v", resp["result"])
	}
}

func TestEthChainId(t *testing.T) {
	srv := newTestServer(t, &stubBackend{chainID: 728126428})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_chainId", []interface{}{})
	if _, ok := resp["result"].(string); !ok {
		t.Fatalf("eth_chainId result should be a hex string, got %v", resp["result"])
	}
}

func TestEthBlockNumber(t *testing.T) {
	srv := newTestServer(t, &stubBackend{blockNumber: 100})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_blockNumber", []interface{}{})
	if resp["result"] != "0x64" {
		t.Fatalf("eth_blockNumber: expected 0x64, got %v", resp["result"])
	}
}

func TestEthSyncing(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_syncing", []interface{}{})
	if resp["result"] != false {
		t.Fatalf("eth_syncing should return false, got %v", resp["result"])
	}
}

func TestEthGetBalance(t *testing.T) {
	// 1_000_000 SUN × 1e12 = 1e18 = "0xde0b6b3a7640000"
	srv := newTestServer(t, &stubBackend{balance: 1_000_000})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getBalance", []interface{}{"0x4101020304050607080900010203040506070809", "latest"})
	if _, ok := resp["result"].(string); !ok {
		t.Fatalf("eth_getBalance result should be a hex string, got %v", resp["result"])
	}
}

func TestEthGetTransactionCount(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getTransactionCount", []interface{}{"0x4101020304050607080900010203040506070809", "latest"})
	if resp["result"] != "0x0" {
		t.Fatalf("eth_getTransactionCount should always be 0x0, got %v", resp["result"])
	}
}

func TestEthGetCode(t *testing.T) {
	srv := newTestServer(t, &stubBackend{code: []byte{0x60, 0x80}})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getCode", []interface{}{"0x4101020304050607080900010203040506070809", "latest"})
	if resp["result"] != "0x6080" {
		t.Fatalf("eth_getCode: expected 0x6080, got %v", resp["result"])
	}
}

func TestEthGetStorageAt(t *testing.T) {
	slot := common.Hash{1}
	srv := newTestServer(t, &stubBackend{storage: slot})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getStorageAt", []interface{}{
		"0x4101020304050607080900010203040506070809",
		"0x0",
		"latest",
	})
	if _, ok := resp["result"].(string); !ok {
		t.Fatalf("eth_getStorageAt result should be a hex string, got %v", resp["result"])
	}
}

func TestEthCall(t *testing.T) {
	// stub returns 32-byte ABI-encoded uint256(1)
	ret := make([]byte, 32)
	ret[31] = 1
	srv := newTestServer(t, &stubBackend{callResult: ret})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_call", []interface{}{
		map[string]interface{}{
			"to":   "0x4101020304050607080900010203040506070809",
			"data": "0x70a08231000000000000000000000000000000000000000000000000000000000000000",
		},
		"latest",
	})
	if _, ok := resp["result"].(string); !ok {
		t.Fatalf("eth_call result should be a hex string, got %v", resp["result"])
	}
}

func TestEthGetBlockByNumber_NotFound(t *testing.T) {
	srv := newTestServer(t, &stubBackend{block: nil})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getBlockByNumber", []interface{}{"0x9999999", false})
	if resp["result"] != nil {
		t.Fatalf("eth_getBlockByNumber not-found should return null, got %v", resp["result"])
	}
}

func TestEthGetBlockByHash_NotFound(t *testing.T) {
	srv := newTestServer(t, &stubBackend{block: nil})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getBlockByHash", []interface{}{
		"0x0000000000000000000000000000000000000000000000000000000000000000",
		false,
	})
	if resp["result"] != nil {
		t.Fatalf("eth_getBlockByHash not-found should return null, got %v", resp["result"])
	}
}

func TestEthGetTransactionByHash_NotFound(t *testing.T) {
	srv := newTestServer(t, &stubBackend{tx: nil})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getTransactionByHash", []interface{}{
		"0x0000000000000000000000000000000000000000000000000000000000000000",
	})
	if resp["result"] != nil {
		t.Fatalf("eth_getTransactionByHash not-found should return null, got %v", resp["result"])
	}
}

func TestEthGetTransactionReceipt_NotFound(t *testing.T) {
	srv := newTestServer(t, &stubBackend{txInfo: nil})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getTransactionReceipt", []interface{}{
		"0x0000000000000000000000000000000000000000000000000000000000000000",
	})
	if resp["result"] != nil {
		t.Fatalf("eth_getTransactionReceipt not-found should return null, got %v", resp["result"])
	}
}

func TestEthGetLogs_Empty(t *testing.T) {
	srv := newTestServer(t, &stubBackend{logs: nil})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_getLogs", []interface{}{
		map[string]interface{}{"fromBlock": "0x0", "toBlock": "0x0"},
	})
	logs, ok := resp["result"].([]interface{})
	if !ok || len(logs) != 0 {
		t.Fatalf("eth_getLogs empty should return [], got %v", resp["result"])
	}
}

func TestBatchRequest(t *testing.T) {
	srv := newTestServer(t, &stubBackend{chainID: 1, blockNumber: 5})
	defer srv.Close()

	body, _ := json.Marshal([]map[string]interface{}{
		{"jsonrpc": "2.0", "method": "eth_chainId", "params": []interface{}{}, "id": 1},
		{"jsonrpc": "2.0", "method": "eth_blockNumber", "params": []interface{}{}, "id": 2},
	})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var results []map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		t.Fatalf("decode batch: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("batch: expected 2 responses, got %d", len(results))
	}
	for _, r := range results {
		if _, hasErr := r["error"]; hasErr {
			t.Fatalf("batch item has error: %v", r["error"])
		}
	}
}
```

**Important:** The test file uses `jsonrpc.NewServer(backend, 0).Handler()` and `&jsonrpc.API{}`. You will need to:
1. Export a `Handler()` method on `Server` that returns the `http.Handler`, **OR**
2. Export `NewAPI(backend Backend) *API` and make `API` implement `http.Handler`

The simplest fix: add to `server.go`:
```go
// Handler returns the HTTP handler for use in tests.
func (s *Server) Handler() http.Handler {
    return s.httpServer.Handler
}
```

And add to `api.go`:
```go
// NewAPI creates a new API handler. Exposed for testing.
func NewAPI(backend Backend) *API {
    return &API{backend: backend}
}
```

Then update the test's `newTestServer` to:
```go
func newTestServer(t *testing.T, backend jsonrpc.Backend) *httptest.Server {
    t.Helper()
    api := jsonrpc.NewAPI(backend)
    return httptest.NewServer(api)
}
```

Remove the `srv.Handler()` approach entirely.

- [ ] **Step 2: Run tests — expect all 16 to fail**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go test ./internal/jsonrpc/... -v -run "TestNet|TestWeb3|TestEth|TestBatch" 2>&1 | tail -30
```

Expected: all 16 FAIL (stub handlers return "not implemented" error → `rpcCall` sees `"error"` field → `t.Fatalf`).

- [ ] **Step 3: Commit**

```bash
git add internal/jsonrpc/api_test.go internal/jsonrpc/server.go internal/jsonrpc/api.go
git commit -m "test(phase11): add failing tests for all 15 JSON-RPC methods and batch requests"
```

---

## Task 3: Implement Infrastructure Group (5 Methods)

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/jsonrpc/api.go`

- [ ] **Step 1: Replace 2 stubs in `core/tron_backend.go`**

Replace:
```go
func (b *TronBackend) ChainID() int64 {
	return 0 // stub
}
```
With:
```go
func (b *TronBackend) ChainID() int64 {
	return b.chain.Config().ChainID
}
```

Replace:
```go
func (b *TronBackend) BlockNumber() uint64 {
	return 0 // stub
}
```
With:
```go
func (b *TronBackend) BlockNumber() uint64 {
	return b.chain.CurrentBlock().Number()
}
```

- [ ] **Step 2: Replace 5 stub handlers in `internal/jsonrpc/api.go`**

Replace `netVersion`, `web3ClientVersion`, `ethChainId`, `ethBlockNumber`, `ethSyncing` stubs with:

```go
func (api *API) netVersion(_ json.RawMessage) (interface{}, error) {
	return fmt.Sprintf("%d", api.backend.ChainID()), nil
}

func (api *API) web3ClientVersion(_ json.RawMessage) (interface{}, error) {
	return "go-tron/v0.3.0-dev", nil
}

func (api *API) ethChainId(_ json.RawMessage) (interface{}, error) {
	return hexUint64(uint64(api.backend.ChainID())), nil
}

func (api *API) ethBlockNumber(_ json.RawMessage) (interface{}, error) {
	return hexUint64(api.backend.BlockNumber()), nil
}

func (api *API) ethSyncing(_ json.RawMessage) (interface{}, error) {
	return false, nil
}
```

- [ ] **Step 3: Run the 5 infrastructure tests**

```bash
go test ./internal/jsonrpc/... -v -run "TestNetVersion|TestWeb3ClientVersion|TestEthChainId|TestEthBlockNumber|TestEthSyncing"
```

Expected: all 5 PASS.

- [ ] **Step 4: Commit**

```bash
git add core/tron_backend.go internal/jsonrpc/api.go
git commit -m "feat(phase11): implement infrastructure JSON-RPC methods (net_version, eth_chainId, eth_blockNumber, eth_syncing)"
```

---

## Task 4: Implement Account State Group (4 Methods)

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/jsonrpc/api.go`

- [ ] **Step 1: Replace 3 stubs in `core/tron_backend.go`**

These open the current state trie and read account data. Add `"math/big"` to imports if not already present.

Replace `GetBalance` stub:
```go
func (b *TronBackend) GetBalance(addr tcommon.Address) int64 {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return 0
	}
	return statedb.GetBalance(addr)
}
```

Replace `GetCode` stub:
```go
func (b *TronBackend) GetCode(addr tcommon.Address) []byte {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil
	}
	return statedb.GetCode(addr)
}
```

Replace `GetStorageAt` stub:
```go
func (b *TronBackend) GetStorageAt(addr tcommon.Address, slot tcommon.Hash) tcommon.Hash {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return tcommon.Hash{}
	}
	return statedb.GetState(addr, slot)
}
```

- [ ] **Step 2: Replace 4 stub handlers in `internal/jsonrpc/api.go`**

Add `"math/big"` to the imports in `api.go`.

Replace the 4 account-state stubs:

```go
func (api *API) ethGetBalance(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	addr := common.BytesToAddress(common.FromHex(p[0]))
	balSUN := api.backend.GetBalance(addr)
	// Multiply by 1e12 using big.Int to avoid int64 overflow for large balances.
	wei := new(big.Int).Mul(big.NewInt(balSUN), big.NewInt(1_000_000_000_000))
	return fmt.Sprintf("0x%x", wei), nil
}

func (api *API) ethGetTransactionCount(params json.RawMessage) (interface{}, error) {
	// TRON has no nonces. Always return 0.
	return "0x0", nil
}

func (api *API) ethGetCode(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	addr := common.BytesToAddress(common.FromHex(p[0]))
	return hexBytes(api.backend.GetCode(addr)), nil
}

func (api *API) ethGetStorageAt(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 2 {
		return nil, fmt.Errorf("invalid params")
	}
	addr := common.BytesToAddress(common.FromHex(p[0]))
	var slot common.Hash
	slotBytes := common.FromHex(p[1])
	copy(slot[32-len(slotBytes):], slotBytes)
	val := api.backend.GetStorageAt(addr, slot)
	return hexBytes(val[:]), nil
}
```

Add the `common` import to `api.go`:
```go
import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
)
```

- [ ] **Step 3: Run the 4 account state tests**

```bash
go test ./internal/jsonrpc/... -v -run "TestEthGetBalance|TestEthGetTransactionCount|TestEthGetCode|TestEthGetStorageAt"
```

Expected: all 4 PASS.

- [ ] **Step 4: Commit**

```bash
git add core/tron_backend.go internal/jsonrpc/api.go
git commit -m "feat(phase11): implement account state JSON-RPC methods (eth_getBalance, eth_getCode, eth_getStorageAt)"
```

---

## Task 5: Implement eth_call

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/jsonrpc/api.go`

- [ ] **Step 1: Replace the `Call` stub in `core/tron_backend.go`**

```go
func (b *TronBackend) Call(from, to *tcommon.Address, data []byte, value int64) ([]byte, error) {
	fromAddr := tcommon.Address{}
	if from != nil {
		fromAddr = *from
	}
	if to == nil {
		return nil, fmt.Errorf("eth_call: 'to' address is required")
	}
	result, err := b.TriggerConstantContract(fromAddr, *to, data, 30_000_000)
	if err != nil {
		return nil, err
	}
	return result.Result, nil
}
```

- [ ] **Step 2: Replace the `ethCall` stub in `internal/jsonrpc/api.go`**

```go
func (api *API) ethCall(params json.RawMessage) (interface{}, error) {
	// params: [{from, to, data, value, gas}, blockParam]
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}

	var txObj struct {
		From  string `json:"from"`
		To    string `json:"to"`
		Data  string `json:"data"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(p[0], &txObj); err != nil {
		return nil, fmt.Errorf("invalid tx object: %w", err)
	}
	if txObj.To == "" {
		return nil, fmt.Errorf("eth_call: 'to' required")
	}

	var from *common.Address
	if txObj.From != "" {
		a := common.BytesToAddress(common.FromHex(txObj.From))
		from = &a
	}
	to := common.BytesToAddress(common.FromHex(txObj.To))

	data := common.FromHex(txObj.Data)

	var value int64
	if txObj.Value != "" && txObj.Value != "0x0" && txObj.Value != "0x" {
		v, _ := strconv.ParseInt(txObj.Value, 0, 64)
		value = v
	}

	result, err := api.backend.Call(from, &to, data, value)
	if err != nil {
		return nil, err
	}
	return hexBytes(result), nil
}
```

- [ ] **Step 3: Run the eth_call test**

```bash
go test ./internal/jsonrpc/... -v -run "TestEthCall"
```

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add core/tron_backend.go internal/jsonrpc/api.go
git commit -m "feat(phase11): implement eth_call (read-only TVM simulation)"
```

---

## Task 6: Implement Block Query Group (eth_getBlockByNumber, eth_getBlockByHash)

**Files:**
- Modify: `internal/jsonrpc/api.go`

(`GetBlockByNumber` and `GetBlockByHash` are already implemented on TronBackend — no changes to `core/tron_backend.go` needed.)

- [ ] **Step 1: Add `blockToRPC` helper at the end of `internal/jsonrpc/api.go`**

```go
// blockToRPC converts a types.Block to the Ethereum JSON block object.
// If fullTx is true, transactions are full objects; otherwise just hashes.
func blockToRPC(b *types.Block, fullTx bool) map[string]interface{} {
	txs := b.Transactions()

	var transactions interface{}
	if fullTx {
		list := make([]map[string]interface{}, len(txs))
		for i, tx := range txs {
			list[i] = txToRPC(tx.Proto(), tx.Hash(), b, i)
		}
		transactions = list
	} else {
		hashes := make([]string, len(txs))
		for i, tx := range txs {
			hashes[i] = fmt.Sprintf("0x%x", tx.Hash())
		}
		transactions = hashes
	}

	return map[string]interface{}{
		"hash":             fmt.Sprintf("0x%x", b.Hash()),
		"parentHash":       fmt.Sprintf("0x%x", b.ParentHash()),
		"number":           hexUint64(b.Number()),
		"timestamp":        hexUint64(uint64(b.Timestamp() / 1000)), // ms → s
		"miner":            hex20(b.WitnessAddress()[:]),
		"difficulty":       "0x0",
		"totalDifficulty":  "0x0",
		"extraData":        "0x",
		"size":             "0x0",
		"gasLimit":         "0x0",
		"gasUsed":          "0x0",
		"nonce":            "0x0000000000000000",
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"logsBloom":        "0x" + zeroBloom(),
		"transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"stateRoot":        fmt.Sprintf("0x%x", b.AccountStateRoot()),
		"receiptsRoot":     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"uncles":           []string{},
		"transactions":     transactions,
	}
}

// zeroBloom returns 512 hex zeros (256 bytes = logs bloom placeholder).
func zeroBloom() string {
	const zeros = "0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000"
	return zeros
}
```

The `types` import must be added to `api.go`:
```go
import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)
```

Add also `txToRPC` placeholder (will be completed in Task 7):

```go
// txToRPC converts a raw TRON transaction proto to Ethereum-format JSON.
// hash is provided by the caller (it was the lookup key).
func txToRPC(tx *corepb.Transaction, hash common.Hash, block *types.Block, index int) map[string]interface{} {
	from := "0x0000000000000000000000000000000000000000"
	to := "0x0000000000000000000000000000000000000000"
	input := "0x"
	value := "0x0"

	if len(tx.GetRawData().GetContract()) > 0 {
		c := tx.RawData.Contract[0]
		switch c.Type {
		case corepb.Transaction_Contract_TriggerSmartContract:
			var msg contractpb.TriggerSmartContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
				to = hex20(msg.ContractAddress)
				input = hexBytes(msg.Data)
				value = hexUint64(uint64(msg.CallValue))
			}
		case corepb.Transaction_Contract_CreateSmartContract:
			var msg contractpb.CreateSmartContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
				to = ""
				if msg.NewContract != nil {
					input = hexBytes(msg.NewContract.Bytecode)
				}
			}
		case corepb.Transaction_Contract_TransferContract:
			var msg contractpb.TransferContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
				to = hex20(msg.ToAddress)
				value = hexUint64(uint64(msg.Amount))
			}
		}
	}

	result := map[string]interface{}{
		"hash":             fmt.Sprintf("0x%x", hash),
		"blockHash":        fmt.Sprintf("0x%x", block.Hash()),
		"blockNumber":      hexUint64(block.Number()),
		"transactionIndex": hexUint64(uint64(index)),
		"from":             from,
		"value":            value,
		"gas":              hexUint64(uint64(tx.GetRawData().GetFeeLimit())),
		"gasPrice":         "0x1",
		"input":            input,
		"nonce":            "0x0",
		"type":             "0x0",
		"v":                "0x0",
		"r":                "0x0",
		"s":                "0x0",
	}
	if to != "" {
		result["to"] = to
	} else {
		result["to"] = nil
	}
	return result
}
```

- [ ] **Step 2: Replace the 2 block handler stubs in `internal/jsonrpc/api.go`**

```go
func (api *API) ethGetBlockByNumber(params json.RawMessage) (interface{}, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var blockTag string
	json.Unmarshal(p[0], &blockTag)

	var fullTx bool
	if len(p) > 1 {
		json.Unmarshal(p[1], &fullTx)
	}

	num := parseBlockParam(blockTag)
	if num == ^uint64(0) { // "latest"
		num = api.backend.BlockNumber()
	}
	block, err := api.backend.GetBlockByNumber(num)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // null = not found
	}
	return blockToRPC(block, fullTx), nil
}

func (api *API) ethGetBlockByHash(params json.RawMessage) (interface{}, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var hashStr string
	json.Unmarshal(p[0], &hashStr)

	var fullTx bool
	if len(p) > 1 {
		json.Unmarshal(p[1], &fullTx)
	}

	var hash common.Hash
	copy(hash[:], common.FromHex(hashStr))
	block, err := api.backend.GetBlockByHash(hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return blockToRPC(block, fullTx), nil
}
```

- [ ] **Step 3: Run the 2 block tests**

```bash
go test ./internal/jsonrpc/... -v -run "TestEthGetBlockByNumber|TestEthGetBlockByHash"
```

Expected: both PASS (stub backend returns `nil` block → handlers return `null` → test checks for `nil` result).

- [ ] **Step 4: Commit**

```bash
git add internal/jsonrpc/api.go
git commit -m "feat(phase11): implement eth_getBlockByNumber and eth_getBlockByHash"
```

---

## Task 7: Implement Transaction Query Group (eth_getTransactionByHash, eth_getTransactionReceipt)

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/jsonrpc/api.go`

- [ ] **Step 1: Replace the 2 transaction stubs in `core/tron_backend.go`**

Replace `GetTransactionByHash` stub:
```go
func (b *TronBackend) GetTransactionByHash(hash tcommon.Hash) (*corepb.Transaction, *types.Block, int, error) {
	// Use TransactionInfo to locate the block, then find the tx within it.
	info := rawdb.ReadTransactionInfo(b.chain.db, hash[:])
	if info == nil {
		return nil, nil, 0, nil // not found
	}
	block, err := b.chain.GetBlockByNumber(uint64(info.BlockNumber))
	if err != nil || block == nil {
		return nil, nil, 0, nil
	}
	for i, tx := range block.Transactions() {
		if tx.Hash() == hash {
			return tx.Proto(), block, i, nil
		}
	}
	return nil, nil, 0, nil
}
```

Replace `GetTransactionInfo` stub:
```go
func (b *TronBackend) GetTransactionInfo(hash tcommon.Hash) (*corepb.TransactionInfo, error) {
	info := rawdb.ReadTransactionInfo(b.chain.db, hash[:])
	return info, nil // nil info = not found (not an error)
}
```

- [ ] **Step 2: Add `receiptToRPC` helper at the end of `internal/jsonrpc/api.go`**

```go
// receiptToRPC converts TRON tx + info to an Ethereum receipt JSON object.
func receiptToRPC(hash common.Hash, tx *corepb.Transaction, info *corepb.TransactionInfo, block *types.Block, index int) map[string]interface{} {
	status := "0x1"
	if info.Result == corepb.TransactionInfo_FAILED {
		status = "0x0"
	}

	var contractAddr interface{} = nil
	if len(info.ContractAddress) > 0 {
		contractAddr = hex20(info.ContractAddress)
	}

	var toAddr interface{} = "0x0000000000000000000000000000000000000000"
	if len(tx.GetRawData().GetContract()) > 0 && tx.RawData.Contract[0].Type == corepb.Transaction_Contract_CreateSmartContract {
		toAddr = nil
	}

	energyUsed := int64(0)
	if info.Receipt != nil {
		energyUsed = info.Receipt.EnergyUsageTotal
	}

	// Build logs
	logs := make([]map[string]interface{}, 0)
	for li, l := range info.Log {
		topics := make([]string, len(l.Topics))
		for ti, t := range l.Topics {
			topics[ti] = fmt.Sprintf("0x%064x", t)
		}
		logs = append(logs, map[string]interface{}{
			"address":          hex20(l.Address),
			"topics":           topics,
			"data":             hexBytes(l.Data),
			"blockNumber":      hexUint64(block.Number()),
			"transactionHash":  fmt.Sprintf("0x%x", hash),
			"transactionIndex": hexUint64(uint64(index)),
			"blockHash":        fmt.Sprintf("0x%x", block.Hash()),
			"logIndex":         hexUint64(uint64(li)),
			"removed":          false,
		})
	}

	return map[string]interface{}{
		"transactionHash":   fmt.Sprintf("0x%x", hash),
		"transactionIndex":  hexUint64(uint64(index)),
		"blockHash":         fmt.Sprintf("0x%x", block.Hash()),
		"blockNumber":       hexUint64(block.Number()),
		"from":              "0x0000000000000000000000000000000000000000",
		"to":                toAddr,
		"cumulativeGasUsed": hexUint64(uint64(energyUsed)),
		"gasUsed":           hexUint64(uint64(energyUsed)),
		"contractAddress":   contractAddr,
		"logs":              logs,
		"logsBloom":         "0x" + zeroBloom(),
		"status":            status,
		"type":              "0x0",
	}
}
```

- [ ] **Step 3: Replace the 2 transaction handler stubs in `internal/jsonrpc/api.go`**

```go
func (api *API) ethGetTransactionByHash(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var hash common.Hash
	copy(hash[:], common.FromHex(p[0]))

	tx, block, index, err := api.backend.GetTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, nil // not found → null
	}
	return txToRPC(tx, hash, block, index), nil
}

func (api *API) ethGetTransactionReceipt(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var hash common.Hash
	copy(hash[:], common.FromHex(p[0]))

	info, err := api.backend.GetTransactionInfo(hash)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil // not found → null
	}
	tx, block, index, err := api.backend.GetTransactionByHash(hash)
	if err != nil || tx == nil {
		return nil, nil // receipt exists but can't find tx — treat as not found
	}
	return receiptToRPC(hash, tx, info, block, index), nil
}
```

- [ ] **Step 4: Run the 2 transaction tests**

```bash
go test ./internal/jsonrpc/... -v -run "TestEthGetTransactionByHash|TestEthGetTransactionReceipt"
```

Expected: both PASS (stub returns nil → handler returns null → test checks nil).

- [ ] **Step 5: Commit**

```bash
git add core/tron_backend.go internal/jsonrpc/api.go
git commit -m "feat(phase11): implement eth_getTransactionByHash and eth_getTransactionReceipt"
```

---

## Task 8: Implement eth_getLogs

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/jsonrpc/api.go`

- [ ] **Step 1: Replace `GetLogs` stub in `core/tron_backend.go`**

```go
func (b *TronBackend) GetLogs(filter jsonrpc.LogFilter) ([]*jsonrpc.RPCLog, error) {
	const maxBlockRange = 2000

	var fromBlock, toBlock uint64

	if filter.BlockHash != nil {
		// Single-block mode
		block, err := b.chain.GetBlockByHash(*filter.BlockHash)
		if err != nil || block == nil {
			return []*jsonrpc.RPCLog{}, nil
		}
		fromBlock = block.Number()
		toBlock = block.Number()
	} else {
		current := b.chain.CurrentBlock().Number()
		fromBlock = 0
		if filter.FromBlock != nil {
			fromBlock = *filter.FromBlock
		}
		toBlock = current
		if filter.ToBlock != nil {
			toBlock = *filter.ToBlock
		}
		if toBlock > current {
			toBlock = current
		}
		if toBlock < fromBlock {
			return []*jsonrpc.RPCLog{}, nil
		}
		if toBlock-fromBlock+1 > maxBlockRange {
			return nil, fmt.Errorf("block range too large (max %d)", maxBlockRange)
		}
	}

	var logs []*jsonrpc.RPCLog
	logIndex := uint64(0)

	for num := fromBlock; num <= toBlock; num++ {
		block, err := b.chain.GetBlockByNumber(num)
		if err != nil || block == nil {
			continue
		}
		blockHash := block.Hash()
		infos := rawdb.ReadTransactionInfosByBlock(b.chain.db, num)

		for txIdx, info := range infos {
			for _, l := range info.Log {
				// Address filter
				if len(filter.Addresses) > 0 {
					addr := tcommon.BytesToAddress(l.Address[len(l.Address)-20:])
					match := false
					for _, fa := range filter.Addresses {
						if fa == addr {
							match = true
							break
						}
					}
					if !match {
						continue
					}
				}

				// Topics filter
				if !matchTopics(filter.Topics, l.Topics) {
					continue
				}

				topics := make([]string, len(l.Topics))
				for i, t := range l.Topics {
					topics[i] = fmt.Sprintf("0x%064x", t)
				}

				// Recover the txHash from block transactions at txIdx
				txHash := tcommon.Hash{}
				txs := block.Transactions()
				if txIdx < len(txs) {
					txHash = txs[txIdx].Hash()
				}

				logs = append(logs, &jsonrpc.RPCLog{
					Address:          hex20jrpc(l.Address),
					Topics:           topics,
					Data:             hexBytesJrpc(l.Data),
					BlockNumber:      hexUint64Jrpc(num),
					TransactionHash:  fmt.Sprintf("0x%x", txHash),
					TransactionIndex: hexUint64Jrpc(uint64(txIdx)),
					BlockHash:        fmt.Sprintf("0x%x", blockHash),
					LogIndex:         hexUint64Jrpc(logIndex),
					Removed:          false,
				})
				logIndex++
			}
		}
	}

	if logs == nil {
		logs = []*jsonrpc.RPCLog{}
	}
	return logs, nil
}

// matchTopics returns true if the log topics match the filter topics.
// filter.Topics[i] == nil means any value is accepted at position i.
// filter.Topics[i] with multiple hashes means OR match.
func matchTopics(filterTopics [][]tcommon.Hash, logTopics [][]byte) bool {
	for i, required := range filterTopics {
		if len(required) == 0 {
			continue // nil / empty = any
		}
		if i >= len(logTopics) {
			return false
		}
		var logTopic tcommon.Hash
		copy(logTopic[32-len(logTopics[i]):], logTopics[i])
		matched := false
		for _, h := range required {
			if h == logTopic {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}
```

**Note:** The helper functions `hex20jrpc`, `hexBytesJrpc`, `hexUint64Jrpc` are just the package-level functions in `internal/jsonrpc/api.go` but accessed here in `core`. Since they're defined there, use equivalent logic inline or call package functions via the filter result builder. Actually — all the hex formatting happens in `core/tron_backend.go` when building `*jsonrpc.RPCLog` structs. Use `fmt.Sprintf("0x%x", ...)` directly:

Correct the `GetLogs` implementation to use `fmt.Sprintf` instead of helper function names:

```go
// In the log struct construction, replace helper calls with:
Address:          fmt.Sprintf("0x%x", l.Address[max(0, len(l.Address)-20):]),
Data:             fmt.Sprintf("0x%x", l.Data),
BlockNumber:      fmt.Sprintf("0x%x", num),
TransactionIndex: fmt.Sprintf("0x%x", txIdx),
LogIndex:         fmt.Sprintf("0x%x", logIndex),
```

And add `"fmt"` to imports if not already there.

For the `max` helper (Go 1.21+), use an inline comparison:
```go
addrStart := 0
if len(l.Address) > 20 {
    addrStart = len(l.Address) - 20
}
address := fmt.Sprintf("0x%x", l.Address[addrStart:])
```

- [ ] **Step 2: Replace `ethGetLogs` stub in `internal/jsonrpc/api.go`**

```go
func (api *API) ethGetLogs(params json.RawMessage) (interface{}, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}

	var filterObj struct {
		FromBlock string          `json:"fromBlock"`
		ToBlock   string          `json:"toBlock"`
		BlockHash string          `json:"blockHash"`
		Address   json.RawMessage `json:"address"`
		Topics    json.RawMessage `json:"topics"`
	}
	if err := json.Unmarshal(p[0], &filterObj); err != nil {
		return nil, fmt.Errorf("invalid filter: %w", err)
	}

	filter := LogFilter{}

	if filterObj.BlockHash != "" {
		var h common.Hash
		copy(h[:], common.FromHex(filterObj.BlockHash))
		filter.BlockHash = &h
	} else {
		if filterObj.FromBlock != "" {
			n := parseBlockParam(filterObj.FromBlock)
			if n == ^uint64(0) {
				n = api.backend.BlockNumber()
			}
			filter.FromBlock = &n
		}
		if filterObj.ToBlock != "" {
			n := parseBlockParam(filterObj.ToBlock)
			if n == ^uint64(0) {
				n = api.backend.BlockNumber()
			}
			filter.ToBlock = &n
		}
	}

	// Parse address: string or []string
	if len(filterObj.Address) > 0 && string(filterObj.Address) != "null" {
		var addrStr string
		var addrSlice []string
		if json.Unmarshal(filterObj.Address, &addrStr) == nil {
			filter.Addresses = []common.Address{common.BytesToAddress(common.FromHex(addrStr))}
		} else if json.Unmarshal(filterObj.Address, &addrSlice) == nil {
			for _, a := range addrSlice {
				filter.Addresses = append(filter.Addresses, common.BytesToAddress(common.FromHex(a)))
			}
		}
	}

	// Parse topics: [topic0, topic1, ...]  where each element is null | string | []string
	if len(filterObj.Topics) > 0 && string(filterObj.Topics) != "null" {
		var rawTopics []json.RawMessage
		if err := json.Unmarshal(filterObj.Topics, &rawTopics); err == nil {
			filter.Topics = make([][]common.Hash, len(rawTopics))
			for i, rt := range rawTopics {
				if string(rt) == "null" {
					filter.Topics[i] = nil
					continue
				}
				var single string
				var multi []string
				if json.Unmarshal(rt, &single) == nil {
					var h common.Hash
					copy(h[:], common.FromHex(single))
					filter.Topics[i] = []common.Hash{h}
				} else if json.Unmarshal(rt, &multi) == nil {
					for _, s := range multi {
						var h common.Hash
						copy(h[:], common.FromHex(s))
						filter.Topics[i] = append(filter.Topics[i], h)
					}
				}
			}
		}
	}

	logs, err := api.backend.GetLogs(filter)
	if err != nil {
		return nil, err
	}
	if logs == nil {
		logs = []*RPCLog{}
	}
	return logs, nil
}
```

- [ ] **Step 3: Run all 16 tests**

```bash
go test ./internal/jsonrpc/... -v
```

Expected: all 16 PASS.

- [ ] **Step 4: Commit**

```bash
git add core/tron_backend.go internal/jsonrpc/api.go
git commit -m "feat(phase11): implement eth_getLogs with block-range scan (max 2000 blocks)"
```

---

## Task 9: Full Build Verification + System Test Section 10

**Files:**
- Verify: whole project builds and tests pass
- Modify: `scripts/system_test.sh`

- [ ] **Step 1: Build the complete project**

```bash
cd /Users/asuka/Projects/asuka/go/go-tron
go build ./...
```

Expected: exits 0.

- [ ] **Step 2: Run all tests**

```bash
go test ./...
```

Expected: all packages pass.

- [ ] **Step 3: Build the binary**

```bash
go build -o build/bin/gtron ./cmd/gtron/
```

Expected: exits 0.

- [ ] **Step 4: Add Section 10 to `scripts/system_test.sh`**

Read the file first to find the insertion point: immediately before the `# Summary: print last few lines of logs` comment. Then insert:

```bash
# ─────────────────────────────────────────────────────────────────
# SECTION 10: Phase 11 — JSON-RPC API at :8545
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 10: Ethereum-Compatible JSON-RPC API ==="

JRPC_URL="http://localhost:8545"

# Helper: JSON-RPC POST
jrpc_call() {
    local method="$1"
    local params="$2"
    curl -s -X POST "$JRPC_URL" \
        -H "Content-Type: application/json" \
        -d "{\"jsonrpc\":\"2.0\",\"method\":\"$method\",\"params\":$params,\"id\":1}"
}

# 10.1 net_version
RESULT=$(jrpc_call "net_version" "[]")
check "net_version returns result" "$RESULT" '"result"'

# 10.2 web3_clientVersion
RESULT=$(jrpc_call "web3_clientVersion" "[]")
check "web3_clientVersion returns go-tron" "$RESULT" "go-tron"

# 10.3 eth_chainId
RESULT=$(jrpc_call "eth_chainId" "[]")
check "eth_chainId returns hex result" "$RESULT" '"result":"0x'

# 10.4 eth_blockNumber
RESULT=$(jrpc_call "eth_blockNumber" "[]")
check "eth_blockNumber returns hex result" "$RESULT" '"result":"0x'

# 10.5 eth_syncing
RESULT=$(jrpc_call "eth_syncing" "[]")
check "eth_syncing returns false" "$RESULT" 'false'

# 10.6 eth_getBalance — witness address, latest
RESULT=$(jrpc_call "eth_getBalance" "[\"$WITNESS_ADDR\", \"latest\"]")
check "eth_getBalance returns hex result" "$RESULT" '"result":"0x'

# 10.7 eth_getTransactionCount — always 0x0
RESULT=$(jrpc_call "eth_getTransactionCount" "[\"$WITNESS_ADDR\", \"latest\"]")
check "eth_getTransactionCount returns 0x0" "$RESULT" '"result":"0x0"'

# 10.8 eth_getCode — witness is not a contract → 0x
RESULT=$(jrpc_call "eth_getCode" "[\"$WITNESS_ADDR\", \"latest\"]")
check "eth_getCode for EOA returns 0x" "$RESULT" '"result":"0x"'

# 10.9 eth_getBlockByNumber — block 0 (genesis)
RESULT=$(jrpc_call "eth_getBlockByNumber" "[\"0x0\", false]")
check "eth_getBlockByNumber genesis returns hash" "$RESULT" '"hash"'

# 10.10 eth_getTransactionByHash — unknown hash → null
RESULT=$(jrpc_call "eth_getTransactionByHash" \
    "[\"0x0000000000000000000000000000000000000000000000000000000000000000\"]")
check "eth_getTransactionByHash unknown returns null" "$RESULT" '"result":null'

# 10.11 eth_getLogs — empty range → [] 
RESULT=$(jrpc_call "eth_getLogs" "[{\"fromBlock\":\"0x0\",\"toBlock\":\"0x0\"}]")
check "eth_getLogs returns result array" "$RESULT" '"result"'

# 10.12 batch request
RESULT=$(curl -s -X POST "$JRPC_URL" \
    -H "Content-Type: application/json" \
    -d '[{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1},{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":2}]')
check "batch request returns array" "$RESULT" '"jsonrpc":"2.0"'
```

- [ ] **Step 5: Verify system test script syntax**

```bash
bash -n scripts/system_test.sh
```

Expected: exits 0.

- [ ] **Step 6: Commit**

```bash
git add scripts/system_test.sh
git commit -m "test(phase11): add system test section 10 for JSON-RPC API endpoints"
```

---

## Self-Review Notes

**Spec coverage check:**

| Spec requirement | Task |
|---|---|
| HTTP-only JSON-RPC 2.0 | Task 1 |
| Batch request support | Task 1 (dispatcher), Task 2 (test) |
| net_version | Task 3 |
| web3_clientVersion | Task 3 |
| eth_chainId | Task 3 |
| eth_blockNumber | Task 3 |
| eth_syncing → false | Task 3 |
| eth_getBalance (SUN × 10¹² with big.Int) | Task 4 |
| eth_getTransactionCount → 0x0 | Task 4 |
| eth_getCode | Task 4 |
| eth_getStorageAt | Task 4 |
| eth_call (TVM simulation) | Task 5 |
| eth_getBlockByNumber | Task 6 |
| eth_getBlockByHash | Task 6 |
| Block → Ethereum format mapping | Task 6 (blockToRPC) |
| eth_getTransactionByHash | Task 7 |
| eth_getTransactionReceipt | Task 7 |
| Tx → Ethereum format (TriggerSmartContract, CreateSmartContract, Transfer) | Task 7 (txToRPC) |
| eth_getLogs with block-range scan (max 2000) | Task 8 |
| Topics filter (null, single, OR) | Task 8 |
| Address filter | Task 8 |
| Full build + binary | Task 9 |
| System test section | Task 9 |

**Type consistency:** `LogFilter`, `RPCLog` defined in Task 1 and used unchanged through Task 8. `blockToRPC`, `txToRPC`, `receiptToRPC` defined in Tasks 6-7 and not modified. Hex helpers (`hexUint64`, `hexBytes`, `hex20`, `hex32`) defined in Task 1. `parseBlockParam` defined in Task 1 used in Tasks 6, 8.

**Placeholder check:** All steps contain actual code. No TBD/TODO markers.
