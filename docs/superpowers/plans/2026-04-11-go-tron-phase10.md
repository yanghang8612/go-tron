# Phase 10: HTTP API Query Completion — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add 9 missing HTTP query endpoints covering delegation state, unfreeze windows, unclaimed rewards, transaction pool, and connected peers.

**Architecture:** All changes fit in four existing files. The Backend interface in `internal/tronapi/backend.go` gains 9 new methods and 8 new response types. `core/tron_backend.go` implements them plus a `peersFunc` injection (same pattern as `TxBroadcaster`). `internal/tronapi/api.go` registers and handles 9 new routes. `cmd/gtron/main.go` wires the peer lister.

**Tech Stack:** Go standard library, `encoding/hex`, `encoding/json`, `net/http`, `httptest`, existing `rawdb`, `state`, `txpool` packages.

---

## File Map

| File | What changes |
|---|---|
| `internal/tronapi/backend.go` | Add `PeerInfo` + 7 response types; add 9 methods to `Backend` interface |
| `core/tron_backend.go` | Add `peersFunc` field + `SetPeerLister`; implement 9 `TronBackend` methods |
| `internal/tronapi/api.go` | Register 9 routes; implement 9 handler functions |
| `cmd/gtron/main.go` | Add `net` + `strconv` imports; call `backend.SetPeerLister(...)` |
| `internal/tronapi/api_test.go` | New file: full stub backend + 10 test functions |

---

## Task 1: Add Types, Interface Methods, and TronBackend Stubs

**Files:**
- Modify: `internal/tronapi/backend.go`
- Modify: `core/tron_backend.go`

- [ ] **Step 1: Add 8 new response types and PeerInfo to `internal/tronapi/backend.go`**

Append these types before the `Backend` interface declaration:

```go
// PeerInfo describes a connected P2P peer.
type PeerInfo struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// DelegatedResourceInfo holds the delegation record between two addresses.
type DelegatedResourceInfo struct {
	FromAddress               string `json:"fromAddress"`
	ToAddress                 string `json:"toAddress"`
	FrozenBalanceForBandwidth int64  `json:"frozenBalanceForBandwidth"`
	FrozenBalanceForEnergy    int64  `json:"frozenBalanceForEnergy"`
	ExpireTimeForBandwidth    int64  `json:"expireTimeForBandwidth"`
	ExpireTimeForEnergy       int64  `json:"expireTimeForEnergy"`
}

// DelegationIndexInfo lists all addresses that addr has delegated resources to.
type DelegationIndexInfo struct {
	Account     string   `json:"account"`
	ToAddresses []string `json:"toAddresses"`
}

// CanDelegateInfo reports how much resource an address can still delegate.
type CanDelegateInfo struct {
	MaxSize         int64 `json:"maxSize"`
	CanDelegateSize int64 `json:"canDelegateSize"`
	Balance         int64 `json:"balance"`
}

// CanWithdrawUnfreezeInfo holds the total withdrawable expired-unfreeze amount.
type CanWithdrawUnfreezeInfo struct {
	Amount int64 `json:"amount"`
}

// AvailableUnfreezeCountInfo holds the number of remaining unfreeze slots (max 32).
type AvailableUnfreezeCountInfo struct {
	Count int64 `json:"count"`
}

// RewardInfo holds unclaimed witness reward (allowance).
type RewardInfo struct {
	Reward int64 `json:"reward"`
}
```

- [ ] **Step 2: Add 9 methods to the `Backend` interface in `internal/tronapi/backend.go`**

Add these lines inside the `Backend` interface, after `ListProposals`:

```go
	// Delegation/resource queries (Stake 2.0)
	GetDelegatedResourceV2(from, to common.Address) (*DelegatedResourceInfo, error)
	GetDelegatedResourceAccountIndexV2(addr common.Address) (*DelegationIndexInfo, error)
	CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*CanDelegateInfo, error)
	GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*CanWithdrawUnfreezeInfo, error)
	GetAvailableUnfreezeCount(addr common.Address) (*AvailableUnfreezeCountInfo, error)

	// Rewards
	GetReward(addr common.Address) (*RewardInfo, error)

	// Transaction pool queries
	GetTransactionFromPending(txID string) (*corepb.Transaction, error)
	GetTransactionListFromPending() ([]*corepb.Transaction, error)

	// Network
	ListNodes() ([]*PeerInfo, error)
```

- [ ] **Step 3: Add `peersFunc` field + `SetPeerLister` + 9 stub implementations to `core/tron_backend.go`**

Add the field to the `TronBackend` struct (after `txBroadcast`):

```go
	peersFunc func() []tronapi.PeerInfo // nil until wired from main
```

Add the setter after `SetTxBroadcaster`:

```go
// SetPeerLister wires in a function that returns connected P2P peers.
// Called from main.go to avoid a core→net import cycle.
func (b *TronBackend) SetPeerLister(fn func() []tronapi.PeerInfo) {
	b.peersFunc = fn
}
```

Add these stub implementations at the end of the file:

```go
func (b *TronBackend) GetDelegatedResourceV2(from, to tcommon.Address) (*tronapi.DelegatedResourceInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetDelegatedResourceAccountIndexV2(addr tcommon.Address) (*tronapi.DelegationIndexInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) CanDelegateResource(addr tcommon.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetCanWithdrawUnfreezeAmount(addr tcommon.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetAvailableUnfreezeCount(addr tcommon.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetReward(addr tcommon.Address) (*tronapi.RewardInfo, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (b *TronBackend) ListNodes() ([]*tronapi.PeerInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

- [ ] **Step 4: Verify the codebase compiles**

```bash
cd /path/to/go-tron
go build ./...
```

Expected: no errors. All existing tests still pass:

```bash
go test ./...
```

- [ ] **Step 5: Commit**

```bash
git add internal/tronapi/backend.go core/tron_backend.go
git commit -m "feat(phase10): add Backend interface methods and TronBackend stubs"
```

---

## Task 2: Create Test File with Mock Backend and All 10 Failing Tests

**Files:**
- Create: `internal/tronapi/api_test.go`

- [ ] **Step 1: Create `internal/tronapi/api_test.go` with the complete stub backend and all 10 test functions**

```go
package tronapi_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// stubBackend is a test double for tronapi.Backend.
// Pre-set fields control what each new method returns.
// All pre-existing methods return zero/nil values.
type stubBackend struct {
	delegatedResource *tronapi.DelegatedResourceInfo
	delegationIndex   *tronapi.DelegationIndexInfo
	canDelegate       *tronapi.CanDelegateInfo
	canWithdraw       *tronapi.CanWithdrawUnfreezeInfo
	availableUnfreeze *tronapi.AvailableUnfreezeCountInfo
	reward            *tronapi.RewardInfo
	pendingTx         *corepb.Transaction
	pendingTxList     []*corepb.Transaction
	nodes             []*tronapi.PeerInfo
}

// --- Pre-existing Backend methods (all return zero values) ---
func (s *stubBackend) CurrentBlock() *types.Block                             { return nil }
func (s *stubBackend) GetBlockByNumber(n uint64) (*types.Block, error)        { return nil, nil }
func (s *stubBackend) GetAccount(addr common.Address) (*types.Account, error) { return nil, nil }
func (s *stubBackend) BroadcastTransaction(tx *types.Transaction) error       { return nil }
func (s *stubBackend) GetNodeInfo() *tronapi.NodeInfo                          { return &tronapi.NodeInfo{} }
func (s *stubBackend) PendingTransactionCount() int                            { return 0 }
func (s *stubBackend) GetContract(addr common.Address) (*contractpb.SmartContract, error) {
	return nil, nil
}
func (s *stubBackend) TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	return nil, nil
}
func (s *stubBackend) GetTransactionByID(h common.Hash) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) GetTransactionInfoByID(h common.Hash) (*corepb.TransactionInfo, error) {
	return nil, nil
}
func (s *stubBackend) GetTransactionInfoByBlockNum(n uint64) ([]*corepb.TransactionInfo, error) {
	return nil, nil
}
func (s *stubBackend) GetBlockByHash(h common.Hash) (*types.Block, error) { return nil, nil }
func (s *stubBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	return nil, nil
}
func (s *stubBackend) BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte, feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildTriggerContractTransaction(owner, contract common.Address, data []byte, feeLimit int64, callValue int64) (*corepb.Transaction, *tronapi.TriggerResult, error) {
	return nil, nil, nil
}
func (s *stubBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	return 0, nil
}
func (s *stubBackend) GetAccountResource(addr common.Address) (*tronapi.AccountResource, error) {
	return nil, nil
}
func (s *stubBackend) GetChainParameters() []tronapi.ChainParameter { return nil }
func (s *stubBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) { return nil, nil }
func (s *stubBackend) NextMaintenanceTime() int64                     { return 0 }
func (s *stubBackend) BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) ListProposals() ([]*tronapi.ProposalInfo, error) { return nil, nil }

// --- New Phase 10 methods ---
func (s *stubBackend) GetDelegatedResourceV2(from, to common.Address) (*tronapi.DelegatedResourceInfo, error) {
	return s.delegatedResource, nil
}
func (s *stubBackend) GetDelegatedResourceAccountIndexV2(addr common.Address) (*tronapi.DelegationIndexInfo, error) {
	return s.delegationIndex, nil
}
func (s *stubBackend) CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	return s.canDelegate, nil
}
func (s *stubBackend) GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	return s.canWithdraw, nil
}
func (s *stubBackend) GetAvailableUnfreezeCount(addr common.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	return s.availableUnfreeze, nil
}
func (s *stubBackend) GetReward(addr common.Address) (*tronapi.RewardInfo, error) {
	return s.reward, nil
}
func (s *stubBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	if s.pendingTx == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	return s.pendingTx, nil
}
func (s *stubBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) {
	return s.pendingTxList, nil
}
func (s *stubBackend) ListNodes() ([]*tronapi.PeerInfo, error) {
	return s.nodes, nil
}

// --- Helpers ---
func newTestServer(t *testing.T, stub *stubBackend) *httptest.Server {
	t.Helper()
	api := tronapi.NewAPI(stub)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)
	return httptest.NewServer(mux)
}

func postJSON(t *testing.T, url, body string) map[string]interface{} {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status %d", url, resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON from %s: %v", url, err)
	}
	return result
}

// --- Tests: delegation group ---

func TestGetDelegatedResourceV2WithData(t *testing.T) {
	stub := &stubBackend{
		delegatedResource: &tronapi.DelegatedResourceInfo{
			FromAddress:            "4101",
			ToAddress:              "4102",
			FrozenBalanceForEnergy: 1000000,
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourcev2",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 1 {
		t.Fatalf("expected delegatedResource=[1 entry], got %v", result)
	}
}

func TestGetDelegatedResourceV2Empty(t *testing.T) {
	stub := &stubBackend{} // delegatedResource is nil
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourcev2",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 0 {
		t.Fatalf("expected delegatedResource=[], got %v", result)
	}
}

func TestGetDelegatedResourceAccountIndexV2(t *testing.T) {
	stub := &stubBackend{
		delegationIndex: &tronapi.DelegationIndexInfo{
			Account:     "4101",
			ToAddresses: []string{"4102", "4103"},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourceaccountindexv2",
		`{"value":"4101"}`)
	addrs, ok := result["toAddresses"].([]interface{})
	if !ok || len(addrs) != 2 {
		t.Fatalf("expected 2 toAddresses, got %v", result)
	}
}

func TestCanDelegateResource(t *testing.T) {
	stub := &stubBackend{
		canDelegate: &tronapi.CanDelegateInfo{MaxSize: 1000000, CanDelegateSize: 800000, Balance: 500000},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/candelegateresource",
		`{"ownerAddress":"4101","balance":500000,"type":0}`)
	if result["maxSize"].(float64) != 1000000 || result["canDelegateSize"].(float64) != 800000 {
		t.Fatalf("unexpected canDelegate response: %v", result)
	}
}

// --- Tests: unfreeze/reward group ---

func TestGetCanWithdrawUnfreezeAmount(t *testing.T) {
	stub := &stubBackend{
		canWithdraw: &tronapi.CanWithdrawUnfreezeInfo{Amount: 5000000},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getcanwithdrawunfreezeamount",
		`{"ownerAddress":"4101","timestamp":1712345678000}`)
	if result["amount"].(float64) != 5000000 {
		t.Fatalf("unexpected amount: %v", result)
	}
}

func TestGetAvailableUnfreezeCount(t *testing.T) {
	stub := &stubBackend{
		availableUnfreeze: &tronapi.AvailableUnfreezeCountInfo{Count: 30},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getavailableunfreezecount",
		`{"ownerAddress":"4101"}`)
	if result["count"].(float64) != 30 {
		t.Fatalf("unexpected count: %v", result)
	}
}

func TestGetReward(t *testing.T) {
	stub := &stubBackend{
		reward: &tronapi.RewardInfo{Reward: 123456},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getreward",
		`{"address":"4101"}`)
	if result["reward"].(float64) != 123456 {
		t.Fatalf("unexpected reward: %v", result)
	}
}

// --- Tests: pool group ---

func TestGetTransactionFromPendingFound(t *testing.T) {
	stub := &stubBackend{
		pendingTx: &corepb.Transaction{RawData: &corepb.TransactionRaw{}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionfrompending",
		`{"value":"aabbcc"}`)
	if _, hasRawData := result["raw_data"]; !hasRawData {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestGetTransactionFromPendingNotFound(t *testing.T) {
	stub := &stubBackend{} // pendingTx is nil → returns error
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionfrompending",
		`{"value":"aabbcc"}`)
	if _, hasError := result["Error"]; !hasError {
		t.Fatalf("expected Error field in not-found response, got %v", result)
	}
}

func TestGetTransactionListFromPending(t *testing.T) {
	stub := &stubBackend{pendingTxList: nil} // empty pool
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionlistfrompending", `{}`)
	txList, ok := result["transaction"].([]interface{})
	if !ok {
		t.Fatalf("expected transaction array, got %v", result)
	}
	if len(txList) != 0 {
		t.Fatalf("expected empty transaction list, got %d entries", len(txList))
	}
}

// --- Tests: network group ---

func TestListNodes(t *testing.T) {
	stub := &stubBackend{
		nodes: []*tronapi.PeerInfo{{Host: "127.0.0.1", Port: 18888}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/wallet/listnodes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var result map[string]interface{}
	json.NewDecoder(resp.Body).Decode(&result)
	nodes, ok := result["nodes"].([]interface{})
	if !ok || len(nodes) != 1 {
		t.Fatalf("expected nodes=[1 entry], got %v", result)
	}
	node := nodes[0].(map[string]interface{})
	addr := node["address"].(map[string]interface{})
	if addr["host"] != "127.0.0.1" {
		t.Fatalf("unexpected host: %v", addr["host"])
	}
}
```

- [ ] **Step 2: Run tests — expect all 10 new tests to fail with 404**

```bash
go test ./internal/tronapi/... -v -run "TestGetDelegated|TestCan|TestGetAvailable|TestGetReward|TestGetTransaction|TestListNodes" 2>&1 | tail -30
```

Expected: all 10 tests FAIL (handlers not yet registered → 404 → `status 404` in t.Fatal).

- [ ] **Step 3: Commit the test file**

```bash
git add internal/tronapi/api_test.go
git commit -m "test(phase10): add failing tests for all 9 new HTTP endpoints"
```

---

## Task 3: Implement Delegation Query Group

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/tronapi/api.go`

- [ ] **Step 1: Replace the 3 delegation stubs in `core/tron_backend.go` with real implementations**

Replace:
```go
func (b *TronBackend) GetDelegatedResourceV2(from, to tcommon.Address) (*tronapi.DelegatedResourceInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) GetDelegatedResourceV2(from, to tcommon.Address) (*tronapi.DelegatedResourceInfo, error) {
	dr := rawdb.ReadDelegatedResource(b.chain.db, from, to)
	if dr == nil {
		return nil, nil
	}
	return &tronapi.DelegatedResourceInfo{
		FromAddress:               hex.EncodeToString(from[:]),
		ToAddress:                 hex.EncodeToString(to[:]),
		FrozenBalanceForBandwidth: dr.FrozenBalanceForBandwidth,
		FrozenBalanceForEnergy:    dr.FrozenBalanceForEnergy,
		ExpireTimeForBandwidth:    dr.ExpireTimeForBandwidth,
		ExpireTimeForEnergy:       dr.ExpireTimeForEnergy,
	}, nil
}
```

Replace:
```go
func (b *TronBackend) GetDelegatedResourceAccountIndexV2(addr tcommon.Address) (*tronapi.DelegationIndexInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) GetDelegatedResourceAccountIndexV2(addr tcommon.Address) (*tronapi.DelegationIndexInfo, error) {
	receivers := rawdb.ReadDelegationIndex(b.chain.db, addr)
	toAddresses := make([]string, len(receivers))
	for i, r := range receivers {
		toAddresses[i] = hex.EncodeToString(r[:])
	}
	return &tronapi.DelegationIndexInfo{
		Account:     hex.EncodeToString(addr[:]),
		ToAddresses: toAddresses,
	}, nil
}
```

Replace:
```go
func (b *TronBackend) CanDelegateResource(addr tcommon.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) CanDelegateResource(addr tcommon.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	maxSize := statedb.GetFrozenV2Amount(addr, resource)

	// Compute already-delegated amount from the delegation index.
	var delegated int64
	for _, receiver := range rawdb.ReadDelegationIndex(b.chain.db, addr) {
		dr := rawdb.ReadDelegatedResource(b.chain.db, addr, receiver)
		if dr == nil {
			continue
		}
		switch resource {
		case corepb.ResourceCode_BANDWIDTH:
			delegated += dr.FrozenBalanceForBandwidth
		case corepb.ResourceCode_ENERGY:
			delegated += dr.FrozenBalanceForEnergy
		}
	}

	canDelegate := maxSize - delegated
	if canDelegate < 0 {
		canDelegate = 0
	}
	return &tronapi.CanDelegateInfo{
		MaxSize:         maxSize,
		CanDelegateSize: canDelegate,
		Balance:         amount,
	}, nil
}
```

- [ ] **Step 2: Register the 3 delegation routes in `internal/tronapi/api.go`**

In `RegisterRoutes`, after `mux.HandleFunc("/wallet/listproposals", api.listProposals)`, add:

```go
	// Phase 10: delegation/resource queries
	mux.HandleFunc("/wallet/getdelegatedresourcev2", api.getDelegatedResourceV2)
	mux.HandleFunc("/wallet/getdelegatedresourceaccountindexv2", api.getDelegatedResourceAccountIndexV2)
	mux.HandleFunc("/wallet/candelegateresource", api.canDelegateResource)
```

- [ ] **Step 3: Implement the 3 delegation handler functions in `internal/tronapi/api.go`**

Add these functions at the end of the file:

```go
func (api *API) getDelegatedResourceV2(w http.ResponseWriter, r *http.Request) {
	var body struct {
		FromAddress string `json:"fromAddress"`
		ToAddress   string `json:"toAddress"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.FromAddress == "" || body.ToAddress == "" {
		http.Error(w, "fromAddress and toAddress required", http.StatusBadRequest)
		return
	}
	from := common.BytesToAddress(common.FromHex(body.FromAddress))
	to := common.BytesToAddress(common.FromHex(body.ToAddress))
	info, err := api.backend.GetDelegatedResourceV2(from, to)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	list := []*DelegatedResourceInfo{}
	if info != nil {
		list = []*DelegatedResourceInfo{info}
	}
	data, _ := json.Marshal(map[string]interface{}{"delegatedResource": list})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getDelegatedResourceAccountIndexV2(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.Value))
	info, err := api.backend.GetDelegatedResourceAccountIndexV2(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) canDelegateResource(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OwnerAddress string `json:"ownerAddress"`
		Balance      int64  `json:"balance"`
		Type         int32  `json:"type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.OwnerAddress == "" {
		http.Error(w, "ownerAddress required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	info, err := api.backend.CanDelegateResource(addr, body.Balance, corepb.ResourceCode(body.Type))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
```

- [ ] **Step 4: Run the delegation tests — expect 4 to pass now**

```bash
go test ./internal/tronapi/... -v -run "TestGetDelegated|TestCanDelegate" 2>&1
```

Expected: `TestGetDelegatedResourceV2WithData`, `TestGetDelegatedResourceV2Empty`, `TestGetDelegatedResourceAccountIndexV2`, `TestCanDelegateResource` — all PASS.

- [ ] **Step 5: Commit**

```bash
git add core/tron_backend.go internal/tronapi/api.go
git commit -m "feat(phase10): implement delegation query endpoints (3 handlers)"
```

---

## Task 4: Implement Unfreeze/Reward Group

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/tronapi/api.go`

- [ ] **Step 1: Replace the 3 unfreeze/reward stubs in `core/tron_backend.go`**

Replace:
```go
func (b *TronBackend) GetCanWithdrawUnfreezeAmount(addr tcommon.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) GetCanWithdrawUnfreezeAmount(addr tcommon.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	acc := statedb.GetAccount(addr)
	if acc == nil {
		return &tronapi.CanWithdrawUnfreezeInfo{Amount: 0}, nil
	}
	var total int64
	for _, u := range acc.UnfrozenV2() {
		if u.UnfreezeExpireTime <= timestamp {
			total += u.UnfreezeAmount
		}
	}
	return &tronapi.CanWithdrawUnfreezeInfo{Amount: total}, nil
}
```

Replace:
```go
func (b *TronBackend) GetAvailableUnfreezeCount(addr tcommon.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) GetAvailableUnfreezeCount(addr tcommon.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	const maxUnfreezeSlots = 32
	count := int64(maxUnfreezeSlots)
	if acc := statedb.GetAccount(addr); acc != nil {
		count = int64(maxUnfreezeSlots - len(acc.UnfrozenV2()))
	}
	if count < 0 {
		count = 0
	}
	return &tronapi.AvailableUnfreezeCountInfo{Count: count}, nil
}
```

Replace:
```go
func (b *TronBackend) GetReward(addr tcommon.Address) (*tronapi.RewardInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) GetReward(addr tcommon.Address) (*tronapi.RewardInfo, error) {
	current := b.chain.CurrentBlock()
	root := current.AccountStateRoot()
	statedb, err := state.New(root, b.chain.StateDB())
	if err != nil {
		return nil, fmt.Errorf("open state: %w", err)
	}
	return &tronapi.RewardInfo{Reward: statedb.GetAllowance(addr)}, nil
}
```

- [ ] **Step 2: Register the 3 routes in `internal/tronapi/api.go`**

After the candelegateresource route, add:

```go
	// Phase 10: unfreeze/reward queries
	mux.HandleFunc("/wallet/getcanwithdrawunfreezeamount", api.getCanWithdrawUnfreezeAmount)
	mux.HandleFunc("/wallet/getavailableunfreezecount", api.getAvailableUnfreezeCount)
	mux.HandleFunc("/wallet/getreward", api.getReward)
```

- [ ] **Step 3: Implement the 3 handler functions in `internal/tronapi/api.go`**

```go
func (api *API) getCanWithdrawUnfreezeAmount(w http.ResponseWriter, r *http.Request) {
	var body struct {
		OwnerAddress string `json:"ownerAddress"`
		Timestamp    int64  `json:"timestamp"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.OwnerAddress == "" {
		http.Error(w, "ownerAddress required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(body.OwnerAddress))
	info, err := api.backend.GetCanWithdrawUnfreezeAmount(addr, body.Timestamp)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getAvailableUnfreezeCount(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("ownerAddress")
	if addrHex == "" {
		var body struct {
			OwnerAddress string `json:"ownerAddress"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		addrHex = body.OwnerAddress
	}
	if addrHex == "" {
		http.Error(w, "ownerAddress required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	info, err := api.backend.GetAvailableUnfreezeCount(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) getReward(w http.ResponseWriter, r *http.Request) {
	addrHex := r.URL.Query().Get("address")
	if addrHex == "" {
		var body struct {
			Address string `json:"address"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		addrHex = body.Address
	}
	if addrHex == "" {
		http.Error(w, "address required", http.StatusBadRequest)
		return
	}
	addr := common.BytesToAddress(common.FromHex(addrHex))
	info, err := api.backend.GetReward(addr)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data, _ := json.Marshal(info)
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
```

- [ ] **Step 4: Run the unfreeze/reward tests — expect 3 more to pass**

```bash
go test ./internal/tronapi/... -v -run "TestGetCanWithdraw|TestGetAvailable|TestGetReward" 2>&1
```

Expected: all 3 PASS.

- [ ] **Step 5: Commit**

```bash
git add core/tron_backend.go internal/tronapi/api.go
git commit -m "feat(phase10): implement unfreeze/reward query endpoints (3 handlers)"
```

---

## Task 5: Implement Pool/Network Group and Wire PeerLister

**Files:**
- Modify: `core/tron_backend.go`
- Modify: `internal/tronapi/api.go`
- Modify: `cmd/gtron/main.go`

- [ ] **Step 1: Replace the 3 pool/network stubs in `core/tron_backend.go`**

Replace:
```go
func (b *TronBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	hashBytes := tcommon.FromHex(txID)
	var hash tcommon.Hash
	copy(hash[:], hashBytes)
	tx := b.pool.Get(hash)
	if tx == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	return tx.Proto(), nil
}
```

Replace:
```go
func (b *TronBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) {
	txs := b.pool.Pending()
	result := make([]*corepb.Transaction, len(txs))
	for i, tx := range txs {
		result[i] = tx.Proto()
	}
	return result, nil
}
```

Replace:
```go
func (b *TronBackend) ListNodes() ([]*tronapi.PeerInfo, error) {
	return nil, fmt.Errorf("not implemented")
}
```

With:
```go
func (b *TronBackend) ListNodes() ([]*tronapi.PeerInfo, error) {
	if b.peersFunc == nil {
		return []*tronapi.PeerInfo{}, nil
	}
	return b.peersFunc(), nil
}
```

- [ ] **Step 2: Register the 3 routes in `internal/tronapi/api.go`**

After the getreward route, add:

```go
	// Phase 10: pool and network queries
	mux.HandleFunc("/wallet/gettransactionfrompending", api.getTransactionFromPending)
	mux.HandleFunc("/wallet/gettransactionlistfrompending", api.getTransactionListFromPending)
	mux.HandleFunc("/wallet/listnodes", api.listNodes)
```

- [ ] **Step 3: Implement the 3 handler functions in `internal/tronapi/api.go`**

```go
func (api *API) getTransactionFromPending(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Value string `json:"value"`
	}
	json.NewDecoder(r.Body).Decode(&body)
	if body.Value == "" {
		body.Value = r.URL.Query().Get("value")
	}
	if body.Value == "" {
		http.Error(w, "value required", http.StatusBadRequest)
		return
	}
	tx, err := api.backend.GetTransactionFromPending(body.Value)
	if err != nil {
		data, _ := json.Marshal(map[string]string{"Error": err.Error()})
		w.Header().Set("Content-Type", "application/json")
		w.Write(data)
		return
	}
	writeTransactionJSON(w, tx)
}

func (api *API) getTransactionListFromPending(w http.ResponseWriter, r *http.Request) {
	txs, err := api.backend.GetTransactionListFromPending()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	var result []map[string]interface{}
	for _, tx := range txs {
		m := marshalMessage(tx.ProtoReflect())
		addTxComputedFields(m, tx.ProtoReflect())
		result = append(result, m)
	}
	if result == nil {
		result = []map[string]interface{}{}
	}
	data, _ := json.Marshal(map[string]interface{}{"transaction": result})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}

func (api *API) listNodes(w http.ResponseWriter, r *http.Request) {
	peers, err := api.backend.ListNodes()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	type nodeAddress struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	}
	type node struct {
		Address nodeAddress `json:"address"`
	}
	nodes := make([]node, len(peers))
	for i, p := range peers {
		nodes[i] = node{Address: nodeAddress{Host: p.Host, Port: p.Port}}
	}
	data, _ := json.Marshal(map[string]interface{}{"nodes": nodes})
	w.Header().Set("Content-Type", "application/json")
	w.Write(data)
}
```

- [ ] **Step 4: Wire `SetPeerLister` in `cmd/gtron/main.go`**

Add `"net"` and `"strconv"` to the import block in `cmd/gtron/main.go`. The existing import block already has many entries; add the two standard library imports:

```go
import (
	"crypto/ecdsa"
	"fmt"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/tronprotocol/go-tron/consensus/dpos"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/producer"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/txpool"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/tronprotocol/go-tron/node"
	"github.com/tronprotocol/go-tron/p2p"
	"github.com/urfave/cli/v2"
)
```

Add `backend.SetPeerLister(...)` immediately after `backend.SetTxBroadcaster(broadcaster)` in the `gtron` function. The current code reads:

```go
broadcaster.SetPeersFunc(handler.HandshakedPeers)
backend.SetTxBroadcaster(broadcaster)
```

Replace with:

```go
broadcaster.SetPeersFunc(handler.HandshakedPeers)
backend.SetTxBroadcaster(broadcaster)
backend.SetPeerLister(func() []tronapi.PeerInfo {
	peers := handler.HandshakedPeers()
	result := make([]tronapi.PeerInfo, 0, len(peers))
	for _, p := range peers {
		host, portStr, err := net.SplitHostPort(p.ID())
		if err != nil {
			continue
		}
		port, _ := strconv.Atoi(portStr)
		result = append(result, tronapi.PeerInfo{Host: host, Port: port})
	}
	return result
})
```

- [ ] **Step 5: Run all new tests — expect all 10 to pass**

```bash
go test ./internal/tronapi/... -v -run "TestGetDelegated|TestCan|TestGetAvailable|TestGetReward|TestGetTransaction|TestListNodes" 2>&1
```

Expected: all 10 tests PASS.

- [ ] **Step 6: Commit**

```bash
git add core/tron_backend.go internal/tronapi/api.go cmd/gtron/main.go
git commit -m "feat(phase10): implement pool/network endpoints and wire PeerLister"
```

---

## Task 6: Full Build and Test Verification

**Files:** None (verification only)

- [ ] **Step 1: Build the complete project**

```bash
go build ./...
```

Expected: exits 0, no errors.

- [ ] **Step 2: Run all tests**

```bash
go test ./... 2>&1
```

Expected: all packages pass. The output should include lines like:
```
ok  	github.com/tronprotocol/go-tron/internal/tronapi	X.XXXs
ok  	github.com/tronprotocol/go-tron/core/txpool		X.XXXs
ok  	github.com/tronprotocol/go-tron/actuator		X.XXXs
ok  	github.com/tronprotocol/go-tron/core/state		X.XXXs
```

- [ ] **Step 3: Build the release binary**

```bash
go build -o build/bin/gtron ./cmd/gtron/
```

Expected: exits 0.

---

## Task 7: System Test Section 9

**Files:**
- Modify: `scripts/system_test.sh`

- [ ] **Step 1: Append Section 9 to `scripts/system_test.sh`**

Add the following block immediately before the `# Summary: print last few lines of logs` comment (which is just before `echo "=== SR Log (last 10 lines) ==="`):

```bash
# ─────────────────────────────────────────────────────────────────
# SECTION 9: Phase 10 Query APIs
# ─────────────────────────────────────────────────────────────────
echo ""
echo "=== Test Group 8: Phase 10 Query APIs ==="

# 9.1 getdelegatedresourcev2 — no delegation in dev chain → empty list
RESULT=$(http_post $SR_HTTP "/wallet/getdelegatedresourcev2" \
    "{\"fromAddress\": \"$WITNESS_ADDR\", \"toAddress\": \"$WITNESS_ADDR\"}")
check "getdelegatedresourcev2 returns delegatedResource key" "$RESULT" '"delegatedResource"'

# 9.2 getdelegatedresourceaccountindexv2 — no delegation → empty toAddresses
RESULT=$(http_post $SR_HTTP "/wallet/getdelegatedresourceaccountindexv2" \
    "{\"value\": \"$WITNESS_ADDR\"}")
check "getdelegatedresourceaccountindexv2 returns toAddresses key" "$RESULT" '"toAddresses"'

# 9.3 candelegateresource — witness has no frozen → maxSize=0
RESULT=$(http_post $SR_HTTP "/wallet/candelegateresource" \
    "{\"ownerAddress\": \"$WITNESS_ADDR\", \"balance\": 0, \"type\": 0}")
check "candelegateresource returns maxSize key" "$RESULT" '"maxSize"'

# 9.4 getcanwithdrawunfreezeamount — no pending unfreezes → amount=0
RESULT=$(http_post $SR_HTTP "/wallet/getcanwithdrawunfreezeamount" \
    "{\"ownerAddress\": \"$WITNESS_ADDR\", \"timestamp\": 9999999999999}")
check "getcanwithdrawunfreezeamount returns amount key" "$RESULT" '"amount"'

# 9.5 getavailableunfreezecount — no pending unfreezes → count=32
RESULT=$(http_post $SR_HTTP "/wallet/getavailableunfreezecount" \
    "{\"ownerAddress\": \"$WITNESS_ADDR\"}")
COUNT9=$(json_field "d.get('count',0)" "$RESULT" || echo "0")
check_eq "getavailableunfreezecount returns 32 for fresh account" "$COUNT9" "32"

# 9.6 getreward — witness earns allowance after block production
RESULT=$(http_post $SR_HTTP "/wallet/getreward" \
    "{\"address\": \"$WITNESS_ADDR\"}")
check "getreward returns reward key" "$RESULT" '"reward"'

# 9.7 gettransactionfrompending — tx not in pool → Error field
RESULT=$(http_post $SR_HTTP "/wallet/gettransactionfrompending" \
    '{"value":"0000000000000000000000000000000000000000000000000000000000000000"}')
check "gettransactionfrompending returns Error for unknown txid" "$RESULT" '"Error"'

# 9.8 gettransactionlistfrompending — pool should be empty between blocks
RESULT=$(http_post $SR_HTTP "/wallet/gettransactionlistfrompending" '{}')
check "gettransactionlistfrompending returns transaction key" "$RESULT" '"transaction"'

# 9.9 listnodes — SR should see at least 0 nodes (relay node connects to SR)
RESULT=$(http_get $SR_HTTP "/wallet/listnodes")
check "listnodes returns nodes key" "$RESULT" '"nodes"'
```

- [ ] **Step 2: Run the system test to verify all sections pass**

```bash
bash scripts/system_test.sh 2>&1 | tail -40
```

Expected: output ends with `Results: N passed, 0 failed, 0 skipped`. The new section 9 should contribute 9 passes.

- [ ] **Step 3: Commit**

```bash
git add scripts/system_test.sh
git commit -m "test(phase10): add system test section 9 for all 9 new query endpoints"
```
