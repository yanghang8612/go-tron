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
		`{"owner_address":"4101","balance":500000,"type":0}`)
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
		`{"owner_address":"4101","timestamp":1712345678000}`)
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
		`{"owner_address":"4101"}`)
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
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	nodes, ok := result["nodes"].([]interface{})
	if !ok || len(nodes) != 1 {
		t.Fatalf("expected nodes=[1 entry], got %v", result)
	}
	node, ok := nodes[0].(map[string]interface{})
	if !ok {
		t.Fatalf("nodes[0] is not an object")
	}
	addr, ok := node["address"].(map[string]interface{})
	if !ok {
		t.Fatalf("node address is not an object")
	}
	if addr["host"] != "127.0.0.1" {
		t.Fatalf("unexpected host: %v", addr["host"])
	}
}
