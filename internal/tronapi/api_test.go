package tronapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type mockBackend struct{}

func (m *mockBackend) CurrentBlock() *types.Block {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    100,
				Timestamp: 300000,
			},
		},
	}
	return types.NewBlockFromPB(pb)
}

func (m *mockBackend) GetBlockByNumber(num uint64) (*types.Block, error) {
	pb := &corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    int64(num),
				Timestamp: int64(num) * 3000,
			},
		},
	}
	return types.NewBlockFromPB(pb), nil
}

func (m *mockBackend) GetAccount(addr common.Address) (*types.Account, error) {
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetBalance(5_000_000)
	return acc, nil
}

func (m *mockBackend) BroadcastTransaction(tx *types.Transaction) error {
	return nil
}

func (m *mockBackend) GetNodeInfo() *NodeInfo {
	return &NodeInfo{Version: "test", CurrentBlock: 100}
}

func (m *mockBackend) PendingTransactionCount() int {
	return 0
}

func (m *mockBackend) GetContract(addr common.Address) (*contractpb.SmartContract, error) {
	return nil, fmt.Errorf("contract not found")
}

func (m *mockBackend) TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*TriggerResult, error) {
	return &TriggerResult{Result: []byte{0x42}, EnergyUsed: 100}, nil
}

func (m *mockBackend) GetTransactionByID(txHash common.Hash) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not found")
}

func (m *mockBackend) GetTransactionInfoByID(txHash common.Hash) (*corepb.TransactionInfo, error) {
	return nil, fmt.Errorf("not found")
}

func (m *mockBackend) GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error) {
	return nil, nil
}

func (m *mockBackend) GetBlockByHash(hash common.Hash) (*types.Block, error) {
	return nil, fmt.Errorf("not found")
}

func (m *mockBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	var blocks []*types.Block
	for i := start; i < end; i++ {
		b, _ := m.GetBlockByNumber(i)
		blocks = append(blocks, b)
	}
	return blocks, nil
}

func (m *mockBackend) BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}

func (m *mockBackend) BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte,
	feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}

func (m *mockBackend) BuildTriggerContractTransaction(owner, contract common.Address, data []byte,
	feeLimit int64, callValue int64) (*corepb.Transaction, *TriggerResult, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, &TriggerResult{Result: []byte{0x42}, EnergyUsed: 100}, nil
}

func (m *mockBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	return 21000, nil
}

func (m *mockBackend) GetAccountResource(addr common.Address) (*AccountResource, error) {
	return &AccountResource{
		FreeNetLimit:     1500,
		TotalNetLimit:    43200000000,
		TotalEnergyLimit: 50000000000,
	}, nil
}

func (m *mockBackend) GetChainParameters() []ChainParameter {
	return []ChainParameter{
		{Key: "energy_fee", Value: 420},
	}
}

func (m *mockBackend) ListWitnesses() ([]*WitnessInfo, error) {
	return []*WitnessInfo{
		{Address: "4100000000000000000000000000000000000001", VoteCount: 100, IsJobs: true},
	}, nil
}

func (m *mockBackend) NextMaintenanceTime() int64 {
	return 1700000000000
}

func (m *mockBackend) BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockBackend) BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockBackend) BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error) {
	return nil, fmt.Errorf("not implemented")
}

func (m *mockBackend) ListProposals() ([]*ProposalInfo, error) {
	return nil, nil
}

func TestGetNowBlock(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("GET", "/wallet/getnowblock", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	header := result["block_header"].(map[string]interface{})
	raw := header["raw_data"].(map[string]interface{})
	// tronapi encodes int64 as number (matching java-tron)
	numVal := raw["number"].(float64)
	if numVal != 100 {
		t.Fatalf("expected block 100, got %v", numVal)
	}
	// Check blockID is present
	if _, ok := result["blockID"]; !ok {
		t.Fatal("expected blockID field in response")
	}
}

func TestGetBlockByNum(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/getblockbynum", nil)
	q := req.URL.Query()
	q.Add("num", "42")
	req.URL.RawQuery = q.Encode()
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
}

func TestEstimateEnergy(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	body := `{"owner_address":"4101","contract_address":"4102","data":"00"}`
	req := httptest.NewRequest("POST", "/wallet/estimateenergy", strings.NewReader(body))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	if _, ok := result["energy_required"]; !ok {
		t.Fatal("expected energy_required in response")
	}
}

func TestGetChainParameters(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/getchainparameters", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	if _, ok := result["chainParameter"]; !ok {
		t.Fatal("expected chainParameter in response")
	}
}

func TestListWitnesses(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/listwitnesses", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]interface{}
	json.NewDecoder(w.Body).Decode(&result)
	if _, ok := result["witnesses"]; !ok {
		t.Fatal("expected witnesses in response")
	}
}

func TestGetNextMaintenanceTime(t *testing.T) {
	api := NewAPI(&mockBackend{})
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)

	req := httptest.NewRequest("POST", "/wallet/getnextmaintenancetime", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var result map[string]int64
	json.NewDecoder(w.Body).Decode(&result)
	if result["num"] != 1700000000000 {
		t.Fatalf("expected 1700000000000, got %d", result["num"])
	}
}
