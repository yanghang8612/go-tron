package tronapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	// protojson encodes int64 as string
	numStr := raw["number"].(string)
	if numStr != "100" {
		t.Fatalf("expected block 100, got %v", numStr)
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
