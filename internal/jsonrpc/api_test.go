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
	gasPrice    int64
	peerCount   int
}

func (s *stubBackend) ChainID() int64      { return s.chainID }
func (s *stubBackend) BlockNumber() uint64  { return s.blockNumber }
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
func (s *stubBackend) GasPrice() int64 { return s.gasPrice }
func (s *stubBackend) PeerCount() int  { return s.peerCount }

// ── Test helpers ─────────────────────────────────────────────────────────────

func newTestServer(t *testing.T, backend jsonrpc.Backend) *httptest.Server {
	t.Helper()
	api := jsonrpc.NewAPI(backend)
	return httptest.NewServer(api)
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
	t.Run("normal slot", func(t *testing.T) {
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
	})

	t.Run("oversized slot (>32 bytes) does not panic", func(t *testing.T) {
		srv := newTestServer(t, &stubBackend{})
		defer srv.Close()
		// 66 hex chars after 0x = 33 decoded bytes — triggers the length guard
		resp := rpcCall(t, srv, "eth_getStorageAt", []interface{}{
			"0x4101020304050607080900010203040506070809",
			"0x000000000000000000000000000000000000000000000000000000000000000001",
			"latest",
		})
		if _, ok := resp["result"].(string); !ok {
			t.Fatalf("oversized slot: result should be a hex string, got %v", resp["result"])
		}
	})
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

func TestEthGasPrice(t *testing.T) {
	srv := newTestServer(t, &stubBackend{gasPrice: 420})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_gasPrice", []interface{}{})
	if resp["result"] != "0x1a4" {
		t.Fatalf("eth_gasPrice: expected 0x1a4, got %v", resp["result"])
	}
}

func TestWeb3Sha3(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	// keccak256("") = 0xc5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
	resp := rpcCall(t, srv, "web3_sha3", []interface{}{"0x"})
	got, ok := resp["result"].(string)
	if !ok || len(got) != 66 {
		t.Fatalf("web3_sha3: expected 66-char hex, got %v", resp["result"])
	}
}

func TestNetListening(t *testing.T) {
	t.Run("no peers", func(t *testing.T) {
		srv := newTestServer(t, &stubBackend{peerCount: 0})
		defer srv.Close()
		resp := rpcCall(t, srv, "net_listening", []interface{}{})
		if resp["result"] != false {
			t.Fatalf("net_listening with 0 peers: expected false, got %v", resp["result"])
		}
	})
	t.Run("has peers", func(t *testing.T) {
		srv := newTestServer(t, &stubBackend{peerCount: 3})
		defer srv.Close()
		resp := rpcCall(t, srv, "net_listening", []interface{}{})
		if resp["result"] != true {
			t.Fatalf("net_listening with 3 peers: expected true, got %v", resp["result"])
		}
	})
}

func TestNetPeerCount(t *testing.T) {
	srv := newTestServer(t, &stubBackend{peerCount: 7})
	defer srv.Close()
	resp := rpcCall(t, srv, "net_peerCount", []interface{}{})
	if resp["result"] != "0x7" {
		t.Fatalf("net_peerCount: expected 0x7, got %v", resp["result"])
	}
}

func TestEthAccounts(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	resp := rpcCall(t, srv, "eth_accounts", []interface{}{})
	accounts, ok := resp["result"].([]interface{})
	if !ok || len(accounts) != 0 {
		t.Fatalf("eth_accounts should return [], got %v", resp["result"])
	}
}

func TestWriteMethodsNotAvailable(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	for _, method := range []string{"eth_sendRawTransaction", "eth_sendTransaction", "eth_sign", "eth_signTransaction"} {
		body, _ := json.Marshal(map[string]interface{}{
			"jsonrpc": "2.0", "method": method, "params": []interface{}{}, "id": 1,
		})
		resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
		if err != nil {
			t.Fatalf("%s: %v", method, err)
		}
		defer resp.Body.Close()
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		errField, hasErr := result["error"].(map[string]interface{})
		if !hasErr {
			t.Fatalf("%s should return an error, got %v", method, result)
		}
		if errField["code"] != float64(-32601) {
			t.Fatalf("%s error code: expected -32601, got %v", method, errField["code"])
		}
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
