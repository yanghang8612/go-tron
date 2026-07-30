package jsonrpc_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

// Slice 7 of the State History Index: JSON-RPC archive-query handler tests.
//
// These exercise the block-tag plumbing added to historical account reads and
// read-only TVM execution — namely that a non-"latest" block argument routes
// to the backend's *At (history-reader-backed) methods, "latest"/absent routes
// to the live read, and a backend gate error (history disabled) surfaces as a
// JSON-RPC error rather than a wrong value. The reconstruction itself is
// covered at the reader / TronBackend layers; here we only validate the
// handler's routing + error mapping.

const archiveTestAddr = "0x4101020304050607080900010203040506070809"

// sunToWeiHex mirrors the handler's SUN→wei scaling (× 1e12) for building
// expected eth_getBalance results.
func sunToWeiHex(sun int64) string {
	wei := new(big.Int).Mul(big.NewInt(sun), big.NewInt(1_000_000_000_000))
	return fmt.Sprintf("0x%x", wei)
}

// rpcCallRaw issues a JSON-RPC call and returns the decoded response WITHOUT
// failing on a JSON-RPC "error" field, so a test can assert an error was
// returned (the shared rpcCall helper fails the test on any error).
func rpcCallRaw(t *testing.T, srv *httptest.Server, method string, params interface{}) map[string]interface{} {
	t.Helper()
	body, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0", "method": method, "params": params, "id": 1,
	})
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", method, err)
	}
	defer resp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode %s: %v", method, err)
	}
	return result
}

// TestEthGetBalance_ArchiveBlock asserts that a numeric block argument routes
// to GetBalanceAt (the archive path), not GetBalance (live). The stub returns
// distinct values for the two paths so the handler's choice is observable.
func TestEthGetBalance_ArchiveBlock(t *testing.T) {
	// live = 1_000_000 SUN, archive = 2_000_000 SUN.
	srv := newTestServer(t, &stubBackend{balance: 1_000_000, balanceAt: 2_000_000, blockNumber: 100})
	defer srv.Close()

	// "latest" → live path.
	live := rpcCall(t, srv, "eth_getBalance", []interface{}{archiveTestAddr, "latest"})
	if live["result"] != sunToWeiHex(1_000_000) {
		t.Errorf("latest balance = %v, want %v (live path)", live["result"], sunToWeiHex(1_000_000))
	}

	// A historical block number (hex and decimal forms) → archive path.
	for _, blockArg := range []interface{}{"0x5", "5"} {
		hist := rpcCall(t, srv, "eth_getBalance", []interface{}{archiveTestAddr, blockArg})
		if hist["result"] != sunToWeiHex(2_000_000) {
			t.Errorf("block %v balance = %v, want %v (archive path)", blockArg, hist["result"], sunToWeiHex(2_000_000))
		}
	}
}

// TestEthGetCode_ArchiveBlock asserts eth_getCode routes a numeric block to
// GetCodeAt.
func TestEthGetCode_ArchiveBlock(t *testing.T) {
	srv := newTestServer(t, &stubBackend{
		code:        []byte{0x60, 0x80},
		codeAt:      []byte{0xde, 0xad},
		blockNumber: 100,
	})
	defer srv.Close()

	if live := rpcCall(t, srv, "eth_getCode", []interface{}{archiveTestAddr, "latest"}); live["result"] != "0x6080" {
		t.Errorf("latest code = %v, want 0x6080", live["result"])
	}
	if hist := rpcCall(t, srv, "eth_getCode", []interface{}{archiveTestAddr, "0x2"}); hist["result"] != "0xdead" {
		t.Errorf("archive code = %v, want 0xdead", hist["result"])
	}
}

// TestEthGetStorageAt_ArchiveBlock asserts eth_getStorageAt routes a numeric
// block (the 3rd positional arg) to GetStorageAtBlock.
func TestEthGetStorageAt_ArchiveBlock(t *testing.T) {
	liveSlot := common.Hash{0x11}
	histSlot := common.Hash{0x22}
	srv := newTestServer(t, &stubBackend{storage: liveSlot, storageAt: histSlot, blockNumber: 100})
	defer srv.Close()

	live := rpcCall(t, srv, "eth_getStorageAt", []interface{}{archiveTestAddr, "0x0", "latest"})
	if want := fmt.Sprintf("0x%x", liveSlot[:]); live["result"] != want {
		t.Errorf("latest storage = %v, want %v (live slot)", live["result"], want)
	}
	hist := rpcCall(t, srv, "eth_getStorageAt", []interface{}{archiveTestAddr, "0x0", "0x3"})
	if want := fmt.Sprintf("0x%x", histSlot[:]); hist["result"] != want {
		t.Errorf("archive storage = %v, want %v (hist slot)", hist["result"], want)
	}
}

func TestEthCall_ArchiveBlock(t *testing.T) {
	backend := &stubBackend{
		callResult:   []byte{0x01},
		callAtResult: []byte{0x02},
		blockNumber:  100,
	}
	srv := newTestServer(t, backend)
	defer srv.Close()

	tx := map[string]interface{}{
		"to":   archiveTestAddr,
		"data": "0x70a08231",
	}
	live := rpcCall(t, srv, "eth_call", []interface{}{tx, "latest"})
	if live["result"] != "0x01" {
		t.Fatalf("latest eth_call = %v, want live result 0x01", live["result"])
	}
	hist := rpcCall(t, srv, "eth_call", []interface{}{tx, "0x7"})
	if hist["result"] != "0x02" {
		t.Fatalf("archive eth_call = %v, want archive result 0x02", hist["result"])
	}
	if backend.liveCallCalls != 1 || backend.callAtCalls != 1 || backend.callAtBlock != 7 {
		t.Fatalf("eth_call routing live=%d archive=%d block=%d, want 1/1/7",
			backend.liveCallCalls, backend.callAtCalls, backend.callAtBlock)
	}
}

func TestEthEstimateGas_ArchiveBlock(t *testing.T) {
	backend := &stubBackend{
		estimateGas:   5,
		estimateGasAt: 7,
		blockNumber:   100,
	}
	srv := newTestServer(t, backend)
	defer srv.Close()

	tx := map[string]interface{}{
		"to":   archiveTestAddr,
		"data": "0x70a08231",
	}
	live := rpcCall(t, srv, "eth_estimateGas", []interface{}{tx, "latest"})
	if live["result"] != "0x5" {
		t.Fatalf("latest eth_estimateGas = %v, want live result 0x5", live["result"])
	}
	hist := rpcCall(t, srv, "eth_estimateGas", []interface{}{tx, "0x8"})
	if hist["result"] != "0x7" {
		t.Fatalf("archive eth_estimateGas = %v, want archive result 0x7", hist["result"])
	}
	if backend.liveEstimateCalls != 1 || backend.estimateAtCalls != 1 || backend.estimateAtBlock != 8 {
		t.Fatalf("eth_estimateGas routing live=%d archive=%d block=%d, want 1/1/8",
			backend.liveEstimateCalls, backend.estimateAtCalls, backend.estimateAtBlock)
	}
}

// TestEthArchive_GateError asserts that when the backend's archive query
// returns an error (e.g. history disabled), the handler surfaces it as a
// JSON-RPC error for a historical block — but a "latest" query still
// succeeds via the live path that never calls the gated method.
func TestEthArchive_GateError(t *testing.T) {
	gate := errors.New("archive history not available: node not running with --history.enabled")
	srv := newTestServer(t, &stubBackend{balance: 1_000_000, atErr: gate, blockNumber: 100})
	defer srv.Close()

	// Historical block → archive path → error surfaced for archive reads and
	// read-only execution.
	if got := rpcCallRaw(t, srv, "eth_getBalance", []interface{}{archiveTestAddr, "0x5"}); got["error"] == nil {
		t.Errorf("eth_getBalance(block 5) with history disabled: expected error, got %v", got["result"])
	}
	if got := rpcCallRaw(t, srv, "eth_getCode", []interface{}{archiveTestAddr, "0x5"}); got["error"] == nil {
		t.Errorf("eth_getCode(block 5) with history disabled: expected error")
	}
	if got := rpcCallRaw(t, srv, "eth_getStorageAt", []interface{}{archiveTestAddr, "0x0", "0x5"}); got["error"] == nil {
		t.Errorf("eth_getStorageAt(block 5) with history disabled: expected error")
	}
	tx := map[string]interface{}{"to": archiveTestAddr, "data": "0x70a08231"}
	if got := rpcCallRaw(t, srv, "eth_call", []interface{}{tx, "0x5"}); got["error"] == nil {
		t.Errorf("eth_call(block 5) with history disabled: expected error")
	}
	if got := rpcCallRaw(t, srv, "eth_estimateGas", []interface{}{tx, "0x5"}); got["error"] == nil {
		t.Errorf("eth_estimateGas(block 5) with history disabled: expected error")
	}

	// "latest" → live path → no error even though atErr is set.
	live := rpcCall(t, srv, "eth_getBalance", []interface{}{archiveTestAddr, "latest"})
	if _, ok := live["result"].(string); !ok {
		t.Fatalf("eth_getBalance(latest) should succeed via live path, got %v", live)
	}
	if got := rpcCallRaw(t, srv, "eth_call", []interface{}{tx, "latest"}); got["error"] != nil {
		t.Fatalf("eth_call(latest) should succeed via live path, got %v", got["error"])
	}
	if got := rpcCallRaw(t, srv, "eth_estimateGas", []interface{}{tx, "latest"}); got["error"] != nil {
		t.Fatalf("eth_estimateGas(latest) should succeed via live path, got %v", got["error"])
	}
}
