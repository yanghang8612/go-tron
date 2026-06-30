package jsonrpc_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/rpc"
)

func debugServer(t *testing.T, be jsonrpc.Backend) *httptest.Server {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("debug", jsonrpc.NewDebugAPI(be)); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	t.Cleanup(srv.Stop)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

const debugTestAddr = "0x410102030405060708090a0b0c0d0e0f1011121314"

func TestDebugAPI_TraceCallRoutesAndParses(t *testing.T) {
	be := &stubBackend{traceResult: map[string]interface{}{"failed": true, "structLogs": []interface{}{}}}
	ts := debugServer(t, be)

	body := `{"jsonrpc":"2.0","id":1,"method":"debug_traceCall","params":[` +
		`{"to":"` + debugTestAddr + `","data":"0xabcd","value":"0x0"},"latest",{"disableStack":true}]}`
	result, errObj := postRPC(t, ts.URL, body)
	if errObj != nil {
		t.Fatalf("debug_traceCall error: %+v", errObj)
	}

	if be.gotTraceTo == nil {
		t.Fatal("'to' not forwarded to backend")
	}
	wantTo := common.BytesToAddress(common.FromHex(debugTestAddr))
	if *be.gotTraceTo != wantTo {
		t.Fatalf("to: got %x, want %x", be.gotTraceTo[:], wantTo[:])
	}
	if !bytes.Equal(be.gotTraceData, []byte{0xab, 0xcd}) {
		t.Fatalf("data: got %x, want abcd", be.gotTraceData)
	}
	if be.gotTraceBlock != nil {
		t.Fatalf("\"latest\" must map to a nil block (head), got %d", *be.gotTraceBlock)
	}
	if be.gotTraceCfg == nil || !be.gotTraceCfg.DisableStack {
		t.Fatalf("disableStack toggle not parsed into TraceConfig: %+v", be.gotTraceCfg)
	}

	var got map[string]interface{}
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if got["failed"] != true {
		t.Fatalf("backend trace result not echoed: %s", result)
	}
}

func TestDebugAPI_TraceCallArchiveBlock(t *testing.T) {
	be := &stubBackend{}
	ts := debugServer(t, be)

	body := `{"jsonrpc":"2.0","id":1,"method":"debug_traceCall","params":[` +
		`{"to":"` + debugTestAddr + `"},"0x10",null]}`
	if _, errObj := postRPC(t, ts.URL, body); errObj != nil {
		t.Fatalf("debug_traceCall error: %+v", errObj)
	}
	if be.gotTraceBlock == nil || *be.gotTraceBlock != 16 {
		t.Fatalf("block \"0x10\" must map to block 16, got %v", be.gotTraceBlock)
	}
}

func TestDebugAPI_TraceTransactionRoutesAndParses(t *testing.T) {
	be := &stubBackend{traceResult: map[string]interface{}{"type": "CALL"}}
	ts := debugServer(t, be)

	hash := "0x" + strings.Repeat("ab", 32)
	body := `{"jsonrpc":"2.0","id":1,"method":"debug_traceTransaction","params":["` + hash + `",{"tracer":"callTracer"}]}`
	result, errObj := postRPC(t, ts.URL, body)
	if errObj != nil {
		t.Fatalf("debug_traceTransaction error: %+v", errObj)
	}

	var wantHash common.Hash
	copy(wantHash[:], common.FromHex(hash))
	if be.gotTraceHash != wantHash {
		t.Fatalf("hash: got %x, want %x", be.gotTraceHash[:], wantHash[:])
	}
	if be.gotTraceCfg == nil || be.gotTraceCfg.Tracer == nil || *be.gotTraceCfg.Tracer != "callTracer" {
		t.Fatalf("tracer name not parsed into TraceConfig: %+v", be.gotTraceCfg)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(result, &got); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if got["type"] != "CALL" {
		t.Fatalf("backend trace result not echoed: %s", result)
	}
}

func TestDebugAPI_TraceBlockByNumberRoutesTransactions(t *testing.T) {
	block := buildFreezeBlock()
	txHash := block.Transactions()[0].Hash()
	be := &stubBackend{
		blockNumber: block.Number(),
		block:       block,
		traceResult: map[string]interface{}{"type": "CALL"},
	}
	ts := debugServer(t, be)

	body := `{"jsonrpc":"2.0","id":1,"method":"debug_traceBlockByNumber","params":["0x64",{"tracer":"callTracer"}]}`
	result, errObj := postRPC(t, ts.URL, body)
	if errObj != nil {
		t.Fatalf("debug_traceBlockByNumber error: %+v", errObj)
	}
	if len(be.gotTraceHashes) != 1 || be.gotTraceHashes[0] != txHash {
		t.Fatalf("TraceBlock hashes = %x, want %x", be.gotTraceHashes, txHash)
	}
	if be.gotTraceBlockObj != block {
		t.Fatalf("TraceBlock block = %p, want %p", be.gotTraceBlockObj, block)
	}
	if be.gotTraceCfg == nil || be.gotTraceCfg.Tracer == nil || *be.gotTraceCfg.Tracer != "callTracer" {
		t.Fatalf("tracer name not parsed into TraceConfig: %+v", be.gotTraceCfg)
	}

	var traces []map[string]interface{}
	if err := json.Unmarshal(result, &traces); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if len(traces) != 1 {
		t.Fatalf("trace count = %d, want 1: %s", len(traces), result)
	}
	traceResult, ok := traces[0]["result"].(map[string]interface{})
	if !ok || traceResult["type"] != "CALL" {
		t.Fatalf("trace result = %v, want call tracer payload", traces[0]["result"])
	}
	if traces[0]["txHash"] != "0x"+txHash.Hex() {
		t.Fatalf("txHash = %v, want 0x%s", traces[0]["txHash"], txHash.Hex())
	}
}

func TestDebugAPI_TraceBlockByHashRoutesTransactions(t *testing.T) {
	block := buildFreezeBlock()
	txHash := block.Transactions()[0].Hash()
	be := &stubBackend{
		blockNumber: block.Number(),
		block:       block,
		traceResult: map[string]interface{}{"gas": "0x1"},
	}
	ts := debugServer(t, be)

	body := `{"jsonrpc":"2.0","id":1,"method":"debug_traceBlockByHash","params":["` + freezeBlockHashHex() + `",null]}`
	result, errObj := postRPC(t, ts.URL, body)
	if errObj != nil {
		t.Fatalf("debug_traceBlockByHash error: %+v", errObj)
	}
	if len(be.gotTraceHashes) != 1 || be.gotTraceHashes[0] != txHash {
		t.Fatalf("TraceBlock hashes = %x, want %x", be.gotTraceHashes, txHash)
	}
	if be.gotTraceBlockObj != block {
		t.Fatalf("TraceBlock block = %p, want %p", be.gotTraceBlockObj, block)
	}
	var traces []map[string]interface{}
	if err := json.Unmarshal(result, &traces); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if len(traces) != 1 || traces[0]["txHash"] != "0x"+txHash.Hex() {
		t.Fatalf("debug_traceBlockByHash result = %+v, want one trace for tx %x", traces, txHash)
	}
}

func TestDebugAPI_TraceBlockReturnsPerTxTraceError(t *testing.T) {
	block := buildFreezeBlock()
	traceErr := errors.New("trace replay failed")
	be := &stubBackend{blockNumber: block.Number(), block: block, traceErr: traceErr}
	ts := debugServer(t, be)

	body := `{"jsonrpc":"2.0","id":1,"method":"debug_traceBlockByNumber","params":["0x64",null]}`
	result, errObj := postRPC(t, ts.URL, body)
	if errObj != nil {
		t.Fatalf("debug_traceBlockByNumber unexpected JSON-RPC error: %+v", errObj)
	}

	var traces []map[string]interface{}
	if err := json.Unmarshal(result, &traces); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if len(traces) != 1 || traces[0]["error"] != traceErr.Error() || traces[0]["result"] != nil {
		t.Fatalf("trace error result = %+v, want per-tx error only", traces)
	}
}

func TestDebugAPI_TraceBlockUnknownBlockErrors(t *testing.T) {
	be := &stubBackend{blockNumber: 100}
	ts := debugServer(t, be)

	body := `{"jsonrpc":"2.0","id":1,"method":"debug_traceBlockByNumber","params":["0x64",null]}`
	_, errObj := postRPC(t, ts.URL, body)
	if errObj == nil || !strings.Contains(errObj.Message, "block not found") {
		t.Fatalf("debug_traceBlockByNumber error = %+v, want block not found", errObj)
	}
}
