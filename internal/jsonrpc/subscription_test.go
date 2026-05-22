package jsonrpc_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// wsTestBackend wraps stubBackend and captures the block subscription channel
// so tests can inject blocks directly.
type wsTestBackend struct {
	stubBackend
	subCh chan<- *types.Block
}

func (b *wsTestBackend) SubscribeBlocks(ch chan<- *types.Block)  { b.subCh = ch }
func (b *wsTestBackend) UnsubscribeBlocks(_ chan<- *types.Block) {}

func (b *wsTestBackend) injectBlock(block *types.Block) {
	if b.subCh != nil {
		b.subCh <- block
	}
}

// newWSTestServer creates an httptest.Server backed by the production
// NewServer handler (the reflection framework for HTTP, the subscription
// manager for WS), so these tests exercise the real cutover path. The returned
// backend can inject blocks via the FilterManager that NewServer wires up.
func newWSTestServer(t *testing.T) (*httptest.Server, *wsTestBackend) {
	t.Helper()
	backend := &wsTestBackend{}
	jrpc := jsonrpc.NewServer(backend, 0)
	srv := httptest.NewServer(jrpc.Handler())
	t.Cleanup(srv.Close)
	t.Cleanup(func() { _ = jrpc.Stop() })
	return srv, backend
}

// wsURL converts an http:// URL to ws://.
func wsURL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

// dialWS connects a WebSocket client to the test server.
func dialWS(t *testing.T, srv *httptest.Server) *websocket.Conn {
	t.Helper()
	conn, _, err := websocket.DefaultDialer.Dial(wsURL(srv), nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// rpcMsg serialises a JSON-RPC request.
func rpcMsg(method string, params interface{}) []byte {
	data, _ := json.Marshal(map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  method,
		"params":  params,
		"id":      1,
	})
	return data
}

// readWSMsg reads one text message from conn with a 2-second deadline.
func readWSMsg(t *testing.T, conn *websocket.Conn) map[string]interface{} {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("read ws message: %v", err)
	}
	conn.SetReadDeadline(time.Time{})
	var msg map[string]interface{}
	if err := json.Unmarshal(data, &msg); err != nil {
		t.Fatalf("unmarshal ws message: %v", err)
	}
	return msg
}

// makeBlock creates a minimal block at the given block number.
func makeBlock(num uint64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: int64(num)},
		},
	})
}

// ── Tests ─────────────────────────────────────────────────────────────────────

// TestWS_Subscribe_NewHeads verifies that a newHeads subscriber receives a
// block header push for each injected block.
func TestWS_Subscribe_NewHeads(t *testing.T) {
	srv, backend := newWSTestServer(t)
	conn := dialWS(t, srv)

	// Subscribe
	conn.WriteMessage(websocket.TextMessage, rpcMsg("eth_subscribe", []interface{}{"newHeads"}))
	resp := readWSMsg(t, conn)
	subID, ok := resp["result"].(string)
	if !ok || subID == "" {
		t.Fatalf("expected subscription ID, got %v", resp)
	}

	// Inject a block
	block := makeBlock(42)
	backend.injectBlock(block)

	// Expect a push
	push := readWSMsg(t, conn)
	if push["method"] != "eth_subscription" {
		t.Fatalf("expected eth_subscription push, got method=%v", push["method"])
	}
	params, ok := push["params"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing params in push: %v", push)
	}
	if params["subscription"] != subID {
		t.Fatalf("push subscription ID mismatch: want %s, got %v", subID, params["subscription"])
	}
	result, ok := params["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected block object in result, got %T", params["result"])
	}
	if result["number"] != "0x2a" {
		t.Fatalf("expected block number 0x2a, got %v", result["number"])
	}
}

// TestWS_Subscribe_Logs verifies that a logs subscriber receives only matching logs.
func TestWS_Subscribe_Logs(t *testing.T) {
	srv, backend := newWSTestServer(t)
	conn := dialWS(t, srv)

	addr := "0x0000000000000000000000000000000000000001"
	filter := map[string]interface{}{
		"address": addr,
	}
	conn.WriteMessage(websocket.TextMessage,
		rpcMsg("eth_subscribe", []interface{}{"logs", filter}))
	resp := readWSMsg(t, conn)
	subID, ok := resp["result"].(string)
	if !ok || subID == "" {
		t.Fatalf("expected subscription ID, got %v", resp)
	}

	// Backend returns a matching log when GetTransactionInfo is called.
	// We wire a txInfo with a log at the target address.
	matchingAddr := make([]byte, 21) // 21 bytes: 1 byte prefix + 20 bytes address
	matchingAddr[20] = 0x01
	backend.stubBackend.txInfo = &corepb.TransactionInfo{
		Log: []*corepb.TransactionInfo_Log{
			{Address: matchingAddr, Topics: nil, Data: []byte{0xde}},
		},
	}

	// Build a block with one dummy transaction so fanOut calls GetTransactionInfo.
	rawTx := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{},
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 7},
		},
		Transactions: []*corepb.Transaction{rawTx},
	})
	backend.injectBlock(block)

	// Expect a log push.
	push := readWSMsg(t, conn)
	if push["method"] != "eth_subscription" {
		t.Fatalf("expected eth_subscription push, got method=%v", push["method"])
	}
	params := push["params"].(map[string]interface{})
	log, ok := params["result"].(map[string]interface{})
	if !ok {
		t.Fatalf("expected log object, got %T", params["result"])
	}
	if log["data"] != "0xde" {
		t.Fatalf("unexpected log data: %v", log["data"])
	}
}

// TestWS_Unsubscribe verifies that after unsubscribing no further pushes arrive.
func TestWS_Unsubscribe(t *testing.T) {
	srv, backend := newWSTestServer(t)
	conn := dialWS(t, srv)

	// Subscribe
	conn.WriteMessage(websocket.TextMessage, rpcMsg("eth_subscribe", []interface{}{"newHeads"}))
	resp := readWSMsg(t, conn)
	subID := resp["result"].(string)

	// Unsubscribe
	conn.WriteMessage(websocket.TextMessage, rpcMsg("eth_unsubscribe", []interface{}{subID}))
	unsubResp := readWSMsg(t, conn)
	if unsubResp["result"] != true {
		t.Fatalf("eth_unsubscribe should return true, got %v", unsubResp["result"])
	}

	// Inject a block — should not produce a push.
	backend.injectBlock(makeBlock(1))

	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	_, _, err := conn.ReadMessage()
	if err == nil {
		t.Fatal("expected no push after unsubscribe, but got a message")
	}
	conn.SetReadDeadline(time.Time{})
}

// TestWS_HTTP_Coexistence verifies that plain HTTP POST still works on the
// same server while a WebSocket client is connected.
func TestWS_HTTP_Coexistence(t *testing.T) {
	srv, _ := newWSTestServer(t)
	_ = dialWS(t, srv) // connect but don't do anything

	// Plain HTTP call
	body := strings.NewReader(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)
	httpResp, err := http.Post(srv.URL, "application/json", body)
	if err != nil {
		t.Fatalf("HTTP POST: %v", err)
	}
	defer httpResp.Body.Close()
	var result map[string]interface{}
	if err := json.NewDecoder(httpResp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := result["result"].(string); !ok {
		t.Fatalf("expected hex string result, got %v", result)
	}
}
