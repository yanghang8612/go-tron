package jsonrpc_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/rpc"
)

// postParity fires one JSON-RPC request at url and asserts the response
// "result" equals wantResult (compared as raw JSON), failing on any JSON-RPC
// error. Shared by the eth framework-parity tests.
type rpcErrObj struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

// postRPC fires one JSON-RPC request and returns the parsed result and error.
func postRPC(t *testing.T, url, body string) (json.RawMessage, *rpcErrObj) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	var got struct {
		Result json.RawMessage `json:"result"`
		Error  *rpcErrObj      `json:"error"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	return got.Result, got.Error
}

// postParity asserts the request yields wantResult (compared semantically) with
// no JSON-RPC error.
func postParity(t *testing.T, url, body, wantResult string) {
	t.Helper()
	result, errObj := postRPC(t, url, body)
	if errObj != nil {
		t.Fatalf("unexpected JSON-RPC error: %+v", errObj)
	}
	if !jsonSemanticEqual(result, []byte(wantResult)) {
		t.Fatalf("result mismatch:\n got = %s\nwant = %s", result, wantResult)
	}
}

// jsonSemanticEqual reports whether two JSON documents are equal ignoring
// object key order and insignificant whitespace — so object results (blocks,
// receipts) compare correctly regardless of map iteration order.
func jsonSemanticEqual(a, b []byte) bool {
	var av, bv interface{}
	if err := json.Unmarshal(a, &av); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &bv); err != nil {
		return false
	}
	return reflect.DeepEqual(av, bv)
}

// ethParityServer registers EthAPI (over the deterministic freeze backend) on a
// fresh framework server and returns its test HTTP endpoint.
func ethParityServer(t *testing.T) *httptest.Server {
	t.Helper()
	be := newFreezeBackend()
	srv := rpc.NewServer()
	if err := srv.RegisterName("eth", jsonrpc.NewEthAPI(be, jsonrpc.NewFilterManager(be))); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	t.Cleanup(srv.Stop)
	ts := httptest.NewServer(srv)
	t.Cleanup(ts.Close)
	return ts
}

// TestEthAPI_SimpleFrameworkParity proves the no-parameter eth methods dispatch
// through internal/rpc to EthAPI with output byte-identical to the frozen
// corpus (chainID=728126428, head=0x64, gasPrice=0x1a4).
func TestEthAPI_SimpleFrameworkParity(t *testing.T) {
	ts := ethParityServer(t)
	cases := []struct{ name, body, wantResult string }{
		{"chainId", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`, `"0x2b6653dc"`},
		{"blockNumber", `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`, `"0x64"`},
		{"syncing", `{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}`, `false`},
		{"gasPrice", `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`, `"0x1a4"`},
		{"accounts", `{"jsonrpc":"2.0","id":1,"method":"eth_accounts","params":[]}`, `[]`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { postParity(t, ts.URL, tc.body, tc.wantResult) })
	}
}

// TestEthAPI_AccountFrameworkParity proves the param-bearing account readers
// (getBalance/getTransactionCount/getCode/getStorageAt) dispatch through the
// framework — including TRON 21-byte address parsing, the optional "latest"
// block tag, and getBalance's 1e12 scaling — with output byte-identical to the
// frozen corpus.
func TestEthAPI_AccountFrameworkParity(t *testing.T) {
	ts := ethParityServer(t)
	const addr = "0x4101020304050607080900010203040506070809"
	cases := []struct{ name, body, wantResult string }{
		{
			"getBalance",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getBalance","params":["` + addr + `","latest"]}`,
			`"0xde0b6b3a7640000"`,
		},
		{
			"getTransactionCount",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionCount","params":["` + addr + `","latest"]}`,
			`"0x0"`,
		},
		{
			"getCode",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getCode","params":["` + addr + `","latest"]}`,
			`"0x6080604052"`,
		},
		{
			"getStorageAt",
			`{"jsonrpc":"2.0","id":1,"method":"eth_getStorageAt","params":["` + addr + `","0x0","latest"]}`,
			`"0x00000000000000000000000000000000000000000000000000000000deadbeef"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { postParity(t, ts.URL, tc.body, tc.wantResult) })
	}
}

// TestEthAPI_CallFrameworkParity proves eth_call and eth_estimateGas dispatch
// through the framework — including the {from,to,data,value} object param and
// the optional/ignored block tag — with output byte-identical to the frozen
// corpus.
func TestEthAPI_CallFrameworkParity(t *testing.T) {
	ts := ethParityServer(t)
	const to = "0x41a0b0c0d0e0f000102030405060708090a0b0c0d0"
	cases := []struct{ name, body, wantResult string }{
		{
			"call",
			`{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{"data":"0x70a08231","to":"` + to + `"},"latest"]}`,
			`"0x0000000000000000000000000000000000000000000000000000000000000001"`,
		},
		{
			"estimateGas",
			`{"jsonrpc":"2.0","id":1,"method":"eth_estimateGas","params":[{"data":"0x70a08231","to":"` + to + `"}]}`,
			`"0x0"`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) { postParity(t, ts.URL, tc.body, tc.wantResult) })
	}
}

// TestEthAPI_BlockTxFrameworkParity proves the block/tx/receipt readers dispatch
// through the framework with output identical to the (post-hash-fix) frozen
// corpus. They reuse the shared blockToRPC/txToRPC/receiptToRPC converters, so
// this guards the framework's param parsing (block tag, fullTx flag, tx hash)
// against the very fixtures the legacy handler is frozen against.
func TestEthAPI_BlockTxFrameworkParity(t *testing.T) {
	ts := ethParityServer(t)
	for _, name := range []string{
		"eth_getBlockByNumber_fullTx",
		"eth_getBlockByNumber_hashesOnly",
		"eth_getBlockByHash_fullTx",
		"eth_getBlockByHash_hashesOnly",
		"eth_getTransactionByHash",
		"eth_getTransactionReceipt",
	} {
		t.Run(name, func(t *testing.T) {
			req, wantResult := loadCorpusCase(t, name)
			postParity(t, ts.URL, req, wantResult)
		})
	}
}

// loadCorpusCase reads a freeze-corpus file and returns its request JSON and
// expected result JSON.
func loadCorpusCase(t *testing.T, name string) (reqJSON, resultJSON string) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("fixtures", "jsonrpc-corpus", name+".json"))
	if err != nil {
		t.Fatalf("read corpus %s: %v", name, err)
	}
	var c struct {
		Request  json.RawMessage `json:"request"`
		Response struct {
			Result json.RawMessage `json:"result"`
		} `json:"response"`
	}
	if err := json.Unmarshal(raw, &c); err != nil {
		t.Fatalf("parse corpus %s: %v", name, err)
	}
	return string(c.Request), string(c.Response.Result)
}

// TestEthAPI_GetLogsFrameworkParity proves eth_getLogs dispatches through the
// framework with output identical to the frozen corpus, exercising the
// LogFilter object param (block range, single address, single-topic matrix).
func TestEthAPI_GetLogsFrameworkParity(t *testing.T) {
	ts := ethParityServer(t)
	req, wantResult := loadCorpusCase(t, "eth_getLogs")
	postParity(t, ts.URL, req, wantResult)
}

// TestEthAPI_FilterFrameworkParity proves the stateful filter methods dispatch
// through the framework. newFilter/newBlockFilter return random ids (matched by
// pattern, as the corpus does); uninstall/getFilterChanges/getFilterLogs are
// exercised against a never-installed id (deterministic false / not-found).
func TestEthAPI_FilterFrameworkParity(t *testing.T) {
	ts := ethParityServer(t)
	idRE := regexp.MustCompile(`^0x[0-9a-f]{32}$`)
	const unknownID = "0xdeadbeefdeadbeefdeadbeefdeadbeef"

	assertID := func(t *testing.T, body string) {
		t.Helper()
		result, errObj := postRPC(t, ts.URL, body)
		if errObj != nil {
			t.Fatalf("unexpected error: %+v", errObj)
		}
		var id string
		if err := json.Unmarshal(result, &id); err != nil || !idRE.MatchString(id) {
			t.Fatalf("filter id %s does not match %s (err=%v)", result, idRE, err)
		}
	}

	t.Run("newFilter", func(t *testing.T) {
		assertID(t, `{"jsonrpc":"2.0","id":1,"method":"eth_newFilter","params":[{"fromBlock":"0x0","toBlock":"latest"}]}`)
	})
	t.Run("newBlockFilter", func(t *testing.T) {
		assertID(t, `{"jsonrpc":"2.0","id":1,"method":"eth_newBlockFilter","params":[]}`)
	})
	t.Run("uninstallFilter_unknown", func(t *testing.T) {
		postParity(t, ts.URL,
			`{"jsonrpc":"2.0","id":1,"method":"eth_uninstallFilter","params":["`+unknownID+`"]}`, `false`)
	})
	for _, m := range []string{"eth_getFilterChanges", "eth_getFilterLogs"} {
		t.Run(m+"_unknown", func(t *testing.T) {
			if _, errObj := postRPC(t, ts.URL,
				`{"jsonrpc":"2.0","id":1,"method":"`+m+`","params":["`+unknownID+`"]}`); errObj == nil {
				t.Fatalf("%s on unknown id: expected JSON-RPC error, got none", m)
			}
		})
	}
}
