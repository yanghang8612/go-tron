package jsonrpc_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/internal/jsonrpc"
	"github.com/tronprotocol/go-tron/internal/rpc"
)

// postParity fires one JSON-RPC request at url and asserts the response
// "result" equals wantResult (compared as raw JSON), failing on any JSON-RPC
// error. Shared by the eth framework-parity tests.
func postParity(t *testing.T, url, body, wantResult string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)

	var got struct {
		Result json.RawMessage `json:"result"`
		Error  *struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("decode %q: %v", raw, err)
	}
	if got.Error != nil {
		t.Fatalf("unexpected JSON-RPC error: %+v", got.Error)
	}
	if string(got.Result) != wantResult {
		t.Fatalf("result mismatch:\n got = %s\nwant = %s", got.Result, wantResult)
	}
}

// ethParityServer registers EthAPI (over the deterministic freeze backend) on a
// fresh framework server and returns its test HTTP endpoint.
func ethParityServer(t *testing.T) *httptest.Server {
	t.Helper()
	srv := rpc.NewServer()
	if err := srv.RegisterName("eth", jsonrpc.NewEthAPI(newFreezeBackend())); err != nil {
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
