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

// TestEthAPI_SimpleFrameworkParity proves the vendored internal/rpc reflection
// server dispatches the no-parameter "eth" methods to EthAPI and returns
// byte-identical results to the legacy hand-rolled handler — the same values
// frozen in fixtures/jsonrpc-corpus. It reuses the freeze suite's deterministic
// backend (chainID=728126428, head=0x64, gasPrice=0x1a4). The param-bearing and
// block/tx/receipt eth methods are covered by later increments.
func TestEthAPI_SimpleFrameworkParity(t *testing.T) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("eth", jsonrpc.NewEthAPI(newFreezeBackend())); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	defer srv.Stop()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cases := []struct {
		name, body, wantResult string
	}{
		{
			name:       "chainId",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`,
			wantResult: `"0x2b6653dc"`,
		},
		{
			name:       "blockNumber",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`,
			wantResult: `"0x64"`,
		},
		{
			name:       "syncing",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_syncing","params":[]}`,
			wantResult: `false`,
		},
		{
			name:       "gasPrice",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_gasPrice","params":[]}`,
			wantResult: `"0x1a4"`,
		},
		{
			name:       "accounts",
			body:       `{"jsonrpc":"2.0","id":1,"method":"eth_accounts","params":[]}`,
			wantResult: `[]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := http.Post(ts.URL, "application/json", strings.NewReader(tc.body))
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
			if string(got.Result) != tc.wantResult {
				t.Fatalf("result mismatch:\n got = %s\nwant = %s", got.Result, tc.wantResult)
			}
		})
	}
}
