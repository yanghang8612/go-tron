package jsonrpc

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/internal/rpc"
)

// TestWeb3API_FrameworkParity proves the vendored internal/rpc reflection
// server dispatches the "web3" namespace to Web3API and returns byte-identical
// results to the legacy hand-rolled handler — the same values frozen in
// fixtures/jsonrpc-corpus (web3_clientVersion, web3_sha3). This validates the
// framework integration for a real go-tron namespace ahead of the live cutover
// that will delete the corresponding arms of api.go's dispatch switch.
func TestWeb3API_FrameworkParity(t *testing.T) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("web3", new(Web3API)); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	defer srv.Stop()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cases := []struct {
		name, body, wantResult string
	}{
		{
			name:       "clientVersion",
			body:       `{"jsonrpc":"2.0","id":1,"method":"web3_clientVersion","params":[]}`,
			wantResult: `"go-tron/v0.3.0-dev"`,
		},
		{
			name:       "sha3",
			body:       `{"jsonrpc":"2.0","id":1,"method":"web3_sha3","params":["0x68656c6c6f"]}`,
			wantResult: `"0x1c8aff950685c2ed4bc3174f3472287b56d9517b9c948127319a09a7a36deac8"`,
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
