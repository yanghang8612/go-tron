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

// TestNetAPI_FrameworkParity proves the vendored internal/rpc reflection server
// dispatches the "net" namespace to NetAPI and returns byte-identical results
// to the legacy hand-rolled handler — the same values frozen in
// fixtures/jsonrpc-corpus (net_version, net_listening, net_peerCount). It
// reuses the freeze suite's deterministic backend (chainID=728126428,
// peerCount=3), so the expectations stay locked to the corpus.
func TestNetAPI_FrameworkParity(t *testing.T) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("net", jsonrpc.NewNetAPI(newFreezeBackend())); err != nil {
		t.Fatalf("RegisterName: %v", err)
	}
	defer srv.Stop()
	ts := httptest.NewServer(srv)
	defer ts.Close()

	cases := []struct {
		name, body, wantResult string
	}{
		{
			name:       "version",
			body:       `{"jsonrpc":"2.0","id":1,"method":"net_version","params":[]}`,
			wantResult: `"728126428"`,
		},
		{
			name:       "listening",
			body:       `{"jsonrpc":"2.0","id":1,"method":"net_listening","params":[]}`,
			wantResult: `true`,
		},
		{
			name:       "peerCount",
			body:       `{"jsonrpc":"2.0","id":1,"method":"net_peerCount","params":[]}`,
			wantResult: `"0x3"`,
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
