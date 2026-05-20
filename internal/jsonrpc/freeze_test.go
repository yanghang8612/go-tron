package jsonrpc_test

import (
	"bytes"
	"encoding/json"
	"flag"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"testing"

	"github.com/tronprotocol/go-tron/internal/jsonrpc"
)

// freeze_test.go is the black-box "freeze" regression suite for the
// Ethereum-compatible JSON-RPC HTTP handler (internal/jsonrpc/api.go).
//
// It locks the CURRENT request→response behavior of every method in the
// api.go dispatch switch into a JSON corpus under fixtures/jsonrpc-corpus/.
// The upcoming reflection-based dispatch migration must be zero-diff: the same
// request bytes must produce the same response JSON. This test fires every
// stored request at the live handler and asserts the response matches.
//
// Run `go test ./internal/jsonrpc/ -run TestFreeze -update` to (re)generate
// the corpus, then run without -update to verify the frozen behavior.
//
// Determinism: the backend is seeded with hand-pinned, byte-stable data (see
// freeze_fixtures_test.go). The only nondeterministic outputs are the random
// filter IDs returned by eth_newFilter / eth_newBlockFilter; those cases use a
// `response_pattern` (regex on result) instead of an exact `response`.

var updateCorpus = flag.Bool("update", false, "regenerate the jsonrpc freeze corpus")

const corpusDir = "fixtures/jsonrpc-corpus"

// nonexistentFilterID is a well-formed filter id that is never installed, so
// the filter-state methods return a deterministic "not found" outcome.
const nonexistentFilterID = "0xffffffffffffffffffffffffffffffff"

// corpusCase is one frozen request/response pair. Exactly one of Response or
// ResponsePattern is set:
//   - Response: the exact expected JSON-RPC response (compared by parsed value).
//   - ResponsePattern: a shape assertion for cases whose result is inherently
//     nondeterministic (random filter IDs).
type corpusCase struct {
	Name            string           `json:"name"`
	Request         json.RawMessage  `json:"request"`
	Response        json.RawMessage  `json:"response,omitempty"`
	ResponsePattern *responsePattern `json:"response_pattern,omitempty"`
}

// responsePattern asserts the shape of a nondeterministic response.
type responsePattern struct {
	// JSONRPC / ID are matched exactly.
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	// ResultRegex matches the (string) result field.
	ResultRegex string `json:"result_regex"`
}

// requestSpec describes how to build a corpus case before capture.
type requestSpec struct {
	name   string
	method string
	params interface{}
	// pattern, when non-empty, marks the case as shape-asserted (the result is
	// nondeterministic) and supplies the regex the result string must match.
	pattern string
}

// freezeSpecs returns the full ordered list of request specs. There is one
// spec per arm of the api.go dispatch switch, plus the representative error
// cases (unknown method, malformed params, not-supported writes).
func freezeSpecs() []requestSpec {
	acct := freezeAccountHex
	contract := freezeContractAddr21
	blockHash := freezeBlockHashHex()
	txHash := freezeTxHashHex()
	// Topic from the seeded log, used to exercise topic parsing in eth_getLogs.
	topic := "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

	return []requestSpec{
		// ── net_* / web3_* ──
		{name: "net_version", method: "net_version", params: []interface{}{}},
		{name: "net_listening", method: "net_listening", params: []interface{}{}},
		{name: "net_peerCount", method: "net_peerCount", params: []interface{}{}},
		{name: "web3_clientVersion", method: "web3_clientVersion", params: []interface{}{}},
		{name: "web3_sha3", method: "web3_sha3", params: []interface{}{"0x68656c6c6f"}}, // keccak256("hello")

		// ── eth_* chain metadata ──
		{name: "eth_chainId", method: "eth_chainId", params: []interface{}{}},
		{name: "eth_blockNumber", method: "eth_blockNumber", params: []interface{}{}},
		{name: "eth_syncing", method: "eth_syncing", params: []interface{}{}},
		{name: "eth_gasPrice", method: "eth_gasPrice", params: []interface{}{}},
		{name: "eth_accounts", method: "eth_accounts", params: []interface{}{}},

		// ── account state (latest path) ──
		{name: "eth_getBalance", method: "eth_getBalance", params: []interface{}{acct, "latest"}},
		{name: "eth_getTransactionCount", method: "eth_getTransactionCount", params: []interface{}{acct, "latest"}},
		{name: "eth_getCode", method: "eth_getCode", params: []interface{}{acct, "latest"}},
		{name: "eth_getStorageAt", method: "eth_getStorageAt", params: []interface{}{acct, "0x0", "latest"}},

		// ── TVM read-only ──
		{name: "eth_call", method: "eth_call", params: []interface{}{
			map[string]interface{}{"to": contract, "data": "0x70a08231"}, "latest",
		}},
		{name: "eth_estimateGas", method: "eth_estimateGas", params: []interface{}{
			map[string]interface{}{"to": contract, "data": "0x70a08231"},
		}},

		// ── block queries (both fullTx modes + by-hash) ──
		{name: "eth_getBlockByNumber_hashesOnly", method: "eth_getBlockByNumber", params: []interface{}{"latest", false}},
		{name: "eth_getBlockByNumber_fullTx", method: "eth_getBlockByNumber", params: []interface{}{"0x64", true}},
		{name: "eth_getBlockByHash_hashesOnly", method: "eth_getBlockByHash", params: []interface{}{blockHash, false}},
		{name: "eth_getBlockByHash_fullTx", method: "eth_getBlockByHash", params: []interface{}{blockHash, true}},

		// ── transaction queries ──
		{name: "eth_getTransactionByHash", method: "eth_getTransactionByHash", params: []interface{}{txHash}},
		{name: "eth_getTransactionReceipt", method: "eth_getTransactionReceipt", params: []interface{}{txHash}},

		// ── logs ──
		{name: "eth_getLogs", method: "eth_getLogs", params: []interface{}{
			map[string]interface{}{
				"fromBlock": "0x0",
				"toBlock":   "latest",
				"address":   contract,
				"topics":    []interface{}{topic},
			},
		}},

		// ── filters (creation results are random → shape-asserted) ──
		{name: "eth_newFilter", method: "eth_newFilter", params: []interface{}{
			map[string]interface{}{"fromBlock": "0x0", "toBlock": "latest"},
		}, pattern: `^0x[0-9a-f]{32}$`},
		{name: "eth_newBlockFilter", method: "eth_newBlockFilter", params: []interface{}{}, pattern: `^0x[0-9a-f]{32}$`},

		// filter-state methods against a never-installed id → deterministic.
		{name: "eth_uninstallFilter_unknown", method: "eth_uninstallFilter", params: []interface{}{nonexistentFilterID}},
		{name: "eth_getFilterChanges_unknown", method: "eth_getFilterChanges", params: []interface{}{nonexistentFilterID}},
		{name: "eth_getFilterLogs_unknown", method: "eth_getFilterLogs", params: []interface{}{nonexistentFilterID}},

		// ── write/not-supported methods (identical error arm in api.go:177) ──
		{name: "eth_sendRawTransaction", method: "eth_sendRawTransaction", params: []interface{}{"0xdeadbeef"}},
		{name: "eth_sendTransaction", method: "eth_sendTransaction", params: []interface{}{map[string]interface{}{}}},
		{name: "eth_sign", method: "eth_sign", params: []interface{}{acct, "0xdeadbeef"}},
		{name: "eth_signTransaction", method: "eth_signTransaction", params: []interface{}{map[string]interface{}{}}},

		// ── representative error cases ──
		{name: "error_unknownMethod", method: "eth_doesNotExist", params: []interface{}{}},
		// Empty params array hits the handler's own fmt.Errorf("invalid params")
		// path (not json.Unmarshal wording), keeping the message stable.
		{name: "error_malformedParams", method: "eth_getBalance", params: []interface{}{}},
	}
}

// buildRequestJSON marshals the canonical JSON-RPC envelope for a spec. The
// stored request bytes are exactly what the test fires at the handler.
func buildRequestJSON(t *testing.T, spec requestSpec) json.RawMessage {
	t.Helper()
	env := map[string]interface{}{
		"jsonrpc": "2.0",
		"method":  spec.method,
		"params":  spec.params,
		"id":      1,
	}
	b, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("%s: marshal request: %v", spec.name, err)
	}
	return b
}

// fire posts the given request body at a fresh handler and returns the raw
// response body. A fresh server per call keeps filter state from leaking
// between cases.
func fire(t *testing.T, reqBody []byte) []byte {
	t.Helper()
	srv := httptest.NewServer(jsonrpc.NewAPI(newFreezeBackend()))
	defer srv.Close()
	resp, err := http.Post(srv.URL, "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	return body
}

// TestFreeze_GenerateOrVerify drives both modes. With -update it (re)writes the
// corpus; without it, it loads and verifies the frozen corpus.
func TestFreeze_GenerateOrVerify(t *testing.T) {
	if *updateCorpus {
		generateCorpus(t)
		return
	}
	verifyCorpus(t)
}

// generateCorpus fires every spec at the handler and writes the captured
// response into a per-case corpus file.
func generateCorpus(t *testing.T) {
	if err := os.MkdirAll(corpusDir, 0o755); err != nil {
		t.Fatalf("mkdir corpus: %v", err)
	}
	for _, spec := range freezeSpecs() {
		reqJSON := buildRequestJSON(t, spec)
		respBody := fire(t, reqJSON)

		// Re-marshal the response through a generic decode so the stored bytes
		// are canonicalized (stable key order, no trailing newline).
		var respVal interface{}
		if err := json.Unmarshal(respBody, &respVal); err != nil {
			t.Fatalf("%s: response is not valid JSON: %v\nbody=%s", spec.name, err, respBody)
		}

		c := corpusCase{Name: spec.name, Request: reqJSON}
		if spec.pattern != "" {
			// Shape-asserted case: capture jsonrpc/id and the regex, not the
			// (random) result value.
			obj, _ := respVal.(map[string]interface{})
			id := 0
			if v, ok := obj["id"].(float64); ok {
				id = int(v)
			}
			jrpc, _ := obj["jsonrpc"].(string)
			// Sanity: the captured result must already match the regex.
			result, _ := obj["result"].(string)
			if !regexp.MustCompile(spec.pattern).MatchString(result) {
				t.Fatalf("%s: captured result %q does not match pattern %q", spec.name, result, spec.pattern)
			}
			c.ResponsePattern = &responsePattern{JSONRPC: jrpc, ID: id, ResultRegex: spec.pattern}
		} else {
			canon, err := json.MarshalIndent(respVal, "", "  ")
			if err != nil {
				t.Fatalf("%s: canonicalize response: %v", spec.name, err)
			}
			c.Response = canon
		}

		out, err := json.MarshalIndent(c, "", "  ")
		if err != nil {
			t.Fatalf("%s: marshal corpus case: %v", spec.name, err)
		}
		path := filepath.Join(corpusDir, spec.name+".json")
		if err := os.WriteFile(path, append(out, '\n'), 0o644); err != nil {
			t.Fatalf("%s: write corpus file: %v", spec.name, err)
		}
	}
	t.Logf("wrote %d corpus cases to %s", len(freezeSpecs()), corpusDir)
}

// verifyCorpus loads every corpus file, fires its stored request, and asserts
// the response matches the stored expectation. It also cross-checks that the
// corpus covers every spec (no drift between code and disk).
func verifyCorpus(t *testing.T) {
	files, err := filepath.Glob(filepath.Join(corpusDir, "*.json"))
	if err != nil {
		t.Fatalf("glob corpus: %v", err)
	}
	if len(files) == 0 {
		t.Fatalf("no corpus files under %s — run with -update first", corpusDir)
	}
	sort.Strings(files)

	specByName := map[string]requestSpec{}
	for _, s := range freezeSpecs() {
		specByName[s.name] = s
	}

	seen := map[string]bool{}
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatalf("read %s: %v", f, err)
		}
		var c corpusCase
		if err := json.Unmarshal(data, &c); err != nil {
			t.Fatalf("parse %s: %v", f, err)
		}
		seen[c.Name] = true

		t.Run(c.Name, func(t *testing.T) {
			respBody := fire(t, c.Request)
			var got interface{}
			if err := json.Unmarshal(respBody, &got); err != nil {
				t.Fatalf("response not valid JSON: %v\nbody=%s", err, respBody)
			}

			switch {
			case c.ResponsePattern != nil:
				assertPattern(t, c.ResponsePattern, got)
			case c.Response != nil:
				var want interface{}
				if err := json.Unmarshal(c.Response, &want); err != nil {
					t.Fatalf("stored response not valid JSON: %v", err)
				}
				if !reflect.DeepEqual(got, want) {
					t.Errorf("response mismatch for %s\n got: %s\nwant: %s",
						c.Name, mustJSON(got), mustJSON(want))
				}
			default:
				t.Fatalf("corpus case %s has neither response nor response_pattern", c.Name)
			}
		})
	}

	// Drift guard: every spec must have a corpus file and vice versa.
	for name := range specByName {
		if !seen[name] {
			t.Errorf("spec %q has no corpus file — run with -update", name)
		}
	}
	for name := range seen {
		if _, ok := specByName[name]; !ok {
			t.Errorf("corpus file %q has no matching spec — stale fixture?", name)
		}
	}
}

// assertPattern checks a shape-asserted (nondeterministic-result) response.
func assertPattern(t *testing.T, p *responsePattern, got interface{}) {
	t.Helper()
	obj, ok := got.(map[string]interface{})
	if !ok {
		t.Fatalf("expected object response, got %T", got)
	}
	if jrpc, _ := obj["jsonrpc"].(string); jrpc != p.JSONRPC {
		t.Errorf("jsonrpc = %q, want %q", jrpc, p.JSONRPC)
	}
	if id, _ := obj["id"].(float64); int(id) != p.ID {
		t.Errorf("id = %v, want %d", obj["id"], p.ID)
	}
	if obj["error"] != nil {
		t.Errorf("unexpected error field: %v", obj["error"])
	}
	result, ok := obj["result"].(string)
	if !ok {
		t.Fatalf("result is not a string: %v", obj["result"])
	}
	if !regexp.MustCompile(p.ResultRegex).MatchString(result) {
		t.Errorf("result %q does not match %q", result, p.ResultRegex)
	}
}

func mustJSON(v interface{}) string {
	b, _ := json.MarshalIndent(v, "", "  ")
	return string(b)
}
