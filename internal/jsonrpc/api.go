package jsonrpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
)

// API implements http.Handler and dispatches JSON-RPC 2.0 requests.
type API struct {
	backend Backend
}

// NewAPI creates a new API handler. Exposed for testing.
func NewAPI(backend Backend) *API {
	return &API{backend: backend}
}

// ── JSON-RPC protocol types ────────────────────────────────────────────────

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      json.RawMessage `json:"id"`
}

type rpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	Result  interface{}     `json:"result"`       // must be present on success, even if null
	Error   *rpcError       `json:"error,omitempty"`
	ID      json.RawMessage `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

const (
	codeParseError     = -32700
	codeInvalidRequest = -32600
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
	codeInternal       = -32603
)

// ── HTTP handler ────────────────────────────────────────────────────────────

func (api *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("Content-Type", "application/json")

	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		json.NewEncoder(w).Encode(errResp(nil, codeParseError, "parse error"))
		return
	}

	// Batch vs. single request
	if len(raw) > 0 && raw[0] == '[' {
		var reqs []rpcRequest
		if err := json.Unmarshal(raw, &reqs); err != nil {
			json.NewEncoder(w).Encode(errResp(nil, codeParseError, "parse error"))
			return
		}
		responses := make([]rpcResponse, len(reqs))
		for i, req := range reqs {
			responses[i] = api.dispatch(req)
		}
		json.NewEncoder(w).Encode(responses)
		return
	}

	var req rpcRequest
	if err := json.Unmarshal(raw, &req); err != nil {
		json.NewEncoder(w).Encode(errResp(nil, codeParseError, "parse error"))
		return
	}
	json.NewEncoder(w).Encode(api.dispatch(req))
}

func errResp(id json.RawMessage, code int, msg string) rpcResponse {
	if id == nil {
		id = json.RawMessage("null")
	}
	return rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: code, Message: msg}, ID: id}
}

// ── Dispatcher ───────────────────────────────────────────────────────────────

func (api *API) dispatch(req rpcRequest) rpcResponse {
	id := req.ID
	if id == nil {
		id = json.RawMessage("null")
	}

	var (
		result interface{}
		err    error
	)
	switch req.Method {
	case "net_version":
		result, err = api.netVersion(req.Params)
	case "web3_clientVersion":
		result, err = api.web3ClientVersion(req.Params)
	case "eth_chainId":
		result, err = api.ethChainId(req.Params)
	case "eth_blockNumber":
		result, err = api.ethBlockNumber(req.Params)
	case "eth_syncing":
		result, err = api.ethSyncing(req.Params)
	case "eth_getBalance":
		result, err = api.ethGetBalance(req.Params)
	case "eth_getTransactionCount":
		result, err = api.ethGetTransactionCount(req.Params)
	case "eth_getCode":
		result, err = api.ethGetCode(req.Params)
	case "eth_getStorageAt":
		result, err = api.ethGetStorageAt(req.Params)
	case "eth_call":
		result, err = api.ethCall(req.Params)
	case "eth_getBlockByNumber":
		result, err = api.ethGetBlockByNumber(req.Params)
	case "eth_getBlockByHash":
		result, err = api.ethGetBlockByHash(req.Params)
	case "eth_getTransactionByHash":
		result, err = api.ethGetTransactionByHash(req.Params)
	case "eth_getTransactionReceipt":
		result, err = api.ethGetTransactionReceipt(req.Params)
	case "eth_getLogs":
		result, err = api.ethGetLogs(req.Params)
	default:
		return errResp(id, codeMethodNotFound, "method not found")
	}

	if err != nil {
		return errResp(id, codeInternal, err.Error())
	}
	return rpcResponse{JSONRPC: "2.0", Result: result, ID: id}
}

// ── Hex helpers ──────────────────────────────────────────────────────────────

// hexUint64 formats n as "0x<hex>".
func hexUint64(n uint64) string { return fmt.Sprintf("0x%x", n) }

// hexBytes formats b as "0x<hex>". Returns "0x" for nil/empty.
func hexBytes(b []byte) string {
	if len(b) == 0 {
		return "0x"
	}
	return fmt.Sprintf("0x%x", b)
}

// hex20 formats a byte slice's last 20 bytes as "0x<40 hex chars>".
func hex20(b []byte) string {
	if len(b) < 20 {
		return "0x0000000000000000000000000000000000000000"
	}
	return fmt.Sprintf("0x%x", b[len(b)-20:])
}

// parseBlockParam converts a block tag ("latest", "earliest", "pending", "0x1") to uint64.
// Returns ^uint64(0) as sentinel for "latest"/"pending". Returns an error for invalid input.
func parseBlockParam(s string) (uint64, error) {
	switch s {
	case "", "latest", "pending":
		return ^uint64(0), nil
	case "earliest":
		return 0, nil
	default:
		if len(s) > 2 && s[:2] == "0x" {
			n, err := strconv.ParseUint(s[2:], 16, 64)
			if err != nil {
				return 0, fmt.Errorf("invalid block number %q", s)
			}
			return n, nil
		}
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return 0, fmt.Errorf("invalid block number %q", s)
		}
		return n, nil
	}
}

// ── Stub handlers (replaced task by task) ───────────────────────────────────

func (api *API) netVersion(_ json.RawMessage) (interface{}, error) {
	return fmt.Sprintf("%d", api.backend.ChainID()), nil
}
func (api *API) web3ClientVersion(_ json.RawMessage) (interface{}, error) {
	return "go-tron/v0.3.0-dev", nil
}
func (api *API) ethChainId(_ json.RawMessage) (interface{}, error) {
	return hexUint64(uint64(api.backend.ChainID())), nil
}
func (api *API) ethBlockNumber(_ json.RawMessage) (interface{}, error) {
	return hexUint64(api.backend.BlockNumber()), nil
}
func (api *API) ethSyncing(_ json.RawMessage) (interface{}, error) {
	return false, nil
}
func (api *API) ethGetBalance(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	addr := common.BytesToAddress(common.FromHex(p[0]))
	balSUN := api.backend.GetBalance(addr)
	// Multiply by 1e12 using big.Int to avoid int64 overflow for large balances.
	wei := new(big.Int).Mul(big.NewInt(balSUN), big.NewInt(1_000_000_000_000))
	return fmt.Sprintf("0x%x", wei), nil
}
func (api *API) ethGetTransactionCount(_ json.RawMessage) (interface{}, error) {
	// TRON has no nonces. Always return 0.
	return "0x0", nil
}
func (api *API) ethGetCode(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	addr := common.BytesToAddress(common.FromHex(p[0]))
	return hexBytes(api.backend.GetCode(addr)), nil
}
func (api *API) ethGetStorageAt(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 2 {
		return nil, fmt.Errorf("invalid params")
	}
	addr := common.BytesToAddress(common.FromHex(p[0]))
	var slot common.Hash
	slotBytes := common.FromHex(p[1])
	copy(slot[32-len(slotBytes):], slotBytes)
	val := api.backend.GetStorageAt(addr, slot)
	return hexBytes(val[:]), nil
}
func (api *API) ethCall(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetBlockByNumber(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetBlockByHash(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetTransactionByHash(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetTransactionReceipt(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
func (api *API) ethGetLogs(_ json.RawMessage) (interface{}, error) {
	return nil, fmt.Errorf("not implemented")
}
