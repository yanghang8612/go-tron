package jsonrpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
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
	if len(slotBytes) > 32 {
		slotBytes = slotBytes[len(slotBytes)-32:]
	}
	copy(slot[32-len(slotBytes):], slotBytes)
	val := api.backend.GetStorageAt(addr, slot)
	return hexBytes(val[:]), nil
}
func (api *API) ethCall(params json.RawMessage) (interface{}, error) {
	// params: [{from, to, data, value, gas}, blockParam]
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}

	var txObj struct {
		From  string `json:"from"`
		To    string `json:"to"`
		Data  string `json:"data"`
		Value string `json:"value"`
	}
	if err := json.Unmarshal(p[0], &txObj); err != nil {
		return nil, fmt.Errorf("invalid tx object: %w", err)
	}
	if txObj.To == "" {
		return nil, fmt.Errorf("eth_call: 'to' required")
	}

	var from *common.Address
	if txObj.From != "" {
		a := common.BytesToAddress(common.FromHex(txObj.From))
		from = &a
	}
	to := common.BytesToAddress(common.FromHex(txObj.To))

	data := common.FromHex(txObj.Data)

	var value int64
	if txObj.Value != "" && txObj.Value != "0x0" && txObj.Value != "0x" {
		v, _ := strconv.ParseInt(txObj.Value, 0, 64)
		value = v
	}

	result, err := api.backend.Call(from, &to, data, value)
	if err != nil {
		return nil, err
	}
	return hexBytes(result), nil
}
func (api *API) ethGetBlockByNumber(params json.RawMessage) (interface{}, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var blockTag string
	json.Unmarshal(p[0], &blockTag)

	var fullTx bool
	if len(p) > 1 {
		json.Unmarshal(p[1], &fullTx)
	}

	num, err := parseBlockParam(blockTag)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	if num == ^uint64(0) { // "latest"
		num = api.backend.BlockNumber()
	}
	block, err := api.backend.GetBlockByNumber(num)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil // null = not found
	}
	return blockToRPC(block, fullTx), nil
}
func (api *API) ethGetBlockByHash(params json.RawMessage) (interface{}, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var hashStr string
	json.Unmarshal(p[0], &hashStr)

	var fullTx bool
	if len(p) > 1 {
		json.Unmarshal(p[1], &fullTx)
	}

	var hash common.Hash
	copy(hash[:], common.FromHex(hashStr))
	block, err := api.backend.GetBlockByHash(hash)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, nil
	}
	return blockToRPC(block, fullTx), nil
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

// ── Block/TX conversion helpers ───────────────────────────────────────────────

// zeroBloom returns 512 hex zeros (256 bytes = logs bloom placeholder).
func zeroBloom() string {
	const zeros = "0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000" +
		"0000000000000000000000000000000000000000000000000000000000000000"
	return zeros
}

// txToRPC converts a raw TRON transaction proto to Ethereum-format JSON.
func txToRPC(tx *corepb.Transaction, hash common.Hash, block *types.Block, index int) map[string]interface{} {
	from := "0x0000000000000000000000000000000000000000"
	to := "0x0000000000000000000000000000000000000000"
	input := "0x"
	value := "0x0"

	if len(tx.GetRawData().GetContract()) > 0 {
		c := tx.RawData.Contract[0]
		switch c.Type {
		case corepb.Transaction_Contract_TriggerSmartContract:
			var msg contractpb.TriggerSmartContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
				to = hex20(msg.ContractAddress)
				input = hexBytes(msg.Data)
				value = hexUint64(uint64(msg.CallValue))
			}
		case corepb.Transaction_Contract_CreateSmartContract:
			var msg contractpb.CreateSmartContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
				to = ""
				if msg.NewContract != nil {
					input = hexBytes(msg.NewContract.Bytecode)
				}
			}
		case corepb.Transaction_Contract_TransferContract:
			var msg contractpb.TransferContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
				to = hex20(msg.ToAddress)
				value = hexUint64(uint64(msg.Amount))
			}
		}
	}

	result := map[string]interface{}{
		"hash":             fmt.Sprintf("0x%x", hash),
		"blockHash":        fmt.Sprintf("0x%x", block.Hash()),
		"blockNumber":      hexUint64(block.Number()),
		"transactionIndex": hexUint64(uint64(index)),
		"from":             from,
		"value":            value,
		"gas":              hexUint64(uint64(tx.GetRawData().GetFeeLimit())),
		"gasPrice":         "0x1",
		"input":            input,
		"nonce":            "0x0",
		"type":             "0x0",
		"v":                "0x0",
		"r":                "0x0",
		"s":                "0x0",
	}
	if to != "" {
		result["to"] = to
	} else {
		result["to"] = nil
	}
	return result
}

// blockToRPC converts a types.Block to the Ethereum JSON block object.
func blockToRPC(b *types.Block, fullTx bool) map[string]interface{} {
	txs := b.Transactions()

	var transactions interface{}
	if fullTx {
		list := make([]map[string]interface{}, len(txs))
		for i, tx := range txs {
			list[i] = txToRPC(tx.Proto(), tx.Hash(), b, i)
		}
		transactions = list
	} else {
		hashes := make([]string, len(txs))
		for i, tx := range txs {
			hashes[i] = fmt.Sprintf("0x%x", tx.Hash())
		}
		transactions = hashes
	}

	witnessAddr := b.WitnessAddress()

	return map[string]interface{}{
		"hash":             fmt.Sprintf("0x%x", b.Hash()),
		"parentHash":       fmt.Sprintf("0x%x", b.ParentHash()),
		"number":           hexUint64(b.Number()),
		"timestamp":        hexUint64(uint64(b.Timestamp() / 1000)), // ms → s
		"miner":            fmt.Sprintf("0x%x", witnessAddr[:]),
		"difficulty":       "0x0",
		"totalDifficulty":  "0x0",
		"extraData":        "0x",
		"size":             "0x0",
		"gasLimit":         "0x0",
		"gasUsed":          "0x0",
		"nonce":            "0x0000000000000000",
		"sha3Uncles":       "0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347",
		"logsBloom":        "0x" + zeroBloom(),
		"transactionsRoot": "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"stateRoot":        fmt.Sprintf("0x%x", b.AccountStateRoot()),
		"receiptsRoot":     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"uncles":           []string{},
		"transactions":     transactions,
	}
}
