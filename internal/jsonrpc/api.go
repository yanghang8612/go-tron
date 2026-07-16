package jsonrpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// API implements http.Handler and dispatches JSON-RPC 2.0 requests.
type API struct {
	backend Backend
	filters *FilterManager
	subMgr  *SubscriptionManager
}

// NewAPI creates a new API handler with an active filter manager. Exposed for testing.
func NewAPI(backend Backend) *API {
	sm := newSubscriptionManager()
	fm := NewFilterManager(backend)
	fm.subMgr = sm
	fm.Start()
	return &API{backend: backend, filters: fm, subMgr: sm}
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
	Result  interface{}     `json:"result"` // must be present on success, even if null
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
	if websocket.IsWebSocketUpgrade(r) {
		api.subMgr.ServeWS(w, r)
		return
	}
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
	case "eth_gasPrice":
		result, err = api.ethGasPrice(req.Params)
	case "web3_sha3":
		result, err = api.web3Sha3(req.Params)
	case "net_listening":
		result, err = api.netListening(req.Params)
	case "net_peerCount":
		result, err = api.netPeerCount(req.Params)
	case "eth_accounts":
		result, err = api.ethAccounts(req.Params)
	case "eth_estimateGas":
		result, err = api.ethEstimateGas(req.Params)
	case "eth_newFilter":
		result, err = api.ethNewFilter(req.Params)
	case "eth_newBlockFilter":
		result, err = api.ethNewBlockFilter(req.Params)
	case "eth_uninstallFilter":
		result, err = api.ethUninstallFilter(req.Params)
	case "eth_getFilterChanges":
		result, err = api.ethGetFilterChanges(req.Params)
	case "eth_getFilterLogs":
		result, err = api.ethGetFilterLogs(req.Params)
	case "eth_sendRawTransaction", "eth_sendTransaction", "eth_sign", "eth_signTransaction":
		return errResp(id, codeMethodNotFound, "the method "+req.Method+" does not exist/is not available")
	default:
		return errResp(id, codeMethodNotFound, "method not found")
	}

	if err != nil {
		if coded, ok := err.(interface{ ErrorCode() int }); ok {
			return errResp(id, coded.ErrorCode(), err.Error())
		}
		return errResp(id, codeInternal, err.Error())
	}
	return rpcResponse{JSONRPC: "2.0", Result: result, ID: id}
}

func (api *API) ethNewFilter(params json.RawMessage) (interface{}, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var filterObj struct {
		FromBlock string          `json:"fromBlock"`
		ToBlock   string          `json:"toBlock"`
		BlockHash string          `json:"blockHash"`
		Address   json.RawMessage `json:"address"`
		Topics    json.RawMessage `json:"topics"`
	}
	if err := json.Unmarshal(p[0], &filterObj); err != nil {
		return nil, fmt.Errorf("invalid filter: %w", err)
	}
	lf := LogFilter{}
	if filterObj.BlockHash != "" {
		var h common.Hash
		copy(h[:], common.FromHex(filterObj.BlockHash))
		lf.BlockHash = &h
	} else {
		if filterObj.FromBlock != "" {
			n, err := parseBlockParam(filterObj.FromBlock)
			if err != nil {
				return nil, err
			}
			if n == ^uint64(0) {
				n = api.backend.BlockNumber()
			}
			lf.FromBlock = &n
		}
		if filterObj.ToBlock != "" {
			n, err := parseBlockParam(filterObj.ToBlock)
			if err != nil {
				return nil, err
			}
			if n == ^uint64(0) {
				n = api.backend.BlockNumber()
			}
			lf.ToBlock = &n
		}
	}
	if len(filterObj.Address) > 0 && string(filterObj.Address) != "null" {
		addresses, err := parseFilterAddresses(filterObj.Address)
		if err != nil {
			return nil, err
		}
		lf.Addresses = addresses
	}
	if len(filterObj.Topics) > 0 && string(filterObj.Topics) != "null" {
		var rawTopics []json.RawMessage
		if err := json.Unmarshal(filterObj.Topics, &rawTopics); err == nil {
			lf.Topics = make([][]common.Hash, len(rawTopics))
			for i, rt := range rawTopics {
				if string(rt) == "null" {
					continue
				}
				var single string
				var multi []string
				if json.Unmarshal(rt, &single) == nil {
					var h common.Hash
					copy(h[:], common.FromHex(single))
					lf.Topics[i] = []common.Hash{h}
				} else if json.Unmarshal(rt, &multi) == nil {
					for _, s := range multi {
						var h common.Hash
						copy(h[:], common.FromHex(s))
						lf.Topics[i] = append(lf.Topics[i], h)
					}
				}
			}
		}
	}
	return api.filters.NewLogFilter(lf)
}
func (api *API) ethNewBlockFilter(_ json.RawMessage) (interface{}, error) {
	return api.filters.NewBlockFilter()
}
func (api *API) ethUninstallFilter(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	return api.filters.UninstallFilter(p[0]), nil
}
func (api *API) ethGetFilterChanges(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	result, ok := api.filters.GetFilterChanges(p[0])
	if !ok {
		return nil, fmt.Errorf("filter not found")
	}
	return result, nil
}
func (api *API) ethGetFilterLogs(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	logs, ok := api.filters.GetFilterLogs(p[0])
	if !ok {
		return nil, fmt.Errorf("filter not found")
	}
	return logs, nil
}

func (api *API) ethEstimateGas(params json.RawMessage) (interface{}, error) {
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
	var from *common.Address
	if txObj.From != "" {
		a, err := parseCompatibleAddress(txObj.From)
		if err != nil {
			return nil, err
		}
		from = &a
	}
	var to *common.Address
	if txObj.To != "" {
		a, err := parseCompatibleAddress(txObj.To)
		if err != nil {
			return nil, err
		}
		to = &a
	}
	data := common.FromHex(txObj.Data)
	var value int64
	if txObj.Value != "" && txObj.Value != "0x0" && txObj.Value != "0x" {
		value, _ = strconv.ParseInt(txObj.Value, 0, 64)
	}
	energy, err := api.backend.EstimateGas(from, to, data, value)
	if err != nil {
		return nil, err
	}
	return hexUint64(energy), nil
}

func (api *API) ethGasPrice(_ json.RawMessage) (interface{}, error) {
	return hexUint64(uint64(api.backend.GasPrice())), nil
}
func (api *API) web3Sha3(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	input := common.FromHex(p[0])
	return hexBytes(crypto.Keccak256(input)), nil
}
func (api *API) netListening(_ json.RawMessage) (interface{}, error) {
	return api.backend.PeerCount() >= 1, nil
}
func (api *API) netPeerCount(_ json.RawMessage) (interface{}, error) {
	return hexUint64(uint64(api.backend.PeerCount())), nil
}
func (api *API) ethAccounts(_ json.RawMessage) (interface{}, error) {
	return []string{}, nil
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

// hexHash formats a 32-byte hash as "0x"+64-hex.
//
// Do NOT use fmt.Sprintf("0x%x", h) for a common.Hash: %x on an operand that
// implements fmt.Stringer calls String() first (which for common.Hash already
// returns the 64-char hex), then hex-encodes THAT string — yielding a wrong
// 128-char "0x3030…" value. Formatting via Hex() (a plain string) avoids it.
func hexHash(h common.Hash) string { return "0x" + h.Hex() }

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

// resolveBlockArg parses the optional block-tag argument at params[idx]
// (defaulting to "latest" when absent) and resolves the latest/pending
// sentinel to the current head number. Returns (blockNum, isLatest, err):
// isLatest lets callers take the live read path (and skip the archive
// reader) when the request targets head.
func (api *API) resolveBlockArg(p []string, idx int) (uint64, bool, error) {
	tag := "latest"
	if len(p) > idx && p[idx] != "" {
		tag = p[idx]
	}
	num, err := parseBlockParam(tag)
	if err != nil {
		return 0, false, err
	}
	if num == ^uint64(0) { // "latest"/"pending" sentinel
		return api.backend.BlockNumber(), true, nil
	}
	return num, false, nil
}

func (api *API) ethGetBalance(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	addr, err := parseCompatibleAddress(p[0])
	if err != nil {
		return nil, err
	}
	blockNum, isLatest, err := api.resolveBlockArg(p, 1)
	if err != nil {
		return nil, err
	}
	var balSUN int64
	if isLatest {
		balSUN = api.backend.GetBalance(addr)
	} else if balSUN, err = api.backend.GetBalanceAt(addr, blockNum); err != nil {
		return nil, err
	}
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
	addr, err := parseCompatibleAddress(p[0])
	if err != nil {
		return nil, err
	}
	blockNum, isLatest, err := api.resolveBlockArg(p, 1)
	if err != nil {
		return nil, err
	}
	if isLatest {
		return hexBytes(api.backend.GetCode(addr)), nil
	}
	code, err := api.backend.GetCodeAt(addr, blockNum)
	if err != nil {
		return nil, err
	}
	return hexBytes(code), nil
}
func (api *API) ethGetStorageAt(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 2 {
		return nil, fmt.Errorf("invalid params")
	}
	addr, err := parseCompatibleAddress(p[0])
	if err != nil {
		return nil, err
	}
	var slot common.Hash
	slotBytes := common.FromHex(p[1])
	if len(slotBytes) > 32 {
		slotBytes = slotBytes[len(slotBytes)-32:]
	}
	copy(slot[32-len(slotBytes):], slotBytes)
	blockNum, isLatest, err := api.resolveBlockArg(p, 2)
	if err != nil {
		return nil, err
	}
	if isLatest {
		val := api.backend.GetStorageAt(addr, slot)
		return hexBytes(val[:]), nil
	}
	val, err := api.backend.GetStorageAtBlock(addr, slot, blockNum)
	if err != nil {
		return nil, err
	}
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
		a, err := parseCompatibleAddress(txObj.From)
		if err != nil {
			return nil, err
		}
		from = &a
	}
	to, err := parseCompatibleAddress(txObj.To)
	if err != nil {
		return nil, err
	}

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
	if err != nil || block == nil {
		return nil, nil // unknown block → null (Ethereum spec)
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
	if err != nil || block == nil {
		return nil, nil // unknown block → null (Ethereum spec)
	}
	return blockToRPC(block, fullTx), nil
}
func (api *API) ethGetTransactionByHash(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var hash common.Hash
	copy(hash[:], common.FromHex(p[0]))

	tx, block, index, err := api.backend.GetTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, nil // not found → null
	}
	return txToRPC(tx, hash, block, index), nil
}
func (api *API) ethGetTransactionReceipt(params json.RawMessage) (interface{}, error) {
	var p []string
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}
	var hash common.Hash
	copy(hash[:], common.FromHex(p[0]))

	info, err := api.backend.GetTransactionInfo(hash)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil // not found → null
	}
	tx, block, index, err := api.backend.GetTransactionByHash(hash)
	if err != nil || tx == nil {
		return nil, nil // receipt exists but can't find tx — treat as not found
	}
	return receiptToRPC(hash, tx, info, block, index), nil
}
func (api *API) ethGetLogs(params json.RawMessage) (interface{}, error) {
	var p []json.RawMessage
	if err := json.Unmarshal(params, &p); err != nil || len(p) < 1 {
		return nil, fmt.Errorf("invalid params")
	}

	var filterObj struct {
		FromBlock string          `json:"fromBlock"`
		ToBlock   string          `json:"toBlock"`
		BlockHash string          `json:"blockHash"`
		Address   json.RawMessage `json:"address"`
		Topics    json.RawMessage `json:"topics"`
	}
	if err := json.Unmarshal(p[0], &filterObj); err != nil {
		return nil, fmt.Errorf("invalid filter: %w", err)
	}

	filter := LogFilter{}

	if filterObj.BlockHash != "" {
		var h common.Hash
		copy(h[:], common.FromHex(filterObj.BlockHash))
		filter.BlockHash = &h
	} else {
		if filterObj.FromBlock != "" {
			n, err := parseBlockParam(filterObj.FromBlock)
			if err != nil {
				return nil, fmt.Errorf("invalid fromBlock: %w", err)
			}
			if n == ^uint64(0) {
				n = api.backend.BlockNumber()
			}
			filter.FromBlock = &n
		}
		if filterObj.ToBlock != "" {
			n, err := parseBlockParam(filterObj.ToBlock)
			if err != nil {
				return nil, fmt.Errorf("invalid toBlock: %w", err)
			}
			if n == ^uint64(0) {
				n = api.backend.BlockNumber()
			}
			filter.ToBlock = &n
		}
	}

	// Parse address: string or []string
	if len(filterObj.Address) > 0 && string(filterObj.Address) != "null" {
		addresses, err := parseFilterAddresses(filterObj.Address)
		if err != nil {
			return nil, err
		}
		filter.Addresses = addresses
	}

	// Parse topics: [topic0, topic1, ...] where each element is null | string | []string
	if len(filterObj.Topics) > 0 && string(filterObj.Topics) != "null" {
		var rawTopics []json.RawMessage
		if err := json.Unmarshal(filterObj.Topics, &rawTopics); err == nil {
			filter.Topics = make([][]common.Hash, len(rawTopics))
			for i, rt := range rawTopics {
				if string(rt) == "null" {
					filter.Topics[i] = nil
					continue
				}
				var single string
				var multi []string
				if json.Unmarshal(rt, &single) == nil {
					var h common.Hash
					copy(h[:], common.FromHex(single))
					filter.Topics[i] = []common.Hash{h}
				} else if json.Unmarshal(rt, &multi) == nil {
					for _, s := range multi {
						var h common.Hash
						copy(h[:], common.FromHex(s))
						filter.Topics[i] = append(filter.Topics[i], h)
					}
				}
			}
		}
	}

	logs, err := api.backend.GetLogs(filter)
	if err != nil {
		return nil, err
	}
	if logs == nil {
		logs = []*RPCLog{}
	}
	return logs, nil
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
		"hash":             hexHash(hash),
		"blockHash":        hexHash(block.Hash()),
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

// receiptToRPC converts TRON tx + info to an Ethereum receipt JSON object.
func receiptToRPC(hash common.Hash, tx *corepb.Transaction, info *corepb.TransactionInfo, block *types.Block, index int) map[string]interface{} {
	// Extract the sender address from the transaction.
	from := "0x0000000000000000000000000000000000000000"
	if len(tx.GetRawData().GetContract()) > 0 {
		c := tx.RawData.Contract[0]
		switch c.Type {
		case corepb.Transaction_Contract_TriggerSmartContract:
			var msg contractpb.TriggerSmartContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
			}
		case corepb.Transaction_Contract_CreateSmartContract:
			var msg contractpb.CreateSmartContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
			}
		case corepb.Transaction_Contract_TransferContract:
			var msg contractpb.TransferContract
			if c.Parameter.UnmarshalTo(&msg) == nil {
				from = hex20(msg.OwnerAddress)
			}
		}
	}

	status := "0x1"
	if info.Result == corepb.TransactionInfo_FAILED {
		status = "0x0"
	}

	var contractAddr interface{} = nil
	if len(info.ContractAddress) > 0 {
		contractAddr = hex20(info.ContractAddress)
	}

	var toAddr interface{} = "0x0000000000000000000000000000000000000000"
	if len(tx.GetRawData().GetContract()) > 0 && tx.RawData.Contract[0].Type == corepb.Transaction_Contract_CreateSmartContract {
		toAddr = nil
	}

	energyUsed := int64(0)
	if info.Receipt != nil {
		energyUsed = info.Receipt.EnergyUsageTotal
	}

	// Build logs
	logs := make([]map[string]interface{}, 0)
	for li, l := range info.Log {
		topics := make([]string, len(l.Topics))
		for ti, t := range l.Topics {
			topics[ti] = fmt.Sprintf("0x%064x", t)
		}
		logs = append(logs, map[string]interface{}{
			"address":          hex20(l.Address),
			"topics":           topics,
			"data":             hexBytes(l.Data),
			"blockNumber":      hexUint64(block.Number()),
			"transactionHash":  hexHash(hash),
			"transactionIndex": hexUint64(uint64(index)),
			"blockHash":        hexHash(block.Hash()),
			"logIndex":         hexUint64(uint64(li)),
			"removed":          false,
		})
	}

	return map[string]interface{}{
		"transactionHash":   hexHash(hash),
		"transactionIndex":  hexUint64(uint64(index)),
		"blockHash":         hexHash(block.Hash()),
		"blockNumber":       hexUint64(block.Number()),
		"from":              from,
		"to":                toAddr,
		"cumulativeGasUsed": hexUint64(uint64(energyUsed)),
		"gasUsed":           hexUint64(uint64(energyUsed)),
		"contractAddress":   contractAddr,
		"logs":              logs,
		"logsBloom":         "0x" + zeroBloom(),
		"status":            status,
		"type":              "0x0",
	}
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
			hashes[i] = hexHash(tx.Hash())
		}
		transactions = hashes
	}

	witnessAddr := b.WitnessAddress()

	return map[string]interface{}{
		"hash":             hexHash(b.Hash()),
		"parentHash":       hexHash(b.ParentHash()),
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
		"stateRoot":        hexHash(b.AccountStateRoot()),
		"receiptsRoot":     "0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421",
		"uncles":           []string{},
		"transactions":     transactions,
	}
}
