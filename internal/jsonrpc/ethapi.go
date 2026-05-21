package jsonrpc

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
)

// callArgs is the {from,to,data,value} object accepted by eth_call and
// eth_estimateGas. All fields are 0x-hex strings, parsed exactly as the legacy
// handlers did; the framework unmarshals the request's object param into it.
type callArgs struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Data  string `json:"data"`
	Value string `json:"value"`
}

// parseCallValue mirrors the legacy tx-value parsing: empty/"0x0"/"0x" => 0,
// otherwise base-0 ParseInt with the parse error ignored (as the legacy did).
func parseCallValue(s string) int64 {
	if s == "" || s == "0x0" || s == "0x" {
		return 0
	}
	v, _ := strconv.ParseInt(s, 0, 64)
	return v
}

// EthAPI implements the "eth" JSON-RPC namespace on the reflection-based
// internal/rpc framework. It is the migration target for the eth_* arms of
// api.go's dispatch switch (jsonrpc-reflection).
//
// Covered so far: the no-parameter methods and the param-bearing account
// readers, all of which migrate zero-diff against the frozen jsonrpc-corpus.
// Still to land: eth_call/estimateGas, the block/tx/receipt readers (which
// additionally FIX the legacy double-hex-hash bug, so their corpus entries get
// regenerated at that point), eth_getLogs, and the filter methods.
//
// Method names map by the framework's reflection rule (first letter lowered):
// ChainId -> eth_chainId, GetBalance -> eth_getBalance, etc. Param-bearing
// methods take string arguments and parse them exactly as the legacy handlers
// did (common.FromHex etc.), with a trailing *string block tag that the
// framework leaves nil when the caller omits it — mirroring the legacy
// resolveBlockArg "default to latest" behavior.
type EthAPI struct {
	backend Backend
}

// NewEthAPI builds an EthAPI over the given backend.
func NewEthAPI(backend Backend) *EthAPI { return &EthAPI{backend: backend} }

// resolveBlock mirrors api.resolveBlockArg for the framework methods: a nil or
// empty block tag means "latest" (live read path), otherwise the parsed block
// number with the archive read path. Returns (blockNum, isLatest, err).
func (e *EthAPI) resolveBlock(block *string) (uint64, bool, error) {
	tag := "latest"
	if block != nil && *block != "" {
		tag = *block
	}
	num, err := parseBlockParam(tag)
	if err != nil {
		return 0, false, err
	}
	if num == ^uint64(0) { // "latest"/"pending" sentinel
		return e.backend.BlockNumber(), true, nil
	}
	return num, false, nil
}

// ChainId serves eth_chainId. It is deliberately named ChainId (not ChainID)
// so the framework's first-letter-lowering yields the canonical method name
// eth_chainId, matching go-ethereum's own EthereumAPI.ChainId.
func (e *EthAPI) ChainId() string { return hexUint64(uint64(e.backend.ChainID())) }

// BlockNumber serves eth_blockNumber: the current head height as 0x-hex.
func (e *EthAPI) BlockNumber() string { return hexUint64(e.backend.BlockNumber()) }

// Syncing serves eth_syncing. go-tron always reports false here, mirroring the
// legacy handler; sync progress is surfaced through the TRON HTTP API instead.
func (e *EthAPI) Syncing() bool { return false }

// GasPrice serves eth_gasPrice: the energy fee in SUN as 0x-hex.
func (e *EthAPI) GasPrice() string { return hexUint64(uint64(e.backend.GasPrice())) }

// Accounts serves eth_accounts: always empty (the node holds no managed keys).
func (e *EthAPI) Accounts() []string { return []string{} }

// GetBalance serves eth_getBalance: the SUN balance scaled by 1e12 (to wei-like
// 18-decimal units) as 0x-hex. The optional block tag selects live vs archive.
func (e *EthAPI) GetBalance(addrHex string, block *string) (string, error) {
	addr := common.BytesToAddress(common.FromHex(addrHex))
	blockNum, isLatest, err := e.resolveBlock(block)
	if err != nil {
		return "", err
	}
	var balSUN int64
	if isLatest {
		balSUN = e.backend.GetBalance(addr)
	} else if balSUN, err = e.backend.GetBalanceAt(addr, blockNum); err != nil {
		return "", err
	}
	// Multiply by 1e12 using big.Int to avoid int64 overflow for large balances.
	wei := new(big.Int).Mul(big.NewInt(balSUN), big.NewInt(1_000_000_000_000))
	return fmt.Sprintf("0x%x", wei), nil
}

// GetTransactionCount serves eth_getTransactionCount: TRON has no nonces, so it
// is always 0. The address/block params are accepted for client compatibility
// and ignored, exactly as the legacy handler did.
func (e *EthAPI) GetTransactionCount(_ string, _ *string) string { return "0x0" }

// GetCode serves eth_getCode: the contract bytecode as 0x-hex (live or archive).
func (e *EthAPI) GetCode(addrHex string, block *string) (string, error) {
	addr := common.BytesToAddress(common.FromHex(addrHex))
	blockNum, isLatest, err := e.resolveBlock(block)
	if err != nil {
		return "", err
	}
	if isLatest {
		return hexBytes(e.backend.GetCode(addr)), nil
	}
	code, err := e.backend.GetCodeAt(addr, blockNum)
	if err != nil {
		return "", err
	}
	return hexBytes(code), nil
}

// GetStorageAt serves eth_getStorageAt: the 32-byte storage word at the given
// slot as 0x-hex (live or archive). The slot is right-aligned into 32 bytes,
// matching the legacy handler.
func (e *EthAPI) GetStorageAt(addrHex, slotHex string, block *string) (string, error) {
	addr := common.BytesToAddress(common.FromHex(addrHex))
	var slot common.Hash
	slotBytes := common.FromHex(slotHex)
	if len(slotBytes) > 32 {
		slotBytes = slotBytes[len(slotBytes)-32:]
	}
	copy(slot[32-len(slotBytes):], slotBytes)
	blockNum, isLatest, err := e.resolveBlock(block)
	if err != nil {
		return "", err
	}
	if isLatest {
		val := e.backend.GetStorageAt(addr, slot)
		return hexBytes(val[:]), nil
	}
	val, err := e.backend.GetStorageAtBlock(addr, slot, blockNum)
	if err != nil {
		return "", err
	}
	return hexBytes(val[:]), nil
}

// Call serves eth_call: read-only TVM execution against head state, returning
// the result bytes as 0x-hex. 'to' is required. The block tag is accepted and
// ignored (the legacy handler always reads head), preserving that behavior.
func (e *EthAPI) Call(tx callArgs, block *string) (string, error) {
	if tx.To == "" {
		return "", fmt.Errorf("eth_call: 'to' required")
	}
	var from *common.Address
	if tx.From != "" {
		a := common.BytesToAddress(common.FromHex(tx.From))
		from = &a
	}
	to := common.BytesToAddress(common.FromHex(tx.To))
	result, err := e.backend.Call(from, &to, common.FromHex(tx.Data), parseCallValue(tx.Value))
	if err != nil {
		return "", err
	}
	return hexBytes(result), nil
}

// EstimateGas serves eth_estimateGas: the energy used by a simulated execution
// as 0x-hex. Both from and to are optional (to may be nil for creation-style
// estimates, unlike eth_call). The block tag, if present, is ignored.
func (e *EthAPI) EstimateGas(tx callArgs, block *string) (string, error) {
	var from, to *common.Address
	if tx.From != "" {
		a := common.BytesToAddress(common.FromHex(tx.From))
		from = &a
	}
	if tx.To != "" {
		a := common.BytesToAddress(common.FromHex(tx.To))
		to = &a
	}
	energy, err := e.backend.EstimateGas(from, to, common.FromHex(tx.Data), parseCallValue(tx.Value))
	if err != nil {
		return "", err
	}
	return hexUint64(energy), nil
}

// GetBlockByNumber serves eth_getBlockByNumber. The optional fullTx flag
// (default false) selects full tx objects vs a hash list. Reuses the shared
// blockToRPC converter. An unknown block resolves to null (Ethereum spec).
func (e *EthAPI) GetBlockByNumber(blockTag string, fullTx *bool) (interface{}, error) {
	num, err := parseBlockParam(blockTag)
	if err != nil {
		return nil, fmt.Errorf("invalid params: %w", err)
	}
	if num == ^uint64(0) { // "latest"/"pending"
		num = e.backend.BlockNumber()
	}
	block, err := e.backend.GetBlockByNumber(num)
	if err != nil || block == nil {
		return nil, nil
	}
	return blockToRPC(block, fullTx != nil && *fullTx), nil
}

// GetBlockByHash serves eth_getBlockByHash. Unknown block => null.
func (e *EthAPI) GetBlockByHash(hashHex string, fullTx *bool) (interface{}, error) {
	var hash common.Hash
	copy(hash[:], common.FromHex(hashHex))
	block, err := e.backend.GetBlockByHash(hash)
	if err != nil || block == nil {
		return nil, nil
	}
	return blockToRPC(block, fullTx != nil && *fullTx), nil
}

// GetTransactionByHash serves eth_getTransactionByHash. Not found => null.
func (e *EthAPI) GetTransactionByHash(hashHex string) (interface{}, error) {
	var hash common.Hash
	copy(hash[:], common.FromHex(hashHex))
	tx, block, index, err := e.backend.GetTransactionByHash(hash)
	if err != nil {
		return nil, err
	}
	if tx == nil {
		return nil, nil
	}
	return txToRPC(tx, hash, block, index), nil
}

// GetTransactionReceipt serves eth_getTransactionReceipt. Not found => null.
func (e *EthAPI) GetTransactionReceipt(hashHex string) (interface{}, error) {
	var hash common.Hash
	copy(hash[:], common.FromHex(hashHex))
	info, err := e.backend.GetTransactionInfo(hash)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	tx, block, index, err := e.backend.GetTransactionByHash(hash)
	if err != nil || tx == nil {
		return nil, nil
	}
	return receiptToRPC(hash, tx, info, block, index), nil
}

// logFilterArgs is the eth_getLogs filter object. Address and Topics are kept
// raw because each is polymorphic (address: string|[]string; topics: array of
// null|string|[]string), parsed below exactly as the legacy handler does.
type logFilterArgs struct {
	FromBlock string          `json:"fromBlock"`
	ToBlock   string          `json:"toBlock"`
	BlockHash string          `json:"blockHash"`
	Address   json.RawMessage `json:"address"`
	Topics    json.RawMessage `json:"topics"`
}

// GetLogs serves eth_getLogs. It resolves the block range (or blockHash),
// address set, and topic matrix into a LogFilter and delegates to the backend.
// A nil result is normalized to an empty array, matching the legacy handler.
func (e *EthAPI) GetLogs(filterObj logFilterArgs) ([]*RPCLog, error) {
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
				n = e.backend.BlockNumber()
			}
			filter.FromBlock = &n
		}
		if filterObj.ToBlock != "" {
			n, err := parseBlockParam(filterObj.ToBlock)
			if err != nil {
				return nil, fmt.Errorf("invalid toBlock: %w", err)
			}
			if n == ^uint64(0) {
				n = e.backend.BlockNumber()
			}
			filter.ToBlock = &n
		}
	}

	// address: string or []string
	if len(filterObj.Address) > 0 && string(filterObj.Address) != "null" {
		var addrStr string
		var addrSlice []string
		if json.Unmarshal(filterObj.Address, &addrStr) == nil {
			filter.Addresses = []common.Address{common.BytesToAddress(common.FromHex(addrStr))}
		} else if json.Unmarshal(filterObj.Address, &addrSlice) == nil {
			for _, a := range addrSlice {
				filter.Addresses = append(filter.Addresses, common.BytesToAddress(common.FromHex(a)))
			}
		}
	}

	// topics: [topic0, topic1, ...] where each is null | string | []string
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

	logs, err := e.backend.GetLogs(filter)
	if err != nil {
		return nil, err
	}
	if logs == nil {
		logs = []*RPCLog{}
	}
	return logs, nil
}
