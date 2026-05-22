package jsonrpc

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// This file holds the pieces of the JSON-RPC layer shared across the package
// after the reflection-framework cutover. The hand-rolled per-method HTTP
// dispatch that used to live here was removed once internal/rpc plus the
// EthAPI/NetAPI/Web3API service structs took over HTTP request handling. What
// remains is still live:
//
//   - the minimal JSON-RPC 2.0 protocol types (rpcRequest/rpcResponse/rpcError)
//     and the errResp helper, used by the WebSocket subscription loop in
//     subscription.go — the framework does not own the WS upgrade path; and
//   - the Ethereum-format conversion helpers (blockToRPC/txToRPC/receiptToRPC)
//     and hex formatters, reused verbatim by EthAPI so eth_* responses stay
//     byte-identical to the frozen jsonrpc-corpus.

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
	codeMethodNotFound = -32601
	codeInvalidParams  = -32602
)

// errResp builds a JSON-RPC error response with a non-nil id (defaulting to the
// JSON null literal). Used by the WebSocket subscription loop.
func errResp(id json.RawMessage, code int, msg string) rpcResponse {
	if id == nil {
		id = json.RawMessage("null")
	}
	return rpcResponse{JSONRPC: "2.0", Error: &rpcError{Code: code, Message: msg}, ID: id}
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
