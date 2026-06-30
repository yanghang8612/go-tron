package jsonrpc

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

// DebugAPI implements the geth-compatible "debug" JSON-RPC namespace
// (debug_traceCall, debug_traceTransaction, debug_traceBlockByNumber, and
// debug_traceBlockByHash) on the reflection-based internal/rpc framework.
// Method names map by the framework's first-letter lowering: TraceCall ->
// debug_traceCall, TraceBlockByNumber -> debug_traceBlockByNumber, etc.
//
// NOTE: distinct from internal/debugapi, which is the pprof HTTP server — this
// is the JSON-RPC tracing namespace.
type DebugAPI struct {
	backend Backend
}

type blockTraceResult struct {
	TxHash string      `json:"txHash"`
	Result interface{} `json:"result,omitempty"`
	Error  string      `json:"error,omitempty"`
}

// NewDebugAPI builds a DebugAPI over the given backend.
func NewDebugAPI(backend Backend) *DebugAPI {
	return &DebugAPI{backend: backend}
}

// TraceCall serves debug_traceCall: a read-only traced execution. 'to' is
// required. The optional block tag (default "latest") selects head vs an
// archive block; config selects the tracer and struct-log toggles (the geth
// TraceConfig shape). A revert is reported through the tracer result, not as a
// JSON-RPC error.
func (d *DebugAPI) TraceCall(tx callArgs, block *string, config *tracers.TraceConfig) (interface{}, error) {
	if tx.To == "" {
		return nil, fmt.Errorf("debug_traceCall: 'to' required")
	}
	var from *common.Address
	if tx.From != "" {
		a := common.BytesToAddress(common.FromHex(tx.From))
		from = &a
	}
	to := common.BytesToAddress(common.FromHex(tx.To))
	blockNumber, err := d.resolveTraceBlock(block)
	if err != nil {
		return nil, err
	}
	return d.backend.TraceCall(from, &to, common.FromHex(tx.Data), parseCallValue(tx.Value), blockNumber, config)
}

// TraceTransaction serves debug_traceTransaction: re-execute a historical
// transaction from its parent state with the configured tracer.
func (d *DebugAPI) TraceTransaction(hashHex string, config *tracers.TraceConfig) (interface{}, error) {
	var hash common.Hash
	copy(hash[:], common.FromHex(hashHex))
	return d.backend.TraceTransaction(hash, config)
}

// TraceBlockByNumber serves debug_traceBlockByNumber by tracing every
// transaction in the selected canonical block. It reuses TraceTransaction so
// historical execution uses the same archive state and strict tx/block readers
// as single-transaction tracing.
func (d *DebugAPI) TraceBlockByNumber(blockParam string, config *tracers.TraceConfig) ([]blockTraceResult, error) {
	block, err := blockByNumberOrHash(d.backend, blockParam)
	if err != nil {
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found")
	}
	return d.traceBlock(block, config)
}

// TraceBlockByHash serves debug_traceBlockByHash by tracing every transaction
// in the block resolved through the archive-aware hash lookup path.
func (d *DebugAPI) TraceBlockByHash(hashHex string, config *tracers.TraceConfig) ([]blockTraceResult, error) {
	var hash common.Hash
	copy(hash[:], common.FromHex(hashHex))
	block, err := d.backend.GetBlockByHash(hash)
	if err != nil {
		if blockLookupNotFound(err) {
			return nil, fmt.Errorf("block not found")
		}
		return nil, err
	}
	if block == nil {
		return nil, fmt.Errorf("block not found")
	}
	return d.traceBlock(block, config)
}

func (d *DebugAPI) traceBlock(block *types.Block, config *tracers.TraceConfig) ([]blockTraceResult, error) {
	txs := block.Transactions()
	results := make([]blockTraceResult, 0, len(txs))
	for _, tx := range txs {
		if tx == nil {
			return nil, fmt.Errorf("block %d contains nil transaction", block.Number())
		}
		hash := tx.Hash()
		result, err := d.backend.TraceTransaction(hash, config)
		item := blockTraceResult{TxHash: "0x" + hash.Hex()}
		if err != nil {
			item.Error = err.Error()
		} else {
			item.Result = result
		}
		results = append(results, item)
	}
	return results, nil
}

// resolveTraceBlock maps a block tag to a *uint64 block number: a nil/empty tag
// or "latest"/"pending" selects head (nil); otherwise the parsed number. It
// mirrors EthAPI.resolveBlock's sentinel handling.
func (d *DebugAPI) resolveTraceBlock(block *string) (*uint64, error) {
	if block == nil || *block == "" {
		return nil, nil
	}
	num, err := parseBlockParam(*block)
	if err != nil {
		return nil, err
	}
	if num == ^uint64(0) { // "latest"/"pending" sentinel
		return nil, nil
	}
	return &num, nil
}
