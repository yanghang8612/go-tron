package jsonrpc

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

// DebugAPI implements the geth-compatible "debug" JSON-RPC namespace
// (debug_traceCall, debug_traceTransaction) on the reflection-based
// internal/rpc framework. Method names map by the framework's first-letter
// lowering: TraceCall -> debug_traceCall, TraceTransaction -> debug_traceTransaction.
//
// NOTE: distinct from internal/debugapi, which is the pprof HTTP server — this
// is the JSON-RPC tracing namespace.
type DebugAPI struct {
	backend Backend
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
		a, err := parseCompatibleAddress(tx.From)
		if err != nil {
			return nil, err
		}
		from = &a
	}
	to, err := parseCompatibleAddress(tx.To)
	if err != nil {
		return nil, err
	}
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
