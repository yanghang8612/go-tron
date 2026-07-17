package jsonrpc

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	"github.com/tronprotocol/go-tron/vm/tracers"
)

// LogFilter selects logs across a block range with optional address/topic constraints.
type LogFilter struct {
	FromBlock *uint64
	ToBlock   *uint64
	BlockHash *common.Hash
	Addresses []common.Address
	Topics    [][]common.Hash // Topics[i] = required hashes for position i; nil = any
}

// RPCLog is an Ethereum-format event log entry.
type RPCLog struct {
	Address          string   `json:"address"`
	Topics           []string `json:"topics"`
	Data             string   `json:"data"`
	BlockNumber      string   `json:"blockNumber"`
	BlockTimestamp   string   `json:"blockTimestamp"`
	TransactionHash  string   `json:"transactionHash"`
	TransactionIndex string   `json:"transactionIndex"`
	BlockHash        string   `json:"blockHash"`
	LogIndex         string   `json:"logIndex"`
	Removed          bool     `json:"removed"`
}

// Backend is the data-access interface for the JSON-RPC API.
// Implemented by core.TronBackend.
type Backend interface {
	// Chain metadata
	ChainID() int64
	BlockNumber() uint64

	// Block queries — same signatures as tronapi.Backend, already on TronBackend
	GetBlockByNumber(num uint64) (*types.Block, error)
	GetBlockByHash(hash common.Hash) (*types.Block, error)

	// Account state (always reads latest/current state)
	GetBalance(addr common.Address) int64 // returns SUN; handler multiplies by 1e12
	GetCode(addr common.Address) []byte
	GetStorageAt(addr common.Address, slot common.Hash) common.Hash

	// Archive state — the value AS OF the end of blockNum, reconstructed via
	// the State History Index. Callers pass the resolved block number (the
	// handler turns "latest"/"earliest"/"pending"/hex into a number first).
	// On a node not synced with --history.enabled, a query for a block older
	// than head returns an error; a query at head resolves from live state.
	GetBalanceAt(addr common.Address, blockNum uint64) (int64, error) // SUN; handler multiplies by 1e12
	GetCodeAt(addr common.Address, blockNum uint64) ([]byte, error)
	GetStorageAtBlock(addr common.Address, slot common.Hash, blockNum uint64) (common.Hash, error)

	// Transaction queries
	GetTransactionByHash(hash common.Hash) (*corepb.Transaction, *types.Block, int, error)
	GetTransactionInfo(hash common.Hash) (*corepb.TransactionInfo, error)

	// TVM execution (read-only simulation)
	Call(from, to *common.Address, data []byte, value int64) ([]byte, error)

	// Tracing (debug namespace). TraceCall replays a read-only call with the
	// configured tracer (blockNumber nil = head, else archive at that block);
	// TraceTransaction re-executes a historical tx from its parent state. Both
	// return the tracer's rendered result.
	TraceCall(from, to *common.Address, data []byte, value int64, blockNumber *uint64, cfg *tracers.TraceConfig) (interface{}, error)
	TraceTransaction(hash common.Hash, cfg *tracers.TraceConfig) (interface{}, error)

	// EstimateGas simulates execution and returns energy used.
	EstimateGas(from, to *common.Address, data []byte, value int64) (uint64, error)

	// Log queries
	GetLogs(filter LogFilter) ([]*RPCLog, error)

	// Node metadata
	GasPrice() int64 // energy fee in SUN per energy unit
	PeerCount() int

	// Block subscriptions for the filter subsystem
	SubscribeBlocks(ch chan<- *types.Block)
	UnsubscribeBlocks(ch chan<- *types.Block)
}
