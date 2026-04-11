package jsonrpc

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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

	// Transaction queries
	GetTransactionByHash(hash common.Hash) (*corepb.Transaction, *types.Block, int, error)
	GetTransactionInfo(hash common.Hash) (*corepb.TransactionInfo, error)

	// EVM execution (read-only simulation)
	Call(from, to *common.Address, data []byte, value int64) ([]byte, error)

	// Log queries
	GetLogs(filter LogFilter) ([]*RPCLog, error)
}
