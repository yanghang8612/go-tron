package tronapi

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type NodeInfo struct {
	Version      string `json:"version"`
	CurrentBlock uint64 `json:"currentBlock"`
}

// TriggerResult holds the result of a constant contract call.
type TriggerResult struct {
	Result     []byte `json:"result"`
	EnergyUsed int64  `json:"energy_used"`
}

type Backend interface {
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) (*types.Block, error)
	GetAccount(addr common.Address) (*types.Account, error)
	BroadcastTransaction(tx *types.Transaction) error
	GetNodeInfo() *NodeInfo
	PendingTransactionCount() int
	GetContract(addr common.Address) (*contractpb.SmartContract, error)
	TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*TriggerResult, error)
}
