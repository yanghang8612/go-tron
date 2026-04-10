package tronapi

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type NodeInfo struct {
	Version      string `json:"version"`
	CurrentBlock uint64 `json:"currentBlock"`
}

type TriggerResult struct {
	Result     []byte `json:"result"`
	EnergyUsed int64  `json:"energy_used"`
}

type AccountResource struct {
	FreeNetUsed       int64 `json:"freeNetUsed"`
	FreeNetLimit      int64 `json:"freeNetLimit"`
	NetUsed           int64 `json:"NetUsed"`
	NetLimit          int64 `json:"NetLimit"`
	TotalNetLimit     int64 `json:"TotalNetLimit"`
	TotalNetWeight    int64 `json:"TotalNetWeight"`
	EnergyUsed        int64 `json:"EnergyUsed"`
	EnergyLimit       int64 `json:"EnergyLimit"`
	TotalEnergyLimit  int64 `json:"TotalEnergyLimit"`
	TotalEnergyWeight int64 `json:"TotalEnergyWeight"`
}

type ChainParameter struct {
	Key   string `json:"key"`
	Value int64  `json:"value"`
}

type WitnessInfo struct {
	Address   string `json:"address"`
	VoteCount int64  `json:"voteCount"`
	URL       string `json:"url"`
	IsJobs    bool   `json:"isJobs"`
}

type ProposalInfo struct {
	ProposalID      int64            `json:"proposal_id"`
	ProposerAddress string           `json:"proposer_address"`
	Parameters      map[string]int64 `json:"parameters"`
	ExpirationTime  int64            `json:"expiration_time"`
	CreateTime      int64            `json:"create_time"`
	Approvals       []string         `json:"approvals"`
	State           string           `json:"state"`
}

type Backend interface {
	// Existing
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) (*types.Block, error)
	GetAccount(addr common.Address) (*types.Account, error)
	BroadcastTransaction(tx *types.Transaction) error
	GetNodeInfo() *NodeInfo
	PendingTransactionCount() int
	GetContract(addr common.Address) (*contractpb.SmartContract, error)
	TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*TriggerResult, error)

	// Transaction queries
	GetTransactionByID(txHash common.Hash) (*corepb.Transaction, error)
	GetTransactionInfoByID(txHash common.Hash) (*corepb.TransactionInfo, error)
	GetTransactionInfoByBlockNum(blockNum uint64) ([]*corepb.TransactionInfo, error)

	// Block queries
	GetBlockByHash(hash common.Hash) (*types.Block, error)
	GetBlocksByRange(start, end uint64) ([]*types.Block, error)

	// Transaction building
	BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error)
	BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte,
		feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error)
	BuildTriggerContractTransaction(owner, contract common.Address, data []byte,
		feeLimit int64, callValue int64) (*corepb.Transaction, *TriggerResult, error)
	EstimateEnergy(owner, contract common.Address, data []byte) (int64, error)

	// Resource & chain queries
	GetAccountResource(addr common.Address) (*AccountResource, error)
	GetChainParameters() []ChainParameter
	ListWitnesses() ([]*WitnessInfo, error)
	NextMaintenanceTime() int64

	// Proposal APIs
	BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error)
	BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error)
	BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error)
	ListProposals() ([]*ProposalInfo, error)
}
