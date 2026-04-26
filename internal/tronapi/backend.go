package tronapi

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
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

// PeerInfo describes a connected P2P peer.
type PeerInfo struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

// DelegatedResourceInfo holds the delegation record between two addresses.
type DelegatedResourceInfo struct {
	FromAddress               string `json:"fromAddress"`
	ToAddress                 string `json:"toAddress"`
	FrozenBalanceForBandwidth int64  `json:"frozenBalanceForBandwidth"`
	FrozenBalanceForEnergy    int64  `json:"frozenBalanceForEnergy"`
	ExpireTimeForBandwidth    int64  `json:"expireTimeForBandwidth"`
	ExpireTimeForEnergy       int64  `json:"expireTimeForEnergy"`
}

// DelegationIndexInfo lists all addresses that addr has delegated resources to.
type DelegationIndexInfo struct {
	Account     string   `json:"account"`
	ToAddresses []string `json:"toAddresses"`
}

// CanDelegateInfo reports how much resource an address can still delegate.
type CanDelegateInfo struct {
	MaxSize         int64 `json:"maxSize"`
	CanDelegateSize int64 `json:"canDelegateSize"`
	Balance         int64 `json:"balance"`
}

// CanWithdrawUnfreezeInfo holds the total withdrawable expired-unfreeze amount.
type CanWithdrawUnfreezeInfo struct {
	Amount int64 `json:"amount"`
}

// AvailableUnfreezeCountInfo holds the number of remaining unfreeze slots (max 32).
type AvailableUnfreezeCountInfo struct {
	Count int64 `json:"count"`
}

// RewardInfo holds unclaimed witness reward (allowance).
type RewardInfo struct {
	Reward int64 `json:"reward"`
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

	// Stake 2.0 transaction building
	BuildFreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error)
	BuildUnfreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error)
	BuildDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode, lock bool) (*corepb.Transaction, error)
	BuildUnDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode) (*corepb.Transaction, error)
	BuildCancelAllUnfreezeV2Transaction(owner common.Address) (*corepb.Transaction, error)
	BuildWithdrawExpireUnfreezeTransaction(owner common.Address) (*corepb.Transaction, error)

	// Vote
	BuildVoteWitnessTransaction(owner common.Address, votes map[common.Address]int64) (*corepb.Transaction, error)

	// Proposal APIs
	BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error)
	BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error)
	BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error)
	ListProposals() ([]*ProposalInfo, error)

	// Delegation/resource queries (Stake 2.0)
	GetDelegatedResourceV2(from, to common.Address) (*DelegatedResourceInfo, error)
	GetDelegatedResourceAccountIndexV2(addr common.Address) (*DelegationIndexInfo, error)
	CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*CanDelegateInfo, error)
	GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*CanWithdrawUnfreezeInfo, error)
	GetAvailableUnfreezeCount(addr common.Address) (*AvailableUnfreezeCountInfo, error)

	// Rewards
	GetReward(addr common.Address) (*RewardInfo, error)

	// Transaction pool queries
	GetTransactionFromPending(txID string) (*corepb.Transaction, error)
	GetTransactionListFromPending() ([]*corepb.Transaction, error)

	// Network
	ListNodes() ([]*PeerInfo, error)

	// Asset queries (TRC10)
	GetAssetIssueByID(id int64) *contractpb.AssetIssueContract
	GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract
	GetAssetIssueList() []*contractpb.AssetIssueContract
	GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract
	GetAssetIssueByAccount(addr common.Address) *contractpb.AssetIssueContract

	// Market queries (Phase 13)
	GetMarketOrderByID(orderID []byte) *corepb.MarketOrder
	GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder
	GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList

	// Exchange queries
	ListExchanges() ([]*corepb.Exchange, error)

	// Brokerage
	GetBrokerageInfo(addr common.Address) int64

	// Chain-level counters (stubs until dynamic-properties tracking is wired)
	TotalTransaction() int64
	GetBurnTrx() int64

	// Historical price strings (encoded as "blockNum:price,blockNum:price,...")
	GetBandwidthPrices() string
	GetEnergyPrices() string

	// Paginated queries
	ListProposalsPaginated(offset, limit int) ([]*ProposalInfo, error)
	ListExchangesPaginated(offset, limit int) ([]*corepb.Exchange, error)

	// Account / permission (M5.1 PR-1)
	BuildCreateAccountTransaction(owner, account common.Address) (*corepb.Transaction, error)
	BuildUpdateAccountTransaction(owner common.Address, name []byte) (*corepb.Transaction, error)
	BuildSetAccountIdTransaction(owner common.Address, accountID []byte) (*corepb.Transaction, error)
	BuildAccountPermissionUpdateTransaction(c *contractpb.AccountPermissionUpdateContract) (*corepb.Transaction, error)
	GetAccountById(accountID []byte) (*types.Account, error)
	GetAccountNet(addr common.Address) (*apipb.AccountNetMessage, error)

	// Generic contract transaction builder (M5.1 PR-3+)
	// Wraps tronapi.BuildTransaction with head-block context from the chain.
	// Used for contract types that don't need special Go-level parameter handling.
	BuildContractTransaction(contractType corepb.Transaction_Contract_ContractType, contract proto.Message, feeLimit int64) (*corepb.Transaction, error)

	// Transaction builders (M5.1 PR-2)
	BuildTransferAssetTransaction(owner, to common.Address, assetName []byte, amount int64) (*corepb.Transaction, error)
	BuildParticipateAssetIssueTransaction(owner, to common.Address, assetName []byte, amount int64) (*corepb.Transaction, error)
	BuildCreateWitnessTransaction(owner common.Address, url []byte) (*corepb.Transaction, error)
	BuildUpdateWitnessTransaction(owner common.Address, url []byte) (*corepb.Transaction, error)
	BuildWithdrawBalanceTransaction(owner common.Address) (*corepb.Transaction, error)
	BuildUpdateBrokerageTransaction(owner common.Address, brokerage int32) (*corepb.Transaction, error)
	BuildFreezeBalanceV1Transaction(owner common.Address, amount, duration int64, resource corepb.ResourceCode, receiver common.Address) (*corepb.Transaction, error)
	BuildUnfreezeBalanceV1Transaction(owner common.Address, resource corepb.ResourceCode, receiver common.Address) (*corepb.Transaction, error)

	// Proposal queries (M5.1 PR-6)
	GetProposalByID(id int64) (*ProposalInfo, error)

	// Address validation (M5.1 PR-7) — pure utility, no state needed
	ValidateAddress(addr string) (bool, string)
}
