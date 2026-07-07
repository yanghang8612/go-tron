package tronapi_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/crypto"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
)

// stubBackend is a test double for tronapi.Backend.
// Pre-set fields control what each new method returns.
// All pre-existing methods return zero/nil values.
type stubBackend struct {
	delegatedResources []*tronapi.DelegatedResourceInfo
	legacyDelegIndex   *corepb.DelegatedResourceAccountIndex
	delegationIndex    *tronapi.DelegationIndexInfo
	canDelegate        *tronapi.CanDelegateInfo
	canWithdraw        *tronapi.CanWithdrawUnfreezeInfo
	availableUnfreeze  *tronapi.AvailableUnfreezeCountInfo
	reward             *tronapi.RewardInfo
	pendingTx          *corepb.Transaction
	pendingTxList      []*corepb.Transaction
	nodes              []*tronapi.PeerInfo
	// M5.1 PR-1
	accountByID             *types.Account
	accountNet              *apipb.AccountNetMessage
	accountNetErr           error
	accountNetAtErr         error
	accountResource         *tronapi.AccountResource
	accountBalanceResp      *contractpb.AccountBalanceResponse
	accountBalanceErr       error
	blockBalanceTrace       *contractpb.BlockBalanceTrace
	blockBalanceTraceErr    error
	lastAccountBalanceReq   *contractpb.AccountBalanceRequest
	lastBlockBalanceTraceID *contractpb.BlockBalanceTrace_BlockIdentifier
	accountErr              error
	accountAtErr            error
	accountIDErr            error
	accountIDAtErr          error
	contractErr             error
	contractAtErr           error
	blockErr                error
	hashErr                 error
	txErr                   error
	txInfoErr               error
	txInfoByBlockErr        error
	rangeErr                error
	rangeCalls              int
	lastRangeStart          uint64
	lastRangeEnd            uint64
	// For inspecting what contract was passed to BuildContractTransaction
	lastContractType corepb.Transaction_Contract_ContractType
	lastContract     proto.Message
	// M9.7: controlled by test to simulate validate failure
	validateErr error
	// Proposal output divergence test (D-4): canned proposals returned
	// from ListProposals / ListProposalsPaginated / GetProposalByID.
	proposals       []*tronapi.ProposalInfo
	proposalErr     error
	proposalAtErr   error
	witnesses       []*tronapi.WitnessInfo
	nextMaintErr    error
	burnTrx         int64
	burnTrxErr      error
	bandwidthPrices string
	bandwidthErr    error
	energyPrices    string
	energyErr       error
	chainParamsErr  error
	exchanges       []*corepb.Exchange
	exchangeErr     error
	assetErr        error
	marketErr       error
}

// --- Pre-existing Backend methods (all return zero values) ---
func (s *stubBackend) CurrentBlock() *types.Block { return nil }
func (s *stubBackend) GetBlockByNumber(n uint64) (*types.Block, error) {
	if s.blockErr != nil {
		return nil, s.blockErr
	}
	return nil, nil
}
func (s *stubBackend) GetAccount(addr common.Address) (*types.Account, error) {
	if s.accountErr != nil {
		return nil, s.accountErr
	}
	return nil, nil
}
func (s *stubBackend) GetAccountAt(addr common.Address, blockNum uint64) (*types.Account, error) {
	if s.accountAtErr != nil {
		return nil, s.accountAtErr
	}
	return nil, nil
}
func (s *stubBackend) BroadcastTransaction(tx *types.Transaction) error { return nil }
func (s *stubBackend) GetNodeInfo() *tronapi.NodeInfo                   { return &tronapi.NodeInfo{} }
func (s *stubBackend) PendingTransactionCount() int                     { return 0 }
func (s *stubBackend) GetContract(addr common.Address) (*contractpb.SmartContract, error) {
	if s.contractErr != nil {
		return nil, s.contractErr
	}
	return nil, nil
}
func (s *stubBackend) GetContractAt(addr common.Address, blockNum uint64) (*contractpb.SmartContract, error) {
	if s.contractAtErr != nil {
		return nil, s.contractAtErr
	}
	return nil, nil
}
func (s *stubBackend) TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	return nil, nil
}
func (s *stubBackend) TriggerConstantContractAt(owner, contract common.Address, data []byte, energyLimit int64, blockNum uint64) (*tronapi.TriggerResult, error) {
	return nil, nil
}
func (s *stubBackend) GetTransactionByID(h common.Hash) (*corepb.Transaction, error) {
	if s.txErr != nil {
		return nil, s.txErr
	}
	return nil, nil
}
func (s *stubBackend) GetTransactionInfoByID(h common.Hash) (*corepb.TransactionInfo, error) {
	if s.txInfoErr != nil {
		return nil, s.txInfoErr
	}
	return nil, nil
}
func (s *stubBackend) GetTransactionBlockNumByID(h common.Hash) (uint64, bool, error) {
	return 0, false, nil
}
func (s *stubBackend) GetTransactionInfoByBlockNum(n uint64) ([]*corepb.TransactionInfo, error) {
	if s.txInfoByBlockErr != nil {
		return nil, s.txInfoByBlockErr
	}
	return nil, nil
}
func (s *stubBackend) GetBlockByHash(h common.Hash) (*types.Block, error) {
	if s.hashErr != nil {
		return nil, s.hashErr
	}
	return nil, nil
}
func (s *stubBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	s.rangeCalls++
	s.lastRangeStart = start
	s.lastRangeEnd = end
	if s.rangeErr != nil {
		return nil, s.rangeErr
	}
	return nil, nil
}
func (s *stubBackend) BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte, feeLimit int64, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildTriggerContractTransaction(owner, contract common.Address, data []byte, feeLimit int64, callValue int64) (*corepb.Transaction, *tronapi.TriggerResult, error) {
	return nil, nil, nil
}
func (s *stubBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	return 0, nil
}
func (s *stubBackend) EstimateEnergyAt(owner, contract common.Address, data []byte, blockNum uint64) (int64, error) {
	return 0, nil
}
func (s *stubBackend) GetAccountResource(addr common.Address) (*tronapi.AccountResource, error) {
	return s.accountResource, nil
}
func (s *stubBackend) GetAccountResourceAt(addr common.Address, blockNum uint64) (*tronapi.AccountResource, error) {
	return nil, nil
}
func (s *stubBackend) GetAccountBalanceTrace(req *contractpb.AccountBalanceRequest) (*contractpb.AccountBalanceResponse, error) {
	s.lastAccountBalanceReq = req
	if s.accountBalanceErr != nil {
		return nil, s.accountBalanceErr
	}
	return s.accountBalanceResp, nil
}
func (s *stubBackend) GetBlockBalanceTrace(id *contractpb.BlockBalanceTrace_BlockIdentifier) (*contractpb.BlockBalanceTrace, error) {
	s.lastBlockBalanceTraceID = id
	if s.blockBalanceTraceErr != nil {
		return nil, s.blockBalanceTraceErr
	}
	return s.blockBalanceTrace, nil
}
func (s *stubBackend) GetChainParameters() ([]tronapi.ChainParameter, error) {
	if s.chainParamsErr != nil {
		return nil, s.chainParamsErr
	}
	return nil, nil
}
func (s *stubBackend) GetChainParametersAt(blockNum uint64) ([]tronapi.ChainParameter, error) {
	return nil, nil
}
func (s *stubBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) { return s.witnesses, nil }
func (s *stubBackend) ListWitnessesAt(blockNum uint64) ([]*tronapi.WitnessInfo, error) {
	return s.witnesses, nil
}
func (s *stubBackend) NextMaintenanceTime() (int64, error) {
	if s.nextMaintErr != nil {
		return 0, s.nextMaintErr
	}
	return 0, nil
}
func (s *stubBackend) NextMaintenanceTimeAt(blockNum uint64) (int64, error) {
	return 0, nil
}
func (s *stubBackend) BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (s *stubBackend) ListProposals() ([]*tronapi.ProposalInfo, error) {
	if s.proposalErr != nil {
		return nil, s.proposalErr
	}
	return s.proposals, nil
}
func (s *stubBackend) ListProposalsAt(blockNum uint64) ([]*tronapi.ProposalInfo, error) {
	if s.proposalErr != nil {
		return nil, s.proposalErr
	}
	return s.proposals, nil
}

// --- New Phase 10 methods ---
func (s *stubBackend) GetDelegatedResource(from, to common.Address) ([]*tronapi.DelegatedResourceInfo, error) {
	return s.delegatedResources, nil
}
func (s *stubBackend) GetDelegatedResourceAt(from, to common.Address, blockNum uint64) ([]*tronapi.DelegatedResourceInfo, error) {
	return s.delegatedResources, nil
}
func (s *stubBackend) GetDelegatedResourceAccountIndex(addr common.Address) (*corepb.DelegatedResourceAccountIndex, error) {
	return s.legacyDelegIndex, nil
}
func (s *stubBackend) GetDelegatedResourceAccountIndexAt(addr common.Address, blockNum uint64) (*corepb.DelegatedResourceAccountIndex, error) {
	return s.legacyDelegIndex, nil
}
func (s *stubBackend) GetDelegatedResourceV2(from, to common.Address) ([]*tronapi.DelegatedResourceInfo, error) {
	return s.delegatedResources, nil
}
func (s *stubBackend) GetDelegatedResourceV2At(from, to common.Address, blockNum uint64) ([]*tronapi.DelegatedResourceInfo, error) {
	return s.delegatedResources, nil
}
func (s *stubBackend) GetDelegatedResourceAccountIndexV2(addr common.Address) (*tronapi.DelegationIndexInfo, error) {
	return s.delegationIndex, nil
}
func (s *stubBackend) GetDelegatedResourceAccountIndexV2At(addr common.Address, blockNum uint64) (*tronapi.DelegationIndexInfo, error) {
	return s.delegationIndex, nil
}
func (s *stubBackend) CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	return s.canDelegate, nil
}
func (s *stubBackend) CanDelegateResourceAt(addr common.Address, amount int64, resource corepb.ResourceCode, blockNum uint64) (*tronapi.CanDelegateInfo, error) {
	return s.canDelegate, nil
}
func (s *stubBackend) GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	return s.canWithdraw, nil
}
func (s *stubBackend) GetCanWithdrawUnfreezeAmountAt(addr common.Address, timestamp int64, blockNum uint64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	return s.canWithdraw, nil
}
func (s *stubBackend) GetAvailableUnfreezeCount(addr common.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	return s.availableUnfreeze, nil
}
func (s *stubBackend) GetAvailableUnfreezeCountAt(addr common.Address, blockNum uint64) (*tronapi.AvailableUnfreezeCountInfo, error) {
	return s.availableUnfreeze, nil
}
func (s *stubBackend) GetReward(addr common.Address) (*tronapi.RewardInfo, error) {
	return s.reward, nil
}
func (s *stubBackend) GetRewardAt(addr common.Address, blockNum uint64) (*tronapi.RewardInfo, error) {
	return s.reward, nil
}
func (s *stubBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	if s.pendingTx == nil {
		return nil, fmt.Errorf("transaction not found")
	}
	return s.pendingTx, nil
}
func (s *stubBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) {
	return s.pendingTxList, nil
}
func (s *stubBackend) ListNodes() ([]*tronapi.PeerInfo, error) {
	return s.nodes, nil
}

// --- New Phase 12 methods (TRC10 asset queries) ---
func (s *stubBackend) GetAssetIssueByID(id int64) (*contractpb.AssetIssueContract, error) {
	return nil, s.assetErr
}
func (s *stubBackend) GetAssetIssueByIDAt(id int64, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	return nil, nil
}
func (s *stubBackend) GetAssetIssueByName(name []byte) (*contractpb.AssetIssueContract, error) {
	return nil, s.assetErr
}
func (s *stubBackend) GetAssetIssueByNameAt(name []byte, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	return nil, nil
}
func (s *stubBackend) GetAssetIssueList() ([]*contractpb.AssetIssueContract, error) {
	return nil, s.assetErr
}
func (s *stubBackend) GetAssetIssueListAt(blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	return nil, nil
}
func (s *stubBackend) GetAssetIssueListPaginated(offset, limit int) ([]*contractpb.AssetIssueContract, error) {
	return nil, s.assetErr
}
func (s *stubBackend) GetAssetIssueListPaginatedAt(offset, limit int, blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	return nil, nil
}
func (s *stubBackend) GetAssetIssueByAccount(addr common.Address) (*contractpb.AssetIssueContract, error) {
	return nil, s.assetErr
}
func (s *stubBackend) GetAssetIssueByAccountAt(addr common.Address, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	return nil, nil
}

// --- New Phase 13 methods (Market order queries) ---
func (s *stubBackend) GetMarketOrderByID(orderID []byte) (*corepb.MarketOrder, error) {
	return nil, s.marketErr
}
func (s *stubBackend) GetMarketOrderByIDAt(orderID []byte, blockNum uint64) (*corepb.MarketOrder, error) {
	return nil, nil
}
func (s *stubBackend) GetMarketOrdersByAccount(addr common.Address) ([]*corepb.MarketOrder, error) {
	return nil, s.marketErr
}
func (s *stubBackend) GetMarketOrdersByAccountAt(addr common.Address, blockNum uint64) ([]*corepb.MarketOrder, error) {
	return nil, nil
}
func (s *stubBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) (*corepb.MarketPriceList, error) {
	return nil, s.marketErr
}
func (s *stubBackend) GetMarketPriceByPairAt(sellTokenID, buyTokenID []byte, blockNum uint64) (*corepb.MarketPriceList, error) {
	return nil, nil
}
func (s *stubBackend) GetMarketOrderListByPair(sellTokenID, buyTokenID []byte) ([]*corepb.MarketOrder, error) {
	return nil, s.marketErr
}
func (s *stubBackend) GetMarketOrderListByPairAt(sellTokenID, buyTokenID []byte, blockNum uint64) ([]*corepb.MarketOrder, error) {
	return nil, nil
}
func (s *stubBackend) GetMarketPairList() (*corepb.MarketOrderPairList, error) {
	return nil, s.marketErr
}
func (s *stubBackend) GetMarketPairListAt(blockNum uint64) (*corepb.MarketOrderPairList, error) {
	return nil, nil
}
func (s *stubBackend) ListExchanges() ([]*corepb.Exchange, error) {
	if s.exchangeErr != nil {
		return nil, s.exchangeErr
	}
	return s.exchanges, nil
}
func (s *stubBackend) ListExchangesAt(blockNum uint64) ([]*corepb.Exchange, error) {
	if s.exchangeErr != nil {
		return nil, s.exchangeErr
	}
	return s.exchanges, nil
}
func (s *stubBackend) GetExchangeByID(id int64) (*corepb.Exchange, error) {
	if s.exchangeErr != nil {
		return nil, s.exchangeErr
	}
	for _, exchange := range s.exchanges {
		if exchange.GetExchangeId() == id {
			return exchange, nil
		}
	}
	return nil, nil
}
func (s *stubBackend) GetExchangeByIDAt(id int64, blockNum uint64) (*corepb.Exchange, error) {
	return s.GetExchangeByID(id)
}
func (s *stubBackend) GetBrokerageInfo(addr common.Address) (int64, error) { return 0, nil }
func (s *stubBackend) GetBrokerageInfoAt(addr common.Address, blockNum uint64) (int64, error) {
	return 0, nil
}
func (s *stubBackend) TotalTransaction() (int64, error) { return 0, nil }
func (s *stubBackend) GetBurnTrx() (int64, error) {
	if s.burnTrxErr != nil {
		return 0, s.burnTrxErr
	}
	return s.burnTrx, nil
}
func (s *stubBackend) GetBurnTrxAt(blockNum uint64) (int64, error) {
	return 0, nil
}
func (s *stubBackend) BuildFreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUnfreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode, lock bool) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUnDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildCancelAllUnfreezeV2Transaction(owner common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildWithdrawExpireUnfreezeTransaction(owner common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildVoteWitnessTransaction(owner common.Address, votes map[common.Address]int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) GetBandwidthPrices() (string, error) {
	if s.bandwidthErr != nil {
		return "", s.bandwidthErr
	}
	return s.bandwidthPrices, nil
}
func (s *stubBackend) GetBandwidthPricesAt(blockNum uint64) (string, error) {
	return "", nil
}
func (s *stubBackend) GetEnergyPrices() (string, error) {
	if s.energyErr != nil {
		return "", s.energyErr
	}
	return s.energyPrices, nil
}
func (s *stubBackend) GetEnergyPricesAt(blockNum uint64) (string, error) {
	return "", nil
}
func (s *stubBackend) ListProposalsPaginated(offset, limit int) ([]*tronapi.ProposalInfo, error) {
	if s.proposalErr != nil {
		return nil, s.proposalErr
	}
	if len(s.proposals) == 0 {
		return nil, nil
	}
	if offset >= len(s.proposals) {
		return []*tronapi.ProposalInfo{}, nil
	}
	end := offset + limit
	if end > len(s.proposals) {
		end = len(s.proposals)
	}
	return s.proposals[offset:end], nil
}
func (s *stubBackend) ListProposalsPaginatedAt(offset, limit int, blockNum uint64) ([]*tronapi.ProposalInfo, error) {
	return s.ListProposalsPaginated(offset, limit)
}
func (s *stubBackend) ListExchangesPaginated(offset, limit int) ([]*corepb.Exchange, error) {
	if s.exchangeErr != nil {
		return nil, s.exchangeErr
	}
	if len(s.exchanges) == 0 {
		return nil, nil
	}
	if offset >= len(s.exchanges) {
		return []*corepb.Exchange{}, nil
	}
	end := offset + limit
	if end > len(s.exchanges) {
		end = len(s.exchanges)
	}
	return s.exchanges[offset:end], nil
}
func (s *stubBackend) ListExchangesPaginatedAt(offset, limit int, blockNum uint64) ([]*corepb.Exchange, error) {
	return s.ListExchangesPaginated(offset, limit)
}

// --- M5.1 PR-1: Account / permission ---
func (s *stubBackend) BuildCreateAccountTransaction(owner, account common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUpdateAccountTransaction(owner common.Address, name []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildSetAccountIdTransaction(owner common.Address, accountID []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildAccountPermissionUpdateTransaction(c *contractpb.AccountPermissionUpdateContract) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) GetAccountById(accountID []byte) (*types.Account, error) {
	if s.accountIDErr != nil {
		return nil, s.accountIDErr
	}
	if s.accountByID != nil {
		return s.accountByID, nil
	}
	return nil, fmt.Errorf("account not found")
}
func (s *stubBackend) GetAccountByIdAt(accountID []byte, blockNum uint64) (*types.Account, error) {
	if s.accountIDAtErr != nil {
		return nil, s.accountIDAtErr
	}
	if s.accountByID != nil {
		return s.accountByID, nil
	}
	return nil, fmt.Errorf("account not found")
}
func (s *stubBackend) GetAccountNet(addr common.Address) (*apipb.AccountNetMessage, error) {
	if s.accountNetErr != nil {
		return nil, s.accountNetErr
	}
	if s.accountNet != nil {
		return s.accountNet, nil
	}
	return nil, nil
}
func (s *stubBackend) GetAccountNetAt(addr common.Address, blockNum uint64) (*apipb.AccountNetMessage, error) {
	if s.accountNetAtErr != nil {
		return nil, s.accountNetAtErr
	}
	if s.accountNet != nil {
		return s.accountNet, nil
	}
	return nil, nil
}

// --- M5.1 PR-2: Transaction builders ---
func (s *stubBackend) BuildTransferAssetTransaction(owner, to common.Address, assetName []byte, amount int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildParticipateAssetIssueTransaction(owner, to common.Address, assetName []byte, amount int64) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildCreateWitnessTransaction(owner common.Address, url []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUpdateWitnessTransaction(owner common.Address, url []byte) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildWithdrawBalanceTransaction(owner common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUpdateBrokerageTransaction(owner common.Address, brokerage int32) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildFreezeBalanceV1Transaction(owner common.Address, amount, duration int64, resource corepb.ResourceCode, receiver common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) BuildUnfreezeBalanceV1Transaction(owner common.Address, resource corepb.ResourceCode, receiver common.Address) (*corepb.Transaction, error) {
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}

// --- M5.1 PR-3+: Generic contract builder + misc ---
func (s *stubBackend) BuildContractTransaction(contractType corepb.Transaction_Contract_ContractType, contract proto.Message, feeLimit int64) (*corepb.Transaction, error) {
	s.lastContractType = contractType
	s.lastContract = contract
	return &corepb.Transaction{RawData: &corepb.TransactionRaw{}}, nil
}
func (s *stubBackend) GetProposalByID(id int64) (*tronapi.ProposalInfo, error) {
	if s.proposalErr != nil {
		return nil, s.proposalErr
	}
	for _, p := range s.proposals {
		if p.ProposalID == id {
			return p, nil
		}
	}
	if id == 1 {
		return &tronapi.ProposalInfo{ProposalID: 1}, nil
	}
	return nil, fmt.Errorf("proposal %d not found", id)
}
func (s *stubBackend) GetProposalByIDAt(id int64, blockNum uint64) (*tronapi.ProposalInfo, error) {
	if s.proposalAtErr != nil {
		return nil, s.proposalAtErr
	}
	return s.GetProposalByID(id)
}
func (s *stubBackend) ValidateAddress(addr string) (bool, string) {
	return len(addr) == 42, "test"
}

// --- M8.1: confirmation-depth stubs ---
func (s *stubBackend) SolidifiedBlockNum() uint64 { return 0 }
func (s *stubBackend) LatestPbftBlockNum() int64  { return -1 }

// --- M9.7: synchronous actuator validate ---
func (s *stubBackend) ValidateTransaction(tx *types.Transaction) error {
	return s.validateErr
}

// --- Helpers ---
func newTestServer(t *testing.T, stub *stubBackend) *httptest.Server {
	t.Helper()
	api := tronapi.NewAPI(stub)
	mux := http.NewServeMux()
	api.RegisterRoutes(mux)
	return httptest.NewServer(mux)
}

func postJSON(t *testing.T, url, body string) map[string]interface{} {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("POST %s: status %d", url, resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON from %s: %v", url, err)
	}
	return result
}

func TestGetBlockByLimitNextRejectsInvalidRangeBeforeBackend(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{name: "negative start", body: `{"startNum":-1,"endNum":3}`},
		{name: "negative end", body: `{"startNum":1,"endNum":-3}`},
		{name: "empty", body: `{"startNum":3,"endNum":3}`},
		{name: "reversed", body: `{"startNum":4,"endNum":3}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			stub := &stubBackend{}
			srv := newTestServer(t, stub)
			defer srv.Close()

			resp, err := http.Post(srv.URL+"/wallet/getblockbylimitnext", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("POST getblockbylimitnext: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
			}
			if stub.rangeCalls != 0 {
				t.Fatalf("GetBlocksByRange called %d times for invalid range, want 0", stub.rangeCalls)
			}
		})
	}
}

func TestGetBlockByLimitNextBackendErrorReturnsEmptyList(t *testing.T) {
	stub := &stubBackend{rangeErr: errors.New("rawdb: block 2 decode: corrupt")}
	srv := newTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getblockbylimitnext", "application/json", strings.NewReader(`{"startNum":1,"endNum":3}`))
	if err != nil {
		t.Fatalf("POST getblockbylimitnext: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	var body struct {
		Block []json.RawMessage `json:"block"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode getblockbylimitnext: %v", err)
	}
	if len(body.Block) != 0 {
		t.Fatalf("block list len = %d, want 0", len(body.Block))
	}
	if stub.rangeCalls != 1 || stub.lastRangeStart != 1 || stub.lastRangeEnd != 3 {
		t.Fatalf("range call = %d [%d,%d), want 1 [1,3)", stub.rangeCalls, stub.lastRangeStart, stub.lastRangeEnd)
	}
}

func TestGetBlockByNumBackendErrorReturnsEmpty(t *testing.T) {
	backendErr := errors.New("rawdb: block 1 decode: corrupt")
	srv := newTestServer(t, &stubBackend{blockErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getblockbynum", "application/json", strings.NewReader(`{"num":1}`))
	if err != nil {
		t.Fatalf("POST getblockbynum: %v", err)
	}
	defer resp.Body.Close()
	assertHTTPEmptyObject(t, resp)
}

func TestGetBlockByNumNotFoundReturnsEmpty(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getblockbynum", "application/json", strings.NewReader(`{"num":1}`))
	if err != nil {
		t.Fatalf("POST getblockbynum: %v", err)
	}
	defer resp.Body.Close()
	assertHTTPEmptyObject(t, resp)
}

func TestGetBlockByIdBackendErrorReturnsEmpty(t *testing.T) {
	backendErr := errors.New("rawdb: cold block index corrupt")
	srv := newTestServer(t, &stubBackend{hashErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getblockbyid", "application/json", strings.NewReader(`{"value":"aabbcc"}`))
	if err != nil {
		t.Fatalf("POST getblockbyid: %v", err)
	}
	defer resp.Body.Close()
	assertHTTPEmptyObject(t, resp)
}

func TestGetAccountBackendErrorReturnsInternal(t *testing.T) {
	backendErr := errors.New("state history: cold account segment corrupt")
	srv := newTestServer(t, &stubBackend{accountErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getaccount", "application/json", strings.NewReader(`{"address":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("POST getaccount: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getaccount status = %d, want 500", resp.StatusCode)
	}
}

func TestGetAccountPreservesNotFoundErrorAsEmpty(t *testing.T) {
	srv := newTestServer(t, &stubBackend{accountErr: errors.New("account not found")})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getaccount", "application/json", strings.NewReader(`{"address":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("POST getaccount: %v", err)
	}
	defer resp.Body.Close()
	assertHTTPEmptyObject(t, resp)
}

func TestGetAccountByIdBackendErrorReturnsInternal(t *testing.T) {
	backendErr := errors.New("state history: cold account-id index corrupt")
	srv := newTestServer(t, &stubBackend{accountIDErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getaccountbyid", "application/json", strings.NewReader(`{"account_id":"user1234"}`))
	if err != nil {
		t.Fatalf("POST getaccountbyid: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getaccountbyid status = %d, want 500", resp.StatusCode)
	}
}

func TestGetContractBackendErrorReturnsInternal(t *testing.T) {
	backendErr := errors.New("state latest: contract metadata corrupt")
	srv := newTestServer(t, &stubBackend{contractErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getcontract", "application/json", strings.NewReader(`{"value":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("POST getcontract: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getcontract status = %d, want 500", resp.StatusCode)
	}
}

func TestGetContractPreservesNotFoundErrorAsEmpty(t *testing.T) {
	srv := newTestServer(t, &stubBackend{contractErr: errors.New("contract not found")})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getcontract", "application/json", strings.NewReader(`{"value":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("POST getcontract: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getcontract status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if strings.TrimSpace(string(body)) != "{}" {
		t.Fatalf("getcontract body = %q, want {}", string(body))
	}
}

func TestGetAccountBalanceTrace(t *testing.T) {
	addr := make([]byte, common.AddressLength)
	addr[0] = common.AddressPrefixMainnet
	for i := 1; i < len(addr); i++ {
		addr[i] = byte(i)
	}
	hash := testBytes(common.HashLength, 0x80)
	stub := &stubBackend{
		accountBalanceResp: &contractpb.AccountBalanceResponse{
			Balance: 123_456,
			BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
				Number: 7,
				Hash:   hash,
			},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	body := fmt.Sprintf(`{"account_identifier":{"address":"%s"},"block_identifier":{"number":7,"hash":"%s"}}`,
		hex.EncodeToString(addr), hex.EncodeToString(hash))
	got := postJSON(t, srv.URL+"/wallet/getaccountbalance", body)
	if got["balance"].(float64) != 123456 {
		t.Fatalf("balance = %v, want 123456", got["balance"])
	}
	blockID := got["block_identifier"].(map[string]interface{})
	if blockID["number"].(float64) != 7 || blockID["hash"].(string) != hex.EncodeToString(hash) {
		t.Fatalf("block_identifier = %+v", blockID)
	}
	if stub.lastAccountBalanceReq == nil {
		t.Fatal("backend was not called")
	}
	if hex.EncodeToString(stub.lastAccountBalanceReq.GetAccountIdentifier().GetAddress()) != hex.EncodeToString(addr) {
		t.Fatalf("backend address = %x, want %x", stub.lastAccountBalanceReq.GetAccountIdentifier().GetAddress(), addr)
	}
	if stub.lastAccountBalanceReq.GetBlockIdentifier().GetNumber() != 7 ||
		hex.EncodeToString(stub.lastAccountBalanceReq.GetBlockIdentifier().GetHash()) != hex.EncodeToString(hash) {
		t.Fatalf("backend block id = %+v", stub.lastAccountBalanceReq.GetBlockIdentifier())
	}
}

func TestGetAccountBalanceTraceBackendErrorReturnsInternal(t *testing.T) {
	hash := testBytes(common.HashLength, 0x80)
	stub := &stubBackend{
		accountBalanceErr: errors.New("read account balance trace segment"),
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	body := fmt.Sprintf(`{"account_identifier":{"address":"4100000000000000000000000000000000000000aa"},"block_identifier":{"number":7,"hash":"%s"}}`,
		hex.EncodeToString(hash))
	resp, err := http.Post(srv.URL+"/wallet/getaccountbalance", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST getaccountbalance: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getaccountbalance status = %d, want 500", resp.StatusCode)
	}
	if stub.lastAccountBalanceReq == nil {
		t.Fatal("backend was not called")
	}
}

func TestGetBlockBalanceTrace(t *testing.T) {
	hash := testBytes(common.HashLength, 0x33)
	stub := &stubBackend{
		blockBalanceTrace: &contractpb.BlockBalanceTrace{
			BlockIdentifier: &contractpb.BlockBalanceTrace_BlockIdentifier{
				Number: 8,
				Hash:   hash,
			},
			Timestamp: 99_999,
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	body := fmt.Sprintf(`{"number":8,"hash":"%s"}`, hex.EncodeToString(hash))
	got := postJSON(t, srv.URL+"/wallet/getblockbalancetrace", body)
	if got["timestamp"].(float64) != 99999 {
		t.Fatalf("timestamp = %v, want 99999", got["timestamp"])
	}
	if stub.lastBlockBalanceTraceID == nil {
		t.Fatal("backend was not called")
	}
	if stub.lastBlockBalanceTraceID.GetNumber() != 8 ||
		hex.EncodeToString(stub.lastBlockBalanceTraceID.GetHash()) != hex.EncodeToString(hash) {
		t.Fatalf("backend block id = %+v", stub.lastBlockBalanceTraceID)
	}
}

func TestGetBlockBalanceTraceBackendErrorReturnsInternal(t *testing.T) {
	hash := testBytes(common.HashLength, 0x33)
	stub := &stubBackend{
		blockBalanceTraceErr: errors.New("read block balance trace segment"),
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	body := fmt.Sprintf(`{"number":8,"hash":"%s"}`, hex.EncodeToString(hash))
	resp, err := http.Post(srv.URL+"/wallet/getblockbalancetrace", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST getblockbalancetrace: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getblockbalancetrace status = %d, want 500", resp.StatusCode)
	}
	if stub.lastBlockBalanceTraceID == nil {
		t.Fatal("backend was not called")
	}
}

func testBytes(n int, start byte) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = start + byte(i)
	}
	return out
}

func assertHTTPEmptyObject(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if strings.TrimSpace(string(body)) != "{}" {
		t.Fatalf("body = %q, want {}", string(body))
	}
}

func assertHTTPEmptyArray(t *testing.T, resp *http.Response) {
	t.Helper()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if strings.TrimSpace(string(body)) != "[]" {
		t.Fatalf("body = %q, want []", string(body))
	}
}

// --- Tests: delegation group ---

func TestGetDelegatedResourceWithData(t *testing.T) {
	stub := &stubBackend{
		delegatedResources: []*tronapi.DelegatedResourceInfo{
			{
				FromAddress:               "4101",
				ToAddress:                 "4102",
				FrozenBalanceForBandwidth: 1000000,
				ExpireTimeForBandwidth:    123,
			},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresource",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 1 {
		t.Fatalf("expected delegatedResource=[1 entry], got %v", result)
	}
}

func TestGetDelegatedResourceV2WithData(t *testing.T) {
	stub := &stubBackend{
		delegatedResources: []*tronapi.DelegatedResourceInfo{
			{
				FromAddress:               "4101",
				ToAddress:                 "4102",
				FrozenBalanceForBandwidth: 1000000,
				ExpireTimeForBandwidth:    0,
			},
			{
				FromAddress:            "4101",
				ToAddress:              "4102",
				FrozenBalanceForEnergy: 1000000,
				ExpireTimeForEnergy:    123456,
			},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourcev2",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 2 {
		t.Fatalf("expected delegatedResource=[2 entries], got %v", result)
	}
}

func TestGetDelegatedResourceV2Empty(t *testing.T) {
	stub := &stubBackend{} // delegatedResources is nil
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourcev2",
		`{"fromAddress":"4101","toAddress":"4102"}`)
	list, ok := result["delegatedResource"].([]interface{})
	if !ok || len(list) != 0 {
		t.Fatalf("expected delegatedResource=[], got %v", result)
	}
}

func TestGetDelegatedResourceAccountIndex(t *testing.T) {
	to := common.Address{0x41, 0x02}
	from := common.Address{0x41, 0x03}
	stub := &stubBackend{
		legacyDelegIndex: &corepb.DelegatedResourceAccountIndex{
			Account:      common.Address{0x41, 0x01}.Bytes(),
			ToAccounts:   [][]byte{to.Bytes()},
			FromAccounts: [][]byte{from.Bytes()},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourceaccountindex",
		`{"value":"4101"}`)
	toAccounts, ok := result["toAccounts"].([]interface{})
	if !ok || len(toAccounts) != 1 || toAccounts[0] != hex.EncodeToString(to.Bytes()) {
		t.Fatalf("expected legacy toAccounts %x, got %v", to.Bytes(), result)
	}
	fromAccounts, ok := result["fromAccounts"].([]interface{})
	if !ok || len(fromAccounts) != 1 || fromAccounts[0] != hex.EncodeToString(from.Bytes()) {
		t.Fatalf("expected legacy fromAccounts %x, got %v", from.Bytes(), result)
	}
}

func TestGetDelegatedResourceAccountIndexV2(t *testing.T) {
	stub := &stubBackend{
		delegationIndex: &tronapi.DelegationIndexInfo{
			Account:     "4101",
			ToAddresses: []string{"4102", "4103"},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getdelegatedresourceaccountindexv2",
		`{"value":"4101"}`)
	addrs, ok := result["toAddresses"].([]interface{})
	if !ok || len(addrs) != 2 {
		t.Fatalf("expected 2 toAddresses, got %v", result)
	}
}

func TestCanDelegateResource(t *testing.T) {
	stub := &stubBackend{
		canDelegate: &tronapi.CanDelegateInfo{MaxSize: 1000000, CanDelegateSize: 800000, Balance: 500000},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/candelegateresource",
		`{"owner_address":"4101","balance":500000,"type":0}`)
	if result["maxSize"].(float64) != 1000000 || result["canDelegateSize"].(float64) != 800000 {
		t.Fatalf("unexpected canDelegate response: %v", result)
	}
}

// --- Tests: unfreeze/reward group ---

func TestGetCanWithdrawUnfreezeAmount(t *testing.T) {
	stub := &stubBackend{
		canWithdraw: &tronapi.CanWithdrawUnfreezeInfo{Amount: 5000000},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getcanwithdrawunfreezeamount",
		`{"owner_address":"4101","timestamp":1712345678000}`)
	if result["amount"].(float64) != 5000000 {
		t.Fatalf("unexpected amount: %v", result)
	}
}

func TestGetAvailableUnfreezeCount(t *testing.T) {
	stub := &stubBackend{
		availableUnfreeze: &tronapi.AvailableUnfreezeCountInfo{Count: 30},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getavailableunfreezecount",
		`{"owner_address":"4101"}`)
	if result["count"].(float64) != 30 {
		t.Fatalf("unexpected count: %v", result)
	}
}

func TestGetBurnTrxAndPrices(t *testing.T) {
	stub := &stubBackend{
		burnTrx:         123456,
		bandwidthPrices: "0:10,100:20",
		energyPrices:    "0:100,200:300",
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	burn := postJSON(t, srv.URL+"/wallet/getburntrx", `{}`)
	if burn["num"].(float64) != 123456 {
		t.Fatalf("getburntrx = %v, want 123456", burn)
	}

	bandwidth := postJSON(t, srv.URL+"/wallet/getbandwidthprices", `{}`)
	if bandwidth["prices"] != "0:10,100:20" {
		t.Fatalf("getbandwidthprices = %v, want 0:10,100:20", bandwidth)
	}

	energy := postJSON(t, srv.URL+"/wallet/getenergyprices", `{}`)
	if energy["prices"] != "0:100,200:300" {
		t.Fatalf("getenergyprices = %v, want 0:100,200:300", energy)
	}
}

func TestLiveDynamicPropertyEndpointsSurfaceBackendErrors(t *testing.T) {
	backendErr := errors.New("load head dynamic properties: corrupt")
	tests := []struct {
		path string
		stub *stubBackend
	}{
		{path: "/wallet/getchainparameters", stub: &stubBackend{chainParamsErr: backendErr}},
		{path: "/wallet/getnextmaintenancetime", stub: &stubBackend{nextMaintErr: backendErr}},
		{path: "/wallet/getburntrx", stub: &stubBackend{burnTrxErr: backendErr}},
		{path: "/wallet/getbandwidthprices", stub: &stubBackend{bandwidthErr: backendErr}},
		{path: "/wallet/getenergyprices", stub: &stubBackend{energyErr: backendErr}},
	}
	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			srv := newTestServer(t, tt.stub)
			defer srv.Close()
			resp, err := http.Post(srv.URL+tt.path, "application/json", strings.NewReader(`{}`))
			if err != nil {
				t.Fatalf("POST %s: %v", tt.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("%s status = %d, want 500", tt.path, resp.StatusCode)
			}
		})
	}
}

func TestGetReward(t *testing.T) {
	stub := &stubBackend{
		reward: &tronapi.RewardInfo{Reward: 123456},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getreward",
		`{"address":"4101"}`)
	if result["reward"].(float64) != 123456 {
		t.Fatalf("unexpected reward: %v", result)
	}
}

// --- Tests: pool group ---

func TestGetTransactionFromPendingFound(t *testing.T) {
	stub := &stubBackend{
		pendingTx: &corepb.Transaction{RawData: &corepb.TransactionRaw{}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionfrompending",
		`{"value":"aabbcc"}`)
	if _, hasRawData := result["raw_data"]; !hasRawData {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestGetTransactionFromPendingNotFound(t *testing.T) {
	stub := &stubBackend{} // pendingTx is nil → returns error
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionfrompending",
		`{"value":"aabbcc"}`)
	if _, hasError := result["Error"]; !hasError {
		t.Fatalf("expected Error field in not-found response, got %v", result)
	}
}

func TestGetTransactionListFromPending(t *testing.T) {
	stub := &stubBackend{pendingTxList: nil} // empty pool
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/gettransactionlistfrompending", `{}`)
	txList, ok := result["transaction"].([]interface{})
	if !ok {
		t.Fatalf("expected transaction array, got %v", result)
	}
	if len(txList) != 0 {
		t.Fatalf("expected empty transaction list, got %d entries", len(txList))
	}
}

// --- Tests: network group ---

func TestListNodes(t *testing.T) {
	stub := &stubBackend{
		nodes: []*tronapi.PeerInfo{{Host: "127.0.0.1", Port: 18888}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/wallet/listnodes")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d", resp.StatusCode)
	}
	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode JSON: %v", err)
	}
	nodes, ok := result["nodes"].([]interface{})
	if !ok || len(nodes) != 1 {
		t.Fatalf("expected nodes=[1 entry], got %v", result)
	}
	node, ok := nodes[0].(map[string]interface{})
	if !ok {
		t.Fatalf("nodes[0] is not an object")
	}
	addr, ok := node["address"].(map[string]interface{})
	if !ok {
		t.Fatalf("node address is not an object")
	}
	if addr["host"] != "127.0.0.1" {
		t.Fatalf("unexpected host: %v", addr["host"])
	}
}

// --- Tests: M5.1 PR-1 account/permission ---

func TestCreateAccount(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/createaccount",
		`{"owner_address":"4101","account_address":"4102"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestUpdateAccount(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/updateaccount",
		`{"owner_address":"4101","account_name":"deadbeef"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestSetAccountId(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	// account_id hex-encodes "myid". Pre-fix the test silently parsed
	// non-hex via the swallowed-error FromHex path; now we either
	// hex-encode or set visible:true.
	result := postJSON(t, srv.URL+"/wallet/setaccountid",
		`{"owner_address":"4101","account_id":"6d796964"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestAccountPermissionUpdate(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/accountpermissionupdate",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}
}

func TestGetAccountByIdFound(t *testing.T) {
	pb := &corepb.Account{Address: []byte{0x41, 0x01}}
	stub := &stubBackend{accountByID: types.NewAccountFromPB(pb)}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountbyid",
		`{"account_id":"myid"}`)
	if result["address"] == nil {
		t.Fatalf("expected address in response, got %v", result)
	}
}

func TestGetAccountByIdNotFound(t *testing.T) {
	stub := &stubBackend{} // accountByID is nil
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountbyid",
		`{"account_id":"unknown"}`)
	if len(result) != 0 {
		t.Fatalf("expected empty object for not-found, got %v", result)
	}
}

func TestGetAccountNet(t *testing.T) {
	stub := &stubBackend{
		accountNet: &apipb.AccountNetMessage{
			FreeNetUsed:   100,
			FreeNetLimit:  1500,
			TotalNetLimit: 43200000000,
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountnet",
		`{"address":"4101"}`)
	// protojson encodes int64 as string
	if result["freeNetUsed"] != "100" {
		t.Fatalf("unexpected freeNetUsed: %v", result)
	}
}

func TestGetAccountNetSurfacesBackendError(t *testing.T) {
	backendErr := errors.New("state history: cold account net dynamic properties corrupt")
	srv := newTestServer(t, &stubBackend{accountNetErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getaccountnet", "application/json", strings.NewReader(`{"address":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("POST getaccountnet: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getaccountnet status = %d, want 500", resp.StatusCode)
	}
}

func TestGetAccountNetPreservesAccountNotFoundAsEmpty(t *testing.T) {
	srv := newTestServer(t, &stubBackend{accountNetErr: errors.New("account not found")})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getaccountnet", "application/json", strings.NewReader(`{"address":"4100000000000000000000000000000000000000aa"}`))
	if err != nil {
		t.Fatalf("POST getaccountnet: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("getaccountnet status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read response: %v", err)
	}
	if strings.TrimSpace(string(body)) != "{}" {
		t.Fatalf("getaccountnet body = %q, want {}", string(body))
	}
}

// TestGetAccountResource_PopulatedFields pins the HTTP JSON contract for the
// fields that previously always came back empty: each must appear under its
// java-matching key (casing matters for SDKs), and an omitempty field left at
// zero must be omitted rather than emitted.
func TestGetAccountResource_PopulatedFields(t *testing.T) {
	stub := &stubBackend{
		accountResource: &tronapi.AccountResource{
			NetLimit:             5000,
			TotalNetWeight:       1000,
			TotalTronPowerWeight: 3000,
			TronPowerUsed:        100,
			TronPowerLimit:       800,
			EnergyUsed:           333,
			EnergyLimit:          9000,
			TotalEnergyWeight:    2000,
			StorageUsed:          555,
			// StorageLimit deliberately left 0 to assert omitempty omits it.
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getaccountresource", `{"address":"4101"}`)

	want := map[string]float64{
		"NetLimit":             5000,
		"TotalNetWeight":       1000,
		"TotalTronPowerWeight": 3000,
		"tronPowerUsed":        100,
		"tronPowerLimit":       800,
		"EnergyUsed":           333,
		"EnergyLimit":          9000,
		"TotalEnergyWeight":    2000,
		"storageUsed":          555,
	}
	for key, v := range want {
		got, ok := result[key]
		if !ok {
			t.Errorf("missing field %q in response %v", key, result)
			continue
		}
		if got.(float64) != v {
			t.Errorf("%s = %v, want %v", key, got, v)
		}
	}
	if _, ok := result["storageLimit"]; ok {
		t.Errorf("storageLimit should be omitted when zero, got %v", result["storageLimit"])
	}
}

// --- Tests: M5.1 PR-2 transaction builders ---

func testTxBuilder(t *testing.T, url, body string) {
	t.Helper()
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+url, body)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response for %s, got %v", url, result)
	}
}

func TestTransferAsset(t *testing.T) {
	testTxBuilder(t, "/wallet/transferasset",
		`{"owner_address":"4101","to_address":"4102","asset_name":"1000001","amount":100}`)
}
func TestParticipateAssetIssue(t *testing.T) {
	testTxBuilder(t, "/wallet/participateassetissue",
		`{"owner_address":"4101","to_address":"4102","asset_name":"1000001","amount":100}`)
}
func TestCreateWitness(t *testing.T) {
	// URL hex-encodes "https://witness.example.com". The handler used to
	// silently accept non-hex via common.FromHex's swallowed error; the
	// P0-2 audit flagged that path so the test must use real hex now.
	testTxBuilder(t, "/wallet/createwitness",
		`{"owner_address":"4101","url":"68747470733a2f2f7769746e6573732e6578616d706c652e636f6d"}`)
}
func TestVoteWitnessAccount(t *testing.T) {
	testTxBuilder(t, "/wallet/votewitnessaccount",
		`{"owner_address":"4101","votes":[{"vote_address":"4102","vote_count":100}]}`)
}
func TestUpdateWitness(t *testing.T) {
	// update_url hex-encodes "https://updated.example.com". Same reason
	// as TestCreateWitness: the FromHex silent-swallow path used to mask
	// non-hex input.
	testTxBuilder(t, "/wallet/updatewitness",
		`{"owner_address":"4101","update_url":"68747470733a2f2f757064617465642e6578616d706c652e636f6d"}`)
}
func TestWithdrawBalance(t *testing.T) {
	testTxBuilder(t, "/wallet/withdrawbalance",
		`{"owner_address":"4101"}`)
}
func TestUpdateBrokerage(t *testing.T) {
	testTxBuilder(t, "/wallet/updatebrokerage",
		`{"owner_address":"4101","brokerage":20}`)
}
func TestFreezeBalance(t *testing.T) {
	testTxBuilder(t, "/wallet/freezebalance",
		`{"owner_address":"4101","frozen_balance":1000000,"frozen_duration":3,"resource":0}`)
}
func TestUnfreezeBalance(t *testing.T) {
	testTxBuilder(t, "/wallet/unfreezebalance",
		`{"owner_address":"4101","resource":0}`)
}
func TestFreezeBalanceV2(t *testing.T) {
	testTxBuilder(t, "/wallet/freezebalancev2",
		`{"owner_address":"4101","frozen_balance":1000000,"resource":0}`)
}
func TestUnfreezeBalanceV2(t *testing.T) {
	testTxBuilder(t, "/wallet/unfreezebalancev2",
		`{"owner_address":"4101","unfreeze_balance":1000000,"resource":0}`)
}
func TestCancelAllUnfreezeV2(t *testing.T) {
	testTxBuilder(t, "/wallet/cancelallunfreezev2",
		`{"owner_address":"4101"}`)
}
func TestDelegateResource(t *testing.T) {
	testTxBuilder(t, "/wallet/delegateresource",
		`{"owner_address":"4101","receiver_address":"4102","balance":1000000,"resource":0}`)
}
func TestUndelegateResource(t *testing.T) {
	testTxBuilder(t, "/wallet/undelegateresource",
		`{"owner_address":"4101","receiver_address":"4102","balance":1000000,"resource":0}`)
}
func TestWithdrawExpireUnfreeze(t *testing.T) {
	testTxBuilder(t, "/wallet/withdrawexpireunfreeze",
		`{"owner_address":"4101"}`)
}

// TestTransferAsset_VisibleBase58: end-to-end exercise of P1 visible=true
// support. java-tron's HTTP API accepts Base58Check addresses + UTF-8
// strings for `asset_name` when visible=true; gtron now does the same.
// Pre-fix the request silently routed to addr(0) (the Base58Check string
// isn't a valid hex address — hex.DecodeString errored, the error was
// swallowed by common.FromHex, BytesToAddress promoted nil to zero).
func TestTransferAsset_VisibleBase58(t *testing.T) {
	owner := common.Address{0x41, 0x01}
	to := common.Address{0x41, 0x02}
	body := `{` +
		`"owner_address":"` + crypto.AddressToBase58(owner) + `",` +
		`"to_address":"` + crypto.AddressToBase58(to) + `",` +
		`"asset_name":"1000001",` +
		`"amount":100,` +
		`"visible":true}`
	testTxBuilder(t, "/wallet/transferasset", body)
}

// TestTransferAsset_RejectsBadHexOwner: the FromHex silent-swallow fix in
// action — a typo'd owner now returns 400 instead of building a tx that
// would later route to addr(0).
func TestTransferAsset_RejectsBadHexOwner(t *testing.T) {
	srv := newTestServer(t, &stubBackend{})
	defer srv.Close()
	body := `{"owner_address":"nothex","to_address":"4102","asset_name":"1000001","amount":100}`
	resp, err := http.Post(srv.URL+"/wallet/transferasset", "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected 400 for non-hex owner_address, got %d", resp.StatusCode)
	}
}

// --- Tests: M5.1 PR-3 TRC10 + PR-4 ClearABI ---

func TestCreateAssetIssue(t *testing.T) {
	testTxBuilder(t, "/wallet/createassetissue",
		`{"owner_address":"4101000000000000000000000000000000000000000000","name":"74657374","abbr":"74","total_supply":1000000,"trx_num":1,"num":1,"start_time":1000,"end_time":2000,"precision":6}`)
}
func TestUpdateAsset(t *testing.T) {
	testTxBuilder(t, "/wallet/updateasset",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}
func TestGetAssetIssueListByName(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getassetissuelistbyname", `{"value":"deadbeef"}`)
	if _, ok := result["assetIssue"]; !ok {
		t.Fatalf("expected assetIssue key, got %v", result)
	}
}

func TestAssetIssueLiveQueriesSurfaceBackendError(t *testing.T) {
	backendErr := errors.New("cold head asset state root corrupt")
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "by id", path: "/wallet/getassetissuebyid", body: `{"value":"1000001"}`},
		{name: "by name", path: "/wallet/getassetissuebyname", body: `{"value":"TOKEN","visible":true}`},
		{name: "list", path: "/wallet/getassetissuelist", body: `{}`},
		{name: "paginated", path: "/wallet/getpaginatedassetissuelist", body: `{"offset":0,"limit":10}`},
		{name: "by account", path: "/wallet/getassetissuebyaccount", body: `{"address":"4101000000000000000000000000000000000000000000"}`},
		{name: "list by name", path: "/wallet/getassetissuelistbyname", body: `{"value":"TOKEN","visible":true}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, &stubBackend{assetErr: backendErr})
			defer srv.Close()
			resp, err := http.Post(srv.URL+tt.path, "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("POST %s: %v", tt.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("%s status = %d, want 500", tt.path, resp.StatusCode)
			}
		})
	}
}

func TestMarketLiveQueriesSurfaceBackendError(t *testing.T) {
	backendErr := errors.New("cold head market state root corrupt")
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "order by id", path: "/wallet/getmarketorderbyid", body: `{"value":"order","visible":true}`},
		{name: "orders by account", path: "/wallet/getmarketordersfromaccount", body: `{"address":"4101000000000000000000000000000000000000000000"}`},
		{name: "price by pair", path: "/wallet/getmarketpricebypair", body: `{"sell_token_id":"sell","buy_token_id":"buy","visible":true}`},
		{name: "order list by pair", path: "/wallet/getmarketorderlistbypair", body: `{"sell_token_id":"sell","buy_token_id":"buy","visible":true}`},
		{name: "pair list", path: "/wallet/getmarketpairlist", body: `{}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, &stubBackend{marketErr: backendErr})
			defer srv.Close()
			resp, err := http.Post(srv.URL+tt.path, "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("POST %s: %v", tt.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("%s status = %d, want 500", tt.path, resp.StatusCode)
			}
		})
	}
}

func TestClearABI(t *testing.T) {
	testTxBuilder(t, "/wallet/clearabi",
		`{"owner_address":"4101","contract_address":"4102"}`)
}

// --- Tests: M5.1 PR-5 Exchange/Market ---

func TestExchangeCreate(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangecreate",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}
func TestExchangeInject(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangeinject",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":1,"token_id":"747278","quant":100}`)
}
func TestExchangeTransaction(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangetransaction",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":1,"token_id":"747278","quant":100,"expected":50}`)
}
func TestExchangeWithdraw(t *testing.T) {
	testTxBuilder(t, "/wallet/exchangewithdraw",
		`{"owner_address":"4101000000000000000000000000000000000000000000","exchange_id":1,"token_id":"747278","quant":100}`)
}
func TestMarketSellAsset(t *testing.T) {
	testTxBuilder(t, "/wallet/marketsellasset",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}
func TestMarketCancelOrder(t *testing.T) {
	testTxBuilder(t, "/wallet/marketcancelorder",
		`{"owner_address":"4101000000000000000000000000000000000000000000"}`)
}

// --- Tests: M9.1 hex decode verification ---

func TestExchangeCreateHexDecode(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()

	// owner_address, first_token_id, second_token_id are all hex strings
	result := postJSON(t, srv.URL+"/wallet/exchangecreate",
		`{"owner_address":"41e2ba4c4a3a8d31db8d893a13c3b0bc40f27ec2ff","first_token_id":"5f","first_token_balance":1000,"second_token_id":"313030303030","second_token_balance":1000}`)
	if _, ok := result["raw_data"]; !ok {
		t.Fatalf("expected raw_data in response, got %v", result)
	}

	// Verify that the contract passed to BuildContractTransaction has hex-decoded bytes.
	if stub.lastContract == nil {
		t.Fatal("lastContract was not captured")
	}
	ec, ok := stub.lastContract.(*contractpb.ExchangeCreateContract)
	if !ok {
		t.Fatalf("expected *ExchangeCreateContract, got %T", stub.lastContract)
	}
	wantOwner := common.FromHex("41e2ba4c4a3a8d31db8d893a13c3b0bc40f27ec2ff")
	if string(ec.OwnerAddress) != string(wantOwner) {
		t.Fatalf("OwnerAddress: got %x, want %x", ec.OwnerAddress, wantOwner)
	}
	wantFirstToken := common.FromHex("5f")
	if string(ec.FirstTokenId) != string(wantFirstToken) {
		t.Fatalf("FirstTokenId: got %x, want %x", ec.FirstTokenId, wantFirstToken)
	}
	if ec.FirstTokenBalance != 1000 {
		t.Fatalf("FirstTokenBalance: got %d, want 1000", ec.FirstTokenBalance)
	}
	wantSecondToken := common.FromHex("313030303030")
	if string(ec.SecondTokenId) != string(wantSecondToken) {
		t.Fatalf("SecondTokenId: got %x, want %x", ec.SecondTokenId, wantSecondToken)
	}
}

// --- Tests: M5.1 PR-6 Proposal/Monitoring ---

func TestGetProposalByIdFound(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":1}`)
	if result["proposal_id"].(float64) != 1 {
		t.Fatalf("expected proposal_id=1, got %v", result)
	}
}
func TestGetProposalByIdNotFound(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":999}`)
	if len(result) != 0 {
		t.Fatalf("expected empty object, got %v", result)
	}
}

func TestGetProposalByIdSurfacesBackendError(t *testing.T) {
	backendErr := errors.New("state history: cold proposal segment corrupt")
	srv := newTestServer(t, &stubBackend{proposalErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/getproposalbyid", "application/json", strings.NewReader(`{"id":42}`))
	if err != nil {
		t.Fatalf("POST getproposalbyid: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("getproposalbyid status = %d, want 500", resp.StatusCode)
	}
}

func TestGetPaginatedProposalList(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/getpaginatedproposallist", `{"offset":0,"limit":10}`)
	if _, ok := result["proposal"]; !ok {
		t.Fatalf("expected proposal key, got %v", result)
	}
}

func TestProposalListQueriesSurfaceBackendError(t *testing.T) {
	backendErr := errors.New("state history: cold proposal index corrupt")
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "list", path: "/wallet/listproposals", body: `{}`},
		{name: "paginated", path: "/wallet/getpaginatedproposallist", body: `{"offset":0,"limit":10}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, &stubBackend{proposalErr: backendErr})
			defer srv.Close()
			resp, err := http.Post(srv.URL+tt.path, "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("POST %s: %v", tt.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("%s status = %d, want 500", tt.path, resp.StatusCode)
			}
		})
	}
}

func TestGetPaginatedNowWitnessList(t *testing.T) {
	stub := &stubBackend{
		witnesses: []*tronapi.WitnessInfo{
			{Address: "000000000000000000000000000000000000000001", VoteCount: 10, URL: "w1"},
			{Address: "000000000000000000000000000000000000000002", VoteCount: 20, URL: "w2", IsJobs: true},
			{Address: "000000000000000000000000000000000000000003", VoteCount: 30, URL: "w3"},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getpaginatednowwitnesslist", `{"offset":1,"limit":1}`)
	witnesses, ok := result["witnesses"].([]interface{})
	if !ok || len(witnesses) != 1 {
		t.Fatalf("expected one witness, got %v", result["witnesses"])
	}
	witness := witnesses[0].(map[string]interface{})
	if witness["url"] != "w2" || witness["voteCount"].(float64) != 20 || witness["isJobs"] != true {
		t.Fatalf("witness = %v, want paginated witness w2", witness)
	}

	resp, err := http.Get(srv.URL + "/wallet/getpaginatednowwitnesslist?offset=2&limit=1")
	if err != nil {
		t.Fatalf("GET getpaginatednowwitnesslist: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET getpaginatednowwitnesslist status = %d", resp.StatusCode)
	}
	var getResult struct {
		Witnesses []tronapi.WitnessInfo `json:"witnesses"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&getResult); err != nil {
		t.Fatalf("decode GET getpaginatednowwitnesslist: %v", err)
	}
	if len(getResult.Witnesses) != 1 || getResult.Witnesses[0].URL != "w3" {
		t.Fatalf("GET witnesses = %+v, want w3", getResult.Witnesses)
	}

	resp, err = http.Get(srv.URL + "/wallet/getpaginatednowwitnesslist?offset=-1&limit=1")
	if err != nil {
		t.Fatalf("GET negative offset getpaginatednowwitnesslist: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("negative offset status = %d, want 400", resp.StatusCode)
	}
}

func TestGetPaginatedExchangeList(t *testing.T) {
	stub := &stubBackend{
		exchanges: []*corepb.Exchange{
			{ExchangeId: 1, FirstTokenBalance: 10, SecondTokenBalance: 100},
			{ExchangeId: 2, FirstTokenBalance: 20, SecondTokenBalance: 200},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getpaginatedexchangelist", `{"offset":1,"limit":1}`)
	exchanges, ok := result["exchanges"].([]interface{})
	if !ok || len(exchanges) != 1 {
		t.Fatalf("expected one exchange, got %v", result["exchanges"])
	}
	exchange := exchanges[0].(map[string]interface{})
	if exchange["exchange_id"].(float64) != 2 || exchange["first_token_balance"].(float64) != 20 {
		t.Fatalf("exchange = %v, want paginated exchange id 2", exchange)
	}

	resp, err := http.Get(srv.URL + "/wallet/getpaginatedexchangelist?offset=0&limit=1")
	if err != nil {
		t.Fatalf("GET getpaginatedexchangelist: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET getpaginatedexchangelist status = %d", resp.StatusCode)
	}
	var getResult struct {
		Exchanges []struct {
			ExchangeID int64 `json:"exchange_id"`
		} `json:"exchanges"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&getResult); err != nil {
		t.Fatalf("decode GET getpaginatedexchangelist: %v", err)
	}
	if len(getResult.Exchanges) != 1 || getResult.Exchanges[0].ExchangeID != 1 {
		t.Fatalf("GET exchanges = %+v, want exchange id 1", getResult.Exchanges)
	}

	resp, err = http.Get(srv.URL + "/wallet/getpaginatedexchangelist?offset=-1&limit=1")
	if err != nil {
		t.Fatalf("GET negative offset getpaginatedexchangelist: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("negative offset status = %d, want 400", resp.StatusCode)
	}
}

func TestGetExchangeByID(t *testing.T) {
	stub := &stubBackend{
		exchanges: []*corepb.Exchange{
			{ExchangeId: 7, FirstTokenBalance: 70, SecondTokenBalance: 700},
		},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getexchangebyid", `{"id":7}`)
	if result["exchange_id"].(float64) != 7 || result["first_token_balance"].(float64) != 70 {
		t.Fatalf("exchange = %v, want exchange 7", result)
	}

	resp, err := http.Get(srv.URL + "/wallet/getexchangebyid?value=7")
	if err != nil {
		t.Fatalf("GET getexchangebyid: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET getexchangebyid status = %d", resp.StatusCode)
	}
	var getResult struct {
		ExchangeID int64 `json:"exchange_id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&getResult); err != nil {
		t.Fatalf("decode GET getexchangebyid: %v", err)
	}
	if getResult.ExchangeID != 7 {
		t.Fatalf("GET exchange id = %d, want 7", getResult.ExchangeID)
	}

	result = postJSON(t, srv.URL+"/wallet/getexchangebyid", `{"id":99}`)
	if len(result) != 0 {
		t.Fatalf("missing exchange result = %v, want empty object", result)
	}
}

func TestExchangeLiveQueriesSurfaceBackendError(t *testing.T) {
	backendErr := errors.New("cold head exchange state root corrupt")
	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "list", path: "/wallet/listexchanges", body: `{}`},
		{name: "paginated", path: "/wallet/getpaginatedexchangelist", body: `{"offset":0,"limit":10}`},
		{name: "by id", path: "/wallet/getexchangebyid", body: `{"id":7}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := newTestServer(t, &stubBackend{exchangeErr: backendErr})
			defer srv.Close()
			resp, err := http.Post(srv.URL+tt.path, "application/json", strings.NewReader(tt.body))
			if err != nil {
				t.Fatalf("POST %s: %v", tt.path, err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusInternalServerError {
				t.Fatalf("%s status = %d, want 500", tt.path, resp.StatusCode)
			}
		})
	}
}

// --- Wire-format parity: parameters MUST be array, not dict ---
//
// java-tron's HTTP serialization flattens proto map<int64,int64> as a
// repeated MapEntry, producing `[{"key":N,"value":V},...]`. SDKs that
// target java-tron break when gtron emits a dict instead.
// Cross-impl divergence ref: docs/dev/cross-impl-divergences-2026-05-02.md.

func TestProposalParametersArrayShape_GetProposalById(t *testing.T) {
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID:      7,
			ProposerAddress: "41" + strings.Repeat("ab", 20),
			Parameters: []tronapi.ProposalParameterEntry{
				{Key: 19, Value: 259200000},
				{Key: 5, Value: 1},
			},
			ExpirationTime: 1234,
			CreateTime:     1000,
			State:          "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":7}`)
	params, ok := result["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array (java-tron parity), got %T: %v",
			result["parameters"], result["parameters"])
	}
	if len(params) != 2 {
		t.Fatalf("expected 2 parameter entries, got %d: %v", len(params), params)
	}
	first, ok := params[0].(map[string]interface{})
	if !ok {
		t.Fatalf("parameter entry must be an object, got %T: %v", params[0], params[0])
	}
	// Field names must match java-tron exactly (lowercase "key"/"value").
	if _, ok := first["key"]; !ok {
		t.Fatalf("parameter entry missing \"key\": %v", first)
	}
	if _, ok := first["value"]; !ok {
		t.Fatalf("parameter entry missing \"value\": %v", first)
	}
	// Spot-check first entry's encoded values — the stub feeds entries
	// in the order {19, 5}, so first should be {key:19,value:259200000}.
	// (TronBackend sorts by key for determinism — covered separately.)
	if first["key"].(float64) != 19 || first["value"].(float64) != 259200000 {
		t.Fatalf("expected {key:19,value:259200000}, got %v", first)
	}
}

func TestProposalParametersArrayShape_ListProposals(t *testing.T) {
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID: 1,
			Parameters: []tronapi.ProposalParameterEntry{
				{Key: 19, Value: 259200000},
			},
			State: "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/listproposals", `{}`)
	proposals, ok := result["proposals"].([]interface{})
	if !ok || len(proposals) != 1 {
		t.Fatalf("expected proposals array of length 1, got %v", result["proposals"])
	}
	first := proposals[0].(map[string]interface{})
	params, ok := first["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array, got %T: %v", first["parameters"], first["parameters"])
	}
	if len(params) != 1 {
		t.Fatalf("expected 1 parameter entry, got %d", len(params))
	}
	entry := params[0].(map[string]interface{})
	if entry["key"].(float64) != 19 || entry["value"].(float64) != 259200000 {
		t.Fatalf("expected {key:19,value:259200000}, got %v", entry)
	}
}

func TestProposalParametersArrayShape_PaginatedList(t *testing.T) {
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID: 1,
			Parameters: []tronapi.ProposalParameterEntry{
				{Key: 11, Value: 100},
				{Key: 9, Value: 200},
			},
			State: "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getpaginatedproposallist", `{"offset":0,"limit":10}`)
	proposals, ok := result["proposal"].([]interface{})
	if !ok || len(proposals) != 1 {
		t.Fatalf("expected proposal array of length 1, got %v", result["proposal"])
	}
	first := proposals[0].(map[string]interface{})
	params, ok := first["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array, got %T: %v", first["parameters"], first["parameters"])
	}
	if len(params) != 2 {
		t.Fatalf("expected 2 parameter entries, got %d: %v", len(params), params)
	}
	for _, e := range params {
		entry := e.(map[string]interface{})
		if _, ok := entry["key"]; !ok {
			t.Fatalf("parameter entry missing \"key\": %v", entry)
		}
		if _, ok := entry["value"]; !ok {
			t.Fatalf("parameter entry missing \"value\": %v", entry)
		}
	}
}

func TestProposalParametersArrayShape_EmptyMapStillArray(t *testing.T) {
	// A proposal with no parameters must still emit `"parameters": []`,
	// not `null` or a missing key — keep the field type stable for SDKs.
	stub := &stubBackend{
		proposals: []*tronapi.ProposalInfo{{
			ProposalID: 42,
			Parameters: []tronapi.ProposalParameterEntry{},
			State:      "PENDING",
		}},
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	result := postJSON(t, srv.URL+"/wallet/getproposalbyid", `{"id":42}`)
	params, ok := result["parameters"].([]interface{})
	if !ok {
		t.Fatalf("parameters must be a JSON array even when empty, got %T: %v",
			result["parameters"], result["parameters"])
	}
	if len(params) != 0 {
		t.Fatalf("expected empty array, got %v", params)
	}
}
func TestMetrics(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/wallet/metrics")
	if err != nil || resp.StatusCode != 200 {
		t.Fatalf("metrics failed: %v %v", err, resp)
	}
}

// --- Tests: M5.1 PR-7 Transaction meta ---

func TestValidateAddressValid(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/validateaddress",
		`{"address":"411234567890123456789012345678901234567890ab"}`)
	if result["message"] != "test" {
		t.Fatalf("unexpected message: %v", result)
	}
}
func TestGetTransactionReceiptById(t *testing.T) {
	stub := &stubBackend{}
	srv := newTestServer(t, stub)
	defer srv.Close()
	result := postJSON(t, srv.URL+"/wallet/gettransactionreceiptbyid",
		`{"value":"aabbcc"}`)
	// stub returns nil tx info → empty object
	_ = result
}

func TestGetTransactionByIdSurfacesBackendError(t *testing.T) {
	backendErr := errors.New("rawdb: block 1 decode: corrupt")
	srv := newTestServer(t, &stubBackend{txErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/gettransactionbyid", "application/json", strings.NewReader(`{"value":"aabbcc"}`))
	if err != nil {
		t.Fatalf("POST gettransactionbyid: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("gettransactionbyid status = %d, want 500", resp.StatusCode)
	}
}

func TestGetTransactionReceiptByIdSurfacesBackendError(t *testing.T) {
	backendErr := errors.New("rawdb: cold tx lookup corrupt")
	srv := newTestServer(t, &stubBackend{txInfoErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/gettransactionreceiptbyid", "application/json", strings.NewReader(`{"value":"aabbcc"}`))
	if err != nil {
		t.Fatalf("POST gettransactionreceiptbyid: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("gettransactionreceiptbyid status = %d, want 500", resp.StatusCode)
	}
}

func TestGetTransactionInfoByBlockNumSurfacesBackendError(t *testing.T) {
	backendErr := errors.New("rawdb: transaction info block 1 decode: corrupt")
	srv := newTestServer(t, &stubBackend{txInfoByBlockErr: backendErr})
	defer srv.Close()

	resp, err := http.Post(srv.URL+"/wallet/gettransactioninfobyblocknum", "application/json", strings.NewReader(`{"num":1}`))
	if err != nil {
		t.Fatalf("POST gettransactioninfobyblocknum: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("gettransactioninfobyblocknum status = %d, want 500", resp.StatusCode)
	}
}

// --- Tests: M9.7 broadcastTransaction synchronous actuator.Validate ---

// buildBroadcastEnvelope creates the JSON body for /wallet/broadcasttransaction.
// Uses a TransferContract so it matches a supported contract type.
func buildBroadcastEnvelope(t *testing.T) string {
	t.Helper()
	transfer := &contractpb.TransferContract{
		OwnerAddress: common.FromHex("410000000000000000000000000000000000000000"),
		ToAddress:    common.FromHex("410000000000000000000000000000000000000001"),
		Amount:       1000,
	}
	paramAny, err := anypb.New(transfer)
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	rawData := &corepb.TransactionRaw{
		Contract: []*corepb.Transaction_Contract{{
			Type:      corepb.Transaction_Contract_TransferContract,
			Parameter: paramAny,
		}},
		Expiration: 9999999999000,
		Timestamp:  1000000,
	}
	rawBytes, err := proto.Marshal(rawData)
	if err != nil {
		t.Fatalf("proto.Marshal TransactionRaw: %v", err)
	}
	h := sha256.Sum256(rawBytes)
	_ = h // txID used internally

	body, err := json.Marshal(map[string]any{
		"raw_data_hex": hex.EncodeToString(rawBytes),
		"signature":    []string{},
	})
	if err != nil {
		t.Fatalf("json.Marshal envelope: %v", err)
	}
	return string(body)
}

func TestBroadcastTransactionValidateError(t *testing.T) {
	stub := &stubBackend{
		validateErr: errors.New("owner account not found"),
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	envelope := buildBroadcastEnvelope(t)
	result := postJSON(t, srv.URL+"/wallet/broadcasttransaction", envelope)

	if result["result"] != false {
		t.Fatalf("expected result=false, got %v", result["result"])
	}
	if result["code"] != "CONTRACT_VALIDATE_ERROR" {
		t.Fatalf("expected code=CONTRACT_VALIDATE_ERROR, got %v", result["code"])
	}
	if result["message"] == "" {
		t.Fatalf("expected non-empty message hex, got empty")
	}
	// message must decode to the original error string
	msgHex, ok := result["message"].(string)
	if !ok {
		t.Fatalf("message is not a string: %T %v", result["message"], result["message"])
	}
	decoded, err := hex.DecodeString(msgHex)
	if err != nil {
		t.Fatalf("message is not valid hex: %v", err)
	}
	if string(decoded) != "owner account not found" {
		t.Fatalf("decoded message mismatch: got %q", string(decoded))
	}
}

func TestBroadcastTransactionValidateSuccess(t *testing.T) {
	stub := &stubBackend{
		validateErr: nil, // passes validation
	}
	srv := newTestServer(t, stub)
	defer srv.Close()

	envelope := buildBroadcastEnvelope(t)
	result := postJSON(t, srv.URL+"/wallet/broadcasttransaction", envelope)

	if result["result"] != true {
		t.Fatalf("expected result=true on success, got %v", result["result"])
	}
	if result["code"] != "SUCCESS" {
		t.Fatalf("expected code=SUCCESS, got %v", result["code"])
	}
}
