package grpcapi

import (
	"context"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SolidityServer implements the WalletSolidity gRPC service. Its block-returning
// methods are clamped to the latest solidified block; all state queries delegate
// to the same backend as the main Wallet service (state is monotonic).
// Shielded and unimplemented-in-wallet methods return codes.Unimplemented via the
// embedded stub.
type SolidityServer struct {
	apipb.UnimplementedWalletSolidityServer
	backend tronapi.Backend
}

// NewSolidityServer creates a WalletSolidity gRPC service adapter.
func NewSolidityServer(backend tronapi.Backend) *SolidityServer {
	return &SolidityServer{backend: backend}
}

// solidNum returns the latest solidified block number.
func (s *SolidityServer) solidNum() uint64 {
	return s.backend.SolidifiedBlockNum()
}

// ── Block queries (solid-bounded) ──────────────────────────────────────────────

func (s *SolidityServer) GetNowBlock(_ context.Context, _ *apipb.EmptyMessage) (*corepb.Block, error) {
	// solidNum()==0 on a fresh chain → looks up genesis block (#0), matching
	// java-tron's WalletSolidityApi which returns the solidified-DB head.
	block, err := s.backend.GetBlockByNumber(s.solidNum())
	if err != nil || block == nil {
		return nil, status.Error(codes.NotFound, "solid block not found")
	}
	return block.Proto(), nil
}

func (s *SolidityServer) GetNowBlock2(_ context.Context, _ *apipb.EmptyMessage) (*apipb.BlockExtention, error) {
	block, err := s.backend.GetBlockByNumber(s.solidNum())
	if err != nil || block == nil {
		return nil, status.Error(codes.NotFound, "solid block not found")
	}
	return blockToExtention(block), nil
}

func (s *SolidityServer) GetBlockByNum(_ context.Context, in *apipb.NumberMessage) (*corepb.Block, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	if uint64(in.Num) > s.solidNum() {
		return nil, status.Error(codes.NotFound, "block not yet solidified")
	}
	block, err := s.backend.GetBlockByNumber(uint64(in.Num))
	if err != nil || block == nil {
		return nil, status.Error(codes.NotFound, "block not found")
	}
	return block.Proto(), nil
}

func (s *SolidityServer) GetBlockByNum2(_ context.Context, in *apipb.NumberMessage) (*apipb.BlockExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	if uint64(in.Num) > s.solidNum() {
		return nil, status.Error(codes.NotFound, "block not yet solidified")
	}
	block, err := s.backend.GetBlockByNumber(uint64(in.Num))
	if err != nil || block == nil {
		return nil, status.Error(codes.NotFound, "block not found")
	}
	return blockToExtention(block), nil
}

func (s *SolidityServer) GetTransactionInfoByBlockNum(_ context.Context, in *apipb.NumberMessage) (*apipb.TransactionInfoList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	if uint64(in.Num) > s.solidNum() {
		return &apipb.TransactionInfoList{}, nil
	}
	infos, err := s.backend.GetTransactionInfoByBlockNum(uint64(in.Num))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.TransactionInfoList{TransactionInfo: infos}, nil
}

func (s *SolidityServer) GetTransactionCountByBlockNum(_ context.Context, in *apipb.NumberMessage) (*apipb.NumberMessage, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	block, err := s.backend.GetBlockByNumber(uint64(in.Num))
	if err != nil || block == nil {
		return nil, status.Error(codes.NotFound, "block not found")
	}
	return &apipb.NumberMessage{Num: int64(len(block.Transactions()))}, nil
}

// ── Account queries (same as Wallet) ──────────────────────────────────────────

func (s *SolidityServer) GetAccount(_ context.Context, in *corepb.Account) (*corepb.Account, error) {
	if in == nil || len(in.Address) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Address)
	acc, err := s.backend.GetAccount(addr)
	if err != nil || acc == nil {
		return &corepb.Account{}, nil
	}
	return acc.Proto(), nil
}

func (s *SolidityServer) GetAccountById(_ context.Context, in *corepb.Account) (*corepb.Account, error) {
	if in == nil {
		return &corepb.Account{}, nil
	}
	if len(in.Address) > 0 {
		addr := common.BytesToAddress(in.Address)
		acc, err := s.backend.GetAccount(addr)
		if err != nil || acc == nil {
			return &corepb.Account{}, nil
		}
		return acc.Proto(), nil
	}
	return &corepb.Account{}, nil
}

// ── Witness / asset queries ────────────────────────────────────────────────────

func (s *SolidityServer) ListWitnesses(_ context.Context, _ *apipb.EmptyMessage) (*apipb.WitnessList, error) {
	witnesses, err := s.backend.ListWitnesses()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	result := make([]*corepb.Witness, len(witnesses))
	for i, w := range witnesses {
		result[i] = &corepb.Witness{
			Address:   common.FromHex(w.Address),
			VoteCount: w.VoteCount,
			Url:       w.URL,
			IsJobs:    w.IsJobs,
		}
	}
	return &apipb.WitnessList{Witnesses: result}, nil
}

func (s *SolidityServer) GetAssetIssueList(_ context.Context, _ *apipb.EmptyMessage) (*apipb.AssetIssueList, error) {
	return &apipb.AssetIssueList{AssetIssue: s.backend.GetAssetIssueList()}, nil
}

func (s *SolidityServer) GetPaginatedAssetIssueList(_ context.Context, in *apipb.PaginatedMessage) (*apipb.AssetIssueList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	assets := s.backend.GetAssetIssueListPaginated(int(in.Offset), int(in.Limit))
	return &apipb.AssetIssueList{AssetIssue: assets}, nil
}

func (s *SolidityServer) GetAssetIssueByName(_ context.Context, in *apipb.BytesMessage) (*contractpb.AssetIssueContract, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "asset name required")
	}
	ac := s.backend.GetAssetIssueByName(in.Value)
	if ac == nil {
		return nil, status.Error(codes.NotFound, "asset not found")
	}
	return ac, nil
}

func (s *SolidityServer) GetAssetIssueById(_ context.Context, in *apipb.BytesMessage) (*contractpb.AssetIssueContract, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "asset id required")
	}
	var id int64
	for _, b := range in.Value {
		if b < '0' || b > '9' {
			return nil, status.Error(codes.InvalidArgument, "asset id must be numeric")
		}
		id = id*10 + int64(b-'0')
	}
	ac := s.backend.GetAssetIssueByID(id)
	if ac == nil {
		return nil, status.Error(codes.NotFound, "asset not found")
	}
	return ac, nil
}

// ── Delegation queries ─────────────────────────────────────────────────────────

func (s *SolidityServer) GetDelegatedResourceV2(_ context.Context, in *apipb.DelegatedResourceMessage) (*apipb.DelegatedResourceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	from := common.BytesToAddress(in.FromAddress)
	to := common.BytesToAddress(in.ToAddress)
	infos, err := s.backend.GetDelegatedResourceV2(from, to)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if len(infos) == 0 {
		return &apipb.DelegatedResourceList{}, nil
	}
	resources := make([]*corepb.DelegatedResource, 0, len(infos))
	for range infos {
		resources = append(resources, &corepb.DelegatedResource{
			From: in.FromAddress,
			To:   in.ToAddress,
		})
	}
	for i, info := range infos {
		resources[i].FrozenBalanceForBandwidth = info.FrozenBalanceForBandwidth
		resources[i].FrozenBalanceForEnergy = info.FrozenBalanceForEnergy
		resources[i].ExpireTimeForBandwidth = info.ExpireTimeForBandwidth
		resources[i].ExpireTimeForEnergy = info.ExpireTimeForEnergy
	}
	return &apipb.DelegatedResourceList{
		DelegatedResource: resources,
	}, nil
}

func (s *SolidityServer) GetDelegatedResourceAccountIndexV2(_ context.Context, in *apipb.BytesMessage) (*corepb.DelegatedResourceAccountIndex, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	idx, err := s.backend.GetDelegatedResourceAccountIndexV2(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if idx == nil {
		return &corepb.DelegatedResourceAccountIndex{Account: in.Value}, nil
	}
	toAccounts := make([][]byte, len(idx.ToAddresses))
	for i, a := range idx.ToAddresses {
		toAccounts[i] = common.FromHex(a)
	}
	return &corepb.DelegatedResourceAccountIndex{Account: in.Value, ToAccounts: toAccounts}, nil
}

func (s *SolidityServer) GetCanDelegatedMaxSize(_ context.Context, in *apipb.CanDelegatedMaxSizeRequestMessage) (*apipb.CanDelegatedMaxSizeResponseMessage, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "owner address required")
	}
	addr := common.BytesToAddress(in.OwnerAddress)
	info, err := s.backend.CanDelegateResource(addr, 0, corepb.ResourceCode(in.Type))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if info == nil {
		return &apipb.CanDelegatedMaxSizeResponseMessage{}, nil
	}
	return &apipb.CanDelegatedMaxSizeResponseMessage{MaxSize: info.CanDelegateSize}, nil
}

func (s *SolidityServer) GetAvailableUnfreezeCount(_ context.Context, in *apipb.GetAvailableUnfreezeCountRequestMessage) (*apipb.GetAvailableUnfreezeCountResponseMessage, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "owner address required")
	}
	addr := common.BytesToAddress(in.OwnerAddress)
	info, err := s.backend.GetAvailableUnfreezeCount(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if info == nil {
		return &apipb.GetAvailableUnfreezeCountResponseMessage{}, nil
	}
	return &apipb.GetAvailableUnfreezeCountResponseMessage{Count: info.Count}, nil
}

func (s *SolidityServer) GetCanWithdrawUnfreezeAmount(_ context.Context, in *apipb.CanWithdrawUnfreezeAmountRequestMessage) (*apipb.CanWithdrawUnfreezeAmountResponseMessage, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "owner address required")
	}
	addr := common.BytesToAddress(in.OwnerAddress)
	info, err := s.backend.GetCanWithdrawUnfreezeAmount(addr, in.Timestamp)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if info == nil {
		return &apipb.CanWithdrawUnfreezeAmountResponseMessage{}, nil
	}
	return &apipb.CanWithdrawUnfreezeAmountResponseMessage{Amount: info.Amount}, nil
}

// ── Exchange queries ───────────────────────────────────────────────────────────

func (s *SolidityServer) ListExchanges(_ context.Context, _ *apipb.EmptyMessage) (*apipb.ExchangeList, error) {
	exchanges, err := s.backend.ListExchanges()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.ExchangeList{Exchanges: exchanges}, nil
}

// ── Transaction queries ────────────────────────────────────────────────────────

func (s *SolidityServer) GetTransactionById(_ context.Context, in *apipb.BytesMessage) (*corepb.Transaction, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tx hash required")
	}
	hash := common.BytesToHash(in.Value)
	tx, err := s.backend.GetTransactionByID(hash)
	if err != nil || tx == nil {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}
	return tx, nil
}

func (s *SolidityServer) GetTransactionInfoById(_ context.Context, in *apipb.BytesMessage) (*corepb.TransactionInfo, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tx hash required")
	}
	hash := common.BytesToHash(in.Value)
	info, err := s.backend.GetTransactionInfoByID(hash)
	if err != nil || info == nil {
		return nil, status.Error(codes.NotFound, "transaction info not found")
	}
	return info, nil
}

// ── Reward / brokerage ────────────────────────────────────────────────────────

func (s *SolidityServer) GetRewardInfo(_ context.Context, in *apipb.BytesMessage) (*apipb.NumberMessage, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	info, err := s.backend.GetReward(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if info == nil {
		return &apipb.NumberMessage{Num: 0}, nil
	}
	return &apipb.NumberMessage{Num: info.Reward}, nil
}

func (s *SolidityServer) GetBrokerageInfo(_ context.Context, in *apipb.BytesMessage) (*apipb.NumberMessage, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	return &apipb.NumberMessage{Num: s.backend.GetBrokerageInfo(addr)}, nil
}

// ── Contract execution ─────────────────────────────────────────────────────────

func (s *SolidityServer) TriggerConstantContract(_ context.Context, in *contractpb.TriggerSmartContract) (*apipb.TransactionExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	contract := common.BytesToAddress(in.ContractAddress)
	result, err := s.backend.TriggerConstantContract(owner, contract, in.Data, 30_000_000)
	ext := &apipb.TransactionExtention{
		Result: &apipb.Return{Result: err == nil},
	}
	if result != nil {
		ext.ConstantResult = [][]byte{result.Result}
		ext.EnergyUsed = result.EnergyUsed
	}
	if err != nil {
		ext.Result.Message = []byte(err.Error())
	}
	return ext, nil
}

func (s *SolidityServer) EstimateEnergy(_ context.Context, in *contractpb.TriggerSmartContract) (*apipb.EstimateEnergyMessage, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	contract := common.BytesToAddress(in.ContractAddress)
	energy, err := s.backend.EstimateEnergy(owner, contract, in.Data)
	if err != nil {
		return &apipb.EstimateEnergyMessage{
			Result: &apipb.Return{Result: false, Message: []byte(err.Error())},
		}, nil
	}
	return &apipb.EstimateEnergyMessage{
		Result:         &apipb.Return{Result: true},
		EnergyRequired: energy,
	}, nil
}

// ── Market queries ─────────────────────────────────────────────────────────────

func (s *SolidityServer) GetMarketOrderById(_ context.Context, in *apipb.BytesMessage) (*corepb.MarketOrder, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "order id required")
	}
	order := s.backend.GetMarketOrderByID(in.Value)
	if order == nil {
		return nil, status.Error(codes.NotFound, "order not found")
	}
	return order, nil
}

func (s *SolidityServer) GetMarketOrderByAccount(_ context.Context, in *apipb.BytesMessage) (*corepb.MarketOrderList, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	orders := s.backend.GetMarketOrdersByAccount(addr)
	return &corepb.MarketOrderList{Orders: orders}, nil
}

func (s *SolidityServer) GetMarketPriceByPair(_ context.Context, in *corepb.MarketOrderPair) (*corepb.MarketPriceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	pl := s.backend.GetMarketPriceByPair(in.SellTokenId, in.BuyTokenId)
	if pl == nil {
		return &corepb.MarketPriceList{}, nil
	}
	return pl, nil
}

// ── Price history ──────────────────────────────────────────────────────────────

func (s *SolidityServer) GetBandwidthPrices(_ context.Context, _ *apipb.EmptyMessage) (*apipb.PricesResponseMessage, error) {
	return &apipb.PricesResponseMessage{Prices: s.backend.GetBandwidthPrices()}, nil
}

func (s *SolidityServer) GetEnergyPrices(_ context.Context, _ *apipb.EmptyMessage) (*apipb.PricesResponseMessage, error) {
	return &apipb.PricesResponseMessage{Prices: s.backend.GetEnergyPrices()}, nil
}

func (s *SolidityServer) GetBurnTrx(_ context.Context, _ *apipb.EmptyMessage) (*apipb.NumberMessage, error) {
	return &apipb.NumberMessage{Num: s.backend.GetBurnTrx()}, nil
}
