package grpcapi

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// SolidityServer implements the WalletSolidity gRPC service. Block and state
// reads are clamped to the latest solidified block so they share the same
// archive/as-of path as the HTTP /walletsolidity endpoints.
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

func (s *SolidityServer) GetBlock(_ context.Context, in *apipb.BlockReq) (*apipb.BlockExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	idOrNum := strings.TrimSpace(in.GetIdOrNum())
	if idOrNum == "" || strings.EqualFold(idOrNum, "latest") {
		return s.getSolidBlockByNumber(s.solidNum(), in.GetDetail())
	}
	if strings.EqualFold(idOrNum, "earliest") {
		return s.getSolidBlockByNumber(0, in.GetDetail())
	}

	if hash, hashBytes, ok, err := parseGRPCBlockID(idOrNum); ok || err != nil {
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid block id")
		}
		num, ok := blockNumberFromGRPCBlockIDBytes(hashBytes)
		if !ok {
			return nil, status.Error(codes.InvalidArgument, "invalid block id")
		}
		if num > s.solidNum() {
			return nil, status.Error(codes.NotFound, "block not yet solidified")
		}
		block, err := s.backend.GetBlockByHash(hash)
		if err != nil {
			if blockLookupNotFound(err) {
				return nil, status.Error(codes.NotFound, "block not found")
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		if block == nil {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		if block.Number() > s.solidNum() {
			return nil, status.Error(codes.NotFound, "block not yet solidified")
		}
		return blockToExtentionWithDetail(block, in.GetDetail()), nil
	}

	num, err := parseGRPCBlockNumber(idOrNum)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid block number")
	}
	return s.getSolidBlockByNumber(num, in.GetDetail())
}

func (s *SolidityServer) getSolidBlockByNumber(num uint64, detail bool) (*apipb.BlockExtention, error) {
	if num > s.solidNum() {
		return nil, status.Error(codes.NotFound, "block not yet solidified")
	}
	block, err := s.backend.GetBlockByNumber(num)
	if err != nil {
		if blockLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if block == nil {
		return nil, status.Error(codes.NotFound, "block not found")
	}
	return blockToExtentionWithDetail(block, detail), nil
}

func parseGRPCBlockID(value string) (common.Hash, []byte, bool, error) {
	raw := value
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		raw = raw[2:]
	}
	if len(raw) != common.HashLength*2 {
		return common.Hash{}, nil, false, nil
	}
	hashBytes, err := hex.DecodeString(raw)
	if err != nil {
		return common.Hash{}, nil, true, err
	}
	return common.BytesToHash(hashBytes), hashBytes, true, nil
}

func parseGRPCBlockNumber(value string) (uint64, error) {
	raw := value
	base := 10
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		raw = raw[2:]
		base = 16
	}
	if raw == "" {
		return 0, strconv.ErrSyntax
	}
	return strconv.ParseUint(raw, base, 64)
}

func blockNumberFromGRPCBlockIDBytes(hashBytes []byte) (uint64, bool) {
	if len(hashBytes) < 8 {
		return 0, false
	}
	return binary.BigEndian.Uint64(hashBytes[:8]), true
}

func (s *SolidityServer) GetNowBlock(_ context.Context, _ *apipb.EmptyMessage) (*corepb.Block, error) {
	// solidNum()==0 on a fresh chain → looks up genesis block (#0), matching
	// java-tron's WalletSolidityApi which returns the solidified-DB head.
	block, err := s.backend.GetBlockByNumber(s.solidNum())
	if err != nil {
		if blockLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "solid block not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if block == nil {
		return nil, status.Error(codes.NotFound, "solid block not found")
	}
	return block.Proto(), nil
}

func (s *SolidityServer) GetNowBlock2(_ context.Context, _ *apipb.EmptyMessage) (*apipb.BlockExtention, error) {
	block, err := s.backend.GetBlockByNumber(s.solidNum())
	if err != nil {
		if blockLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "solid block not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if block == nil {
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
	if err != nil {
		if blockLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if block == nil {
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
	if err != nil {
		if blockLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if block == nil {
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
	if uint64(in.Num) > s.solidNum() {
		return nil, status.Error(codes.NotFound, "block not yet solidified")
	}
	block, err := s.backend.GetBlockByNumber(uint64(in.Num))
	if err != nil {
		if blockLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if block == nil {
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
	acc, err := s.backend.GetAccountAt(addr, s.solidNum())
	if err != nil {
		if accountLookupNotFound(err) {
			return &corepb.Account{}, nil
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if acc == nil {
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
		acc, err := s.backend.GetAccountAt(addr, s.solidNum())
		if err != nil {
			if accountLookupNotFound(err) {
				return &corepb.Account{}, nil
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		if acc == nil {
			return &corepb.Account{}, nil
		}
		return acc.Proto(), nil
	}
	if len(in.AccountId) > 0 {
		acc, err := s.backend.GetAccountByIdAt(in.AccountId, s.solidNum())
		if err != nil {
			if accountLookupNotFound(err) {
				return &corepb.Account{}, nil
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		if acc == nil {
			return &corepb.Account{}, nil
		}
		return acc.Proto(), nil
	}
	return &corepb.Account{}, nil
}

// ── Witness / asset queries ────────────────────────────────────────────────────

func (s *SolidityServer) ListWitnesses(_ context.Context, _ *apipb.EmptyMessage) (*apipb.WitnessList, error) {
	witnesses, err := s.backend.ListWitnessesAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return witnessListFromInfos(witnesses), nil
}

func (s *SolidityServer) GetPaginatedNowWitnessList(_ context.Context, in *apipb.PaginatedMessage) (*apipb.WitnessList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	witnesses, err := s.backend.ListWitnessesAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	page, err := paginateWitnessInfos(witnesses, in.Offset, in.Limit)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return witnessListFromInfos(page), nil
}

func (s *SolidityServer) GetAssetIssueList(_ context.Context, _ *apipb.EmptyMessage) (*apipb.AssetIssueList, error) {
	assets, err := s.backend.GetAssetIssueListAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.AssetIssueList{AssetIssue: assets}, nil
}

func (s *SolidityServer) GetPaginatedAssetIssueList(_ context.Context, in *apipb.PaginatedMessage) (*apipb.AssetIssueList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	assets, err := s.backend.GetAssetIssueListPaginatedAt(int(in.Offset), int(in.Limit), s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.AssetIssueList{AssetIssue: assets}, nil
}

func (s *SolidityServer) GetAssetIssueByName(_ context.Context, in *apipb.BytesMessage) (*contractpb.AssetIssueContract, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "asset name required")
	}
	ac, err := s.backend.GetAssetIssueByNameAt(in.Value, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if ac == nil {
		return nil, status.Error(codes.NotFound, "asset not found")
	}
	return ac, nil
}

func (s *SolidityServer) GetAssetIssueListByName(_ context.Context, in *apipb.BytesMessage) (*apipb.AssetIssueList, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "asset name required")
	}
	ac, err := s.backend.GetAssetIssueByNameAt(in.Value, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if ac == nil {
		return &apipb.AssetIssueList{}, nil
	}
	return &apipb.AssetIssueList{AssetIssue: []*contractpb.AssetIssueContract{ac}}, nil
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
	ac, err := s.backend.GetAssetIssueByIDAt(id, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if ac == nil {
		return nil, status.Error(codes.NotFound, "asset not found")
	}
	return ac, nil
}

// ── Delegation queries ─────────────────────────────────────────────────────────

func (s *SolidityServer) GetDelegatedResource(_ context.Context, in *apipb.DelegatedResourceMessage) (*apipb.DelegatedResourceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	from := common.BytesToAddress(in.FromAddress)
	to := common.BytesToAddress(in.ToAddress)
	infos, err := s.backend.GetDelegatedResourceAt(from, to, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.DelegatedResourceList{
		DelegatedResource: delegatedResourcesFromInfos(in.FromAddress, in.ToAddress, infos),
	}, nil
}

func (s *SolidityServer) GetDelegatedResourceV2(_ context.Context, in *apipb.DelegatedResourceMessage) (*apipb.DelegatedResourceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	from := common.BytesToAddress(in.FromAddress)
	to := common.BytesToAddress(in.ToAddress)
	infos, err := s.backend.GetDelegatedResourceV2At(from, to, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.DelegatedResourceList{
		DelegatedResource: delegatedResourcesFromInfos(in.FromAddress, in.ToAddress, infos),
	}, nil
}

func (s *SolidityServer) GetDelegatedResourceAccountIndex(_ context.Context, in *apipb.BytesMessage) (*corepb.DelegatedResourceAccountIndex, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	idx, err := s.backend.GetDelegatedResourceAccountIndexAt(addr, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if idx == nil {
		return &corepb.DelegatedResourceAccountIndex{Account: in.Value}, nil
	}
	return idx, nil
}

func (s *SolidityServer) GetDelegatedResourceAccountIndexV2(_ context.Context, in *apipb.BytesMessage) (*corepb.DelegatedResourceAccountIndex, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	idx, err := s.backend.GetDelegatedResourceAccountIndexV2At(addr, s.solidNum())
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
	info, err := s.backend.CanDelegateResourceAt(addr, 0, corepb.ResourceCode(in.Type), s.solidNum())
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
	info, err := s.backend.GetAvailableUnfreezeCountAt(addr, s.solidNum())
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
	info, err := s.backend.GetCanWithdrawUnfreezeAmountAt(addr, in.Timestamp, s.solidNum())
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
	exchanges, err := s.backend.ListExchangesAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.ExchangeList{Exchanges: exchanges}, nil
}

func (s *SolidityServer) GetExchangeById(_ context.Context, in *apipb.BytesMessage) (*corepb.Exchange, error) {
	id, err := parseExchangeIDMessage(in)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	exchange, err := s.backend.GetExchangeByIDAt(id, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if exchange == nil {
		return nil, status.Error(codes.NotFound, "exchange not found")
	}
	return exchange, nil
}

// ── Transaction queries ────────────────────────────────────────────────────────

func (s *SolidityServer) GetTransactionById(_ context.Context, in *apipb.BytesMessage) (*corepb.Transaction, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tx hash required")
	}
	hash := common.BytesToHash(in.Value)
	if ok, err := s.transactionWithinSolid(hash); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	} else if !ok {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}
	tx, err := s.backend.GetTransactionByID(hash)
	if err != nil {
		if transactionLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "transaction not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if tx == nil {
		return nil, status.Error(codes.NotFound, "transaction not found")
	}
	return tx, nil
}

func (s *SolidityServer) GetTransactionInfoById(_ context.Context, in *apipb.BytesMessage) (*corepb.TransactionInfo, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "tx hash required")
	}
	hash := common.BytesToHash(in.Value)
	if ok, err := s.transactionWithinSolid(hash); err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	} else if !ok {
		return nil, status.Error(codes.NotFound, "transaction info not found")
	}
	info, err := s.backend.GetTransactionInfoByID(hash)
	if err != nil {
		if transactionLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "transaction info not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if info == nil {
		return nil, status.Error(codes.NotFound, "transaction info not found")
	}
	return info, nil
}

func (s *SolidityServer) transactionWithinSolid(hash common.Hash) (bool, error) {
	blockNum, ok, err := s.backend.GetTransactionBlockNumByID(hash)
	if err != nil {
		return false, err
	}
	if !ok {
		return false, nil
	}
	return blockNum <= s.solidNum(), nil
}

// ── Reward / brokerage ────────────────────────────────────────────────────────

func (s *SolidityServer) GetRewardInfo(_ context.Context, in *apipb.BytesMessage) (*apipb.NumberMessage, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	info, err := s.backend.GetRewardAt(addr, s.solidNum())
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
	rate, err := s.backend.GetBrokerageInfoAt(addr, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.NumberMessage{Num: rate}, nil
}

// ── Contract execution ─────────────────────────────────────────────────────────

func (s *SolidityServer) TriggerConstantContract(_ context.Context, in *contractpb.TriggerSmartContract) (*apipb.TransactionExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	contract := common.BytesToAddress(in.ContractAddress)
	result, err := s.backend.TriggerConstantContractAt(owner, contract, in.Data, 30_000_000, s.solidNum())
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
	energy, err := s.backend.EstimateEnergyAt(owner, contract, in.Data, s.solidNum())
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
	order, err := s.backend.GetMarketOrderByIDAt(in.Value, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
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
	orders, err := s.backend.GetMarketOrdersByAccountAt(addr, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &corepb.MarketOrderList{Orders: orders}, nil
}

func (s *SolidityServer) GetMarketPriceByPair(_ context.Context, in *corepb.MarketOrderPair) (*corepb.MarketPriceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	pl, err := s.backend.GetMarketPriceByPairAt(in.SellTokenId, in.BuyTokenId, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if pl == nil {
		return &corepb.MarketPriceList{}, nil
	}
	return pl, nil
}

func (s *SolidityServer) GetMarketOrderListByPair(_ context.Context, in *corepb.MarketOrderPair) (*corepb.MarketOrderList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	orders, err := s.backend.GetMarketOrderListByPairAt(in.SellTokenId, in.BuyTokenId, s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &corepb.MarketOrderList{Orders: orders}, nil
}

func (s *SolidityServer) GetMarketPairList(_ context.Context, _ *apipb.EmptyMessage) (*corepb.MarketOrderPairList, error) {
	pairs, err := s.backend.GetMarketPairListAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if pairs == nil {
		return &corepb.MarketOrderPairList{}, nil
	}
	return pairs, nil
}

// ── Price history ──────────────────────────────────────────────────────────────

func (s *SolidityServer) GetBandwidthPrices(_ context.Context, _ *apipb.EmptyMessage) (*apipb.PricesResponseMessage, error) {
	prices, err := s.backend.GetBandwidthPricesAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.PricesResponseMessage{Prices: prices}, nil
}

func (s *SolidityServer) GetEnergyPrices(_ context.Context, _ *apipb.EmptyMessage) (*apipb.PricesResponseMessage, error) {
	prices, err := s.backend.GetEnergyPricesAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.PricesResponseMessage{Prices: prices}, nil
}

func (s *SolidityServer) GetBurnTrx(_ context.Context, _ *apipb.EmptyMessage) (*apipb.NumberMessage, error) {
	burned, err := s.backend.GetBurnTrxAt(s.solidNum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.NumberMessage{Num: burned}, nil
}
