package grpcapi

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

var log = gtronlog.NewModule("grpcapi")

// Server implements the gRPC Wallet service as a thin adapter over tronapi.Backend.
// Unimplemented methods return codes.Unimplemented via the embedded stub.
// Server implements node.Lifecycle: Start() binds the configured addr; Stop() is idempotent.
type Server struct {
	apipb.UnimplementedWalletServer
	backend tronapi.Backend
	addr    string
	grpc    *grpc.Server
}

// NewServer creates a Server that will listen on addr when Start is called.
func NewServer(backend tronapi.Backend, addr string) *Server {
	return &Server{backend: backend, addr: addr}
}

// Start binds the listener and begins serving.
func (s *Server) Start() error {
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("grpc listen %s: %w", s.addr, err)
	}
	s.grpc = grpc.NewServer()
	apipb.RegisterWalletServer(s.grpc, s)
	apipb.RegisterWalletSolidityServer(s.grpc, NewSolidityServer(s.backend))
	go func() {
		if err := s.grpc.Serve(ln); err != nil {
			log.Warn("gRPC server stopped", "err", err)
		}
	}()
	log.Info("gRPC listening", "addr", s.addr)
	return nil
}

// Stop gracefully shuts down the gRPC server.
func (s *Server) Stop() error {
	if s.grpc != nil {
		s.grpc.GracefulStop()
	}
	return nil
}

// GetNowBlock returns the current best block.
func (s *Server) GetNowBlock(_ context.Context, _ *apipb.EmptyMessage) (*corepb.Block, error) {
	block := s.backend.CurrentBlock()
	if block == nil {
		return nil, status.Error(codes.NotFound, "no current block")
	}
	return block.Proto(), nil
}

// GetBlockByNum returns the block at the given number.
func (s *Server) GetBlockByNum(_ context.Context, in *apipb.NumberMessage) (*corepb.Block, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
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

// GetAccount returns the account state for the address carried in in.Address.
func (s *Server) GetAccount(_ context.Context, in *corepb.Account) (*corepb.Account, error) {
	if in == nil || len(in.Address) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Address)
	acc, err := s.backend.GetAccount(addr)
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

func (s *Server) GetAccountBalance(_ context.Context, in *contractpb.AccountBalanceRequest) (*contractpb.AccountBalanceResponse, error) {
	resp, err := s.backend.GetAccountBalanceTrace(in)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return resp, nil
}

func (s *Server) GetBlockBalanceTrace(_ context.Context, in *contractpb.BlockBalanceTrace_BlockIdentifier) (*contractpb.BlockBalanceTrace, error) {
	trace, err := s.backend.GetBlockBalanceTrace(in)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return trace, nil
}

// GetTransactionById returns the transaction with the given 32-byte hash.
func (s *Server) GetTransactionById(_ context.Context, in *apipb.BytesMessage) (*corepb.Transaction, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "transaction hash required")
	}
	txHash := common.BytesToHash(in.Value)
	tx, err := s.backend.GetTransactionByID(txHash)
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

// GetChainParameters returns the current chain governance parameters.
func (s *Server) GetChainParameters(_ context.Context, _ *apipb.EmptyMessage) (*corepb.ChainParameters, error) {
	params, err := s.backend.GetChainParameters()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	cp := make([]*corepb.ChainParameters_ChainParameter, len(params))
	for i, p := range params {
		cp[i] = &corepb.ChainParameters_ChainParameter{
			Key:   p.Key,
			Value: p.Value,
		}
	}
	return &corepb.ChainParameters{ChainParameter: cp}, nil
}

// blockToExtention converts a types.Block to a BlockExtention.
func blockToExtention(block *types.Block) *apipb.BlockExtention {
	return blockToExtentionWithDetail(block, true)
}

func blockToExtentionWithDetail(block *types.Block, detail bool) *apipb.BlockExtention {
	id := block.ID()
	txs := block.Transactions()
	txExts := make([]*apipb.TransactionExtention, len(txs))
	for i, tx := range txs {
		txExts[i] = &apipb.TransactionExtention{
			Txid: tx.Hash().Bytes(),
		}
		if detail {
			txExts[i].Transaction = tx.Proto()
		}
	}
	return &apipb.BlockExtention{
		Transactions: txExts,
		BlockHeader:  block.Proto().BlockHeader,
		Blockid:      id.Hash[:],
	}
}

// GetNowBlock2 returns the current best block as a BlockExtention.
func (s *Server) GetNowBlock2(_ context.Context, _ *apipb.EmptyMessage) (*apipb.BlockExtention, error) {
	block := s.backend.CurrentBlock()
	if block == nil {
		return nil, status.Error(codes.NotFound, "no current block")
	}
	return blockToExtention(block), nil
}

// GetBlockByNum2 returns the block at the given number as a BlockExtention.
func (s *Server) GetBlockByNum2(_ context.Context, in *apipb.NumberMessage) (*apipb.BlockExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
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

// GetBlock returns a block extension by latest/head, earliest/genesis, number, or BlockID.
func (s *Server) GetBlock(_ context.Context, in *apipb.BlockReq) (*apipb.BlockExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	idOrNum := strings.TrimSpace(in.GetIdOrNum())
	if idOrNum == "" || strings.EqualFold(idOrNum, "latest") {
		block := s.backend.CurrentBlock()
		if block == nil {
			return nil, status.Error(codes.NotFound, "no current block")
		}
		return blockToExtentionWithDetail(block, in.GetDetail()), nil
	}
	if strings.EqualFold(idOrNum, "earliest") {
		block, err := s.backend.GetBlockByNumber(0)
		if err != nil {
			if blockLookupNotFound(err) {
				return nil, status.Error(codes.NotFound, "block not found")
			}
			return nil, status.Error(codes.Internal, err.Error())
		}
		if block == nil {
			return nil, status.Error(codes.NotFound, "block not found")
		}
		return blockToExtentionWithDetail(block, in.GetDetail()), nil
	}

	if hash, _, ok, err := parseGRPCBlockID(idOrNum); ok || err != nil {
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "invalid block id")
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
		return blockToExtentionWithDetail(block, in.GetDetail()), nil
	}

	num, err := parseGRPCBlockNumber(idOrNum)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "invalid block number")
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
	return blockToExtentionWithDetail(block, in.GetDetail()), nil
}

// GetBlockById returns the block matching the given block ID (hash bytes).
func (s *Server) GetBlockById(_ context.Context, in *apipb.BytesMessage) (*corepb.Block, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "block id required")
	}
	hash := common.BytesToHash(in.Value)
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
	return block.Proto(), nil
}

// GetBlockByLimitNext returns all blocks in [startNum, endNum).
func (s *Server) GetBlockByLimitNext(_ context.Context, in *apipb.BlockLimit) (*apipb.BlockList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	start, end, err := validateGRPCBlockLimit(in)
	if err != nil {
		return nil, err
	}
	blocks, err := s.backend.GetBlocksByRange(start, end)
	if err != nil {
		return &apipb.BlockList{}, nil
	}
	result := make([]*corepb.Block, len(blocks))
	for i, b := range blocks {
		result[i] = b.Proto()
	}
	return &apipb.BlockList{Block: result}, nil
}

// GetBlockByLimitNext2 returns blocks in [startNum, endNum) as BlockExtention list.
func (s *Server) GetBlockByLimitNext2(_ context.Context, in *apipb.BlockLimit) (*apipb.BlockListExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	start, end, err := validateGRPCBlockLimit(in)
	if err != nil {
		return nil, err
	}
	blocks, err := s.backend.GetBlocksByRange(start, end)
	if err != nil {
		return &apipb.BlockListExtention{}, nil
	}
	result := make([]*apipb.BlockExtention, len(blocks))
	for i, b := range blocks {
		result[i] = blockToExtention(b)
	}
	return &apipb.BlockListExtention{Block: result}, nil
}

func validateGRPCBlockLimit(in *apipb.BlockLimit) (uint64, uint64, error) {
	if in.StartNum < 0 || in.EndNum < 0 || in.EndNum <= in.StartNum {
		return 0, 0, status.Error(codes.InvalidArgument, "invalid block range")
	}
	return uint64(in.StartNum), uint64(in.EndNum), nil
}

// GetBlockByLatestNum returns the latest N blocks.
func (s *Server) GetBlockByLatestNum(_ context.Context, in *apipb.NumberMessage) (*apipb.BlockList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	n := in.Num
	if n <= 0 {
		return &apipb.BlockList{}, nil
	}
	current := s.backend.CurrentBlock()
	if current == nil {
		return &apipb.BlockList{}, nil
	}
	end := current.Number() + 1
	start := uint64(0)
	if end > uint64(n) {
		start = end - uint64(n)
	}
	blocks, err := s.backend.GetBlocksByRange(start, end)
	if err != nil {
		return &apipb.BlockList{}, nil
	}
	result := make([]*corepb.Block, len(blocks))
	for i, b := range blocks {
		result[i] = b.Proto()
	}
	return &apipb.BlockList{Block: result}, nil
}

// GetBlockByLatestNum2 returns the latest N blocks as BlockExtention list.
func (s *Server) GetBlockByLatestNum2(_ context.Context, in *apipb.NumberMessage) (*apipb.BlockListExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	n := in.Num
	if n <= 0 {
		return &apipb.BlockListExtention{}, nil
	}
	current := s.backend.CurrentBlock()
	if current == nil {
		return &apipb.BlockListExtention{}, nil
	}
	end := current.Number() + 1
	start := uint64(0)
	if end > uint64(n) {
		start = end - uint64(n)
	}
	blocks, err := s.backend.GetBlocksByRange(start, end)
	if err != nil {
		return &apipb.BlockListExtention{}, nil
	}
	result := make([]*apipb.BlockExtention, len(blocks))
	for i, b := range blocks {
		result[i] = blockToExtention(b)
	}
	return &apipb.BlockListExtention{Block: result}, nil
}

// GetTransactionCountByBlockNum returns the number of transactions in the block at num.
func (s *Server) GetTransactionCountByBlockNum(_ context.Context, in *apipb.NumberMessage) (*apipb.NumberMessage, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
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

// GetAccountById returns the account identified by its account ID field.
// The no-index fallback returns an empty account when the ID is unknown.
func (s *Server) GetAccountById(_ context.Context, in *corepb.Account) (*corepb.Account, error) {
	if in == nil {
		return &corepb.Account{}, nil
	}
	// If address bytes are provided use them for the lookup.
	if len(in.Address) > 0 {
		addr := common.BytesToAddress(in.Address)
		acc, err := s.backend.GetAccount(addr)
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
		acc, err := s.backend.GetAccountById(in.AccountId)
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

// GetContract returns the smart contract stored at the given address.
func (s *Server) GetContract(_ context.Context, in *apipb.BytesMessage) (*contractpb.SmartContract, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	sc, err := s.backend.GetContract(addr)
	if err != nil {
		if contractLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "contract not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if sc == nil {
		return nil, status.Error(codes.NotFound, "contract not found")
	}
	return sc, nil
}

// GetContractInfo returns the smart contract along with its runtime bytecode.
func (s *Server) GetContractInfo(_ context.Context, in *apipb.BytesMessage) (*contractpb.SmartContractDataWrapper, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	sc, err := s.backend.GetContract(addr)
	if err != nil {
		if contractLookupNotFound(err) {
			return nil, status.Error(codes.NotFound, "contract not found")
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	if sc == nil {
		return nil, status.Error(codes.NotFound, "contract not found")
	}
	return &contractpb.SmartContractDataWrapper{
		SmartContract: sc,
		Runtimecode:   sc.Bytecode,
	}, nil
}

// ListWitnesses returns all registered witnesses.
func (s *Server) ListWitnesses(_ context.Context, _ *apipb.EmptyMessage) (*apipb.WitnessList, error) {
	witnesses, err := s.backend.ListWitnesses()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return witnessListFromInfos(witnesses), nil
}

func (s *Server) GetPaginatedNowWitnessList(_ context.Context, in *apipb.PaginatedMessage) (*apipb.WitnessList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	witnesses, err := s.backend.ListWitnesses()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	page, err := paginateWitnessInfos(witnesses, in.Offset, in.Limit)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return witnessListFromInfos(page), nil
}

func witnessListFromInfos(witnesses []*tronapi.WitnessInfo) *apipb.WitnessList {
	result := make([]*corepb.Witness, 0, len(witnesses))
	for _, w := range witnesses {
		if w == nil {
			continue
		}
		result = append(result, &corepb.Witness{
			Address:   common.FromHex(w.Address),
			VoteCount: w.VoteCount,
			Url:       w.URL,
			IsJobs:    w.IsJobs,
		})
	}
	return &apipb.WitnessList{Witnesses: result}
}

func paginateWitnessInfos(witnesses []*tronapi.WitnessInfo, offset, limit int64) ([]*tronapi.WitnessInfo, error) {
	if offset < 0 {
		return nil, fmt.Errorf("offset must be non-negative")
	}
	if limit < 0 {
		return nil, fmt.Errorf("limit must be non-negative")
	}
	if limit == 0 || offset >= int64(len(witnesses)) {
		return nil, nil
	}
	remaining := int64(len(witnesses)) - offset
	if limit > remaining {
		limit = remaining
	}
	start := int(offset)
	end := int(offset + limit)
	return witnesses[start:end], nil
}

// GetNextMaintenanceTime returns the timestamp of the next maintenance window.
func (s *Server) GetNextMaintenanceTime(_ context.Context, _ *apipb.EmptyMessage) (*apipb.NumberMessage, error) {
	next, err := s.backend.NextMaintenanceTime()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.NumberMessage{Num: next}, nil
}

// ── PR-A2: Resource / Market / TRC10 / Node read RPCs ────────────────────────

// GetAccountResource returns bandwidth and energy usage for an address.
func (s *Server) GetAccountResource(_ context.Context, in *corepb.Account) (*apipb.AccountResourceMessage, error) {
	if in == nil || len(in.Address) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Address)
	res, err := s.backend.GetAccountResource(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if res == nil {
		return &apipb.AccountResourceMessage{}, nil
	}
	return &apipb.AccountResourceMessage{
		FreeNetUsed:          res.FreeNetUsed,
		FreeNetLimit:         res.FreeNetLimit,
		NetUsed:              res.NetUsed,
		NetLimit:             res.NetLimit,
		TotalNetLimit:        res.TotalNetLimit,
		TotalNetWeight:       res.TotalNetWeight,
		TotalTronPowerWeight: res.TotalTronPowerWeight,
		TronPowerUsed:        res.TronPowerUsed,
		TronPowerLimit:       res.TronPowerLimit,
		EnergyUsed:           res.EnergyUsed,
		EnergyLimit:          res.EnergyLimit,
		TotalEnergyLimit:     res.TotalEnergyLimit,
		TotalEnergyWeight:    res.TotalEnergyWeight,
		StorageUsed:          res.StorageUsed,
		StorageLimit:         res.StorageLimit,
	}, nil
}

// GetDelegatedResourceV2 returns the delegation record from one address to another.
func (s *Server) GetDelegatedResource(_ context.Context, in *apipb.DelegatedResourceMessage) (*apipb.DelegatedResourceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	from := common.BytesToAddress(in.FromAddress)
	to := common.BytesToAddress(in.ToAddress)
	infos, err := s.backend.GetDelegatedResource(from, to)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.DelegatedResourceList{
		DelegatedResource: delegatedResourcesFromInfos(in.FromAddress, in.ToAddress, infos),
	}, nil
}

func (s *Server) GetDelegatedResourceV2(_ context.Context, in *apipb.DelegatedResourceMessage) (*apipb.DelegatedResourceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	from := common.BytesToAddress(in.FromAddress)
	to := common.BytesToAddress(in.ToAddress)
	infos, err := s.backend.GetDelegatedResourceV2(from, to)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.DelegatedResourceList{
		DelegatedResource: delegatedResourcesFromInfos(in.FromAddress, in.ToAddress, infos),
	}, nil
}

func delegatedResourcesFromInfos(from, to []byte, infos []*tronapi.DelegatedResourceInfo) []*corepb.DelegatedResource {
	if len(infos) == 0 {
		return nil
	}
	resources := make([]*corepb.DelegatedResource, len(infos))
	for i, info := range infos {
		resources[i] = &corepb.DelegatedResource{
			From:                      from,
			To:                        to,
			FrozenBalanceForBandwidth: info.FrozenBalanceForBandwidth,
			FrozenBalanceForEnergy:    info.FrozenBalanceForEnergy,
			ExpireTimeForBandwidth:    info.ExpireTimeForBandwidth,
			ExpireTimeForEnergy:       info.ExpireTimeForEnergy,
		}
	}
	return resources
}

// GetDelegatedResourceAccountIndexV2 returns the delegation index for an address.
func (s *Server) GetDelegatedResourceAccountIndex(_ context.Context, in *apipb.BytesMessage) (*corepb.DelegatedResourceAccountIndex, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	idx, err := s.backend.GetDelegatedResourceAccountIndex(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if idx == nil {
		return &corepb.DelegatedResourceAccountIndex{Account: in.Value}, nil
	}
	return idx, nil
}

func (s *Server) GetDelegatedResourceAccountIndexV2(_ context.Context, in *apipb.BytesMessage) (*corepb.DelegatedResourceAccountIndex, error) {
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
	return &corepb.DelegatedResourceAccountIndex{
		Account:    in.Value,
		ToAccounts: toAccounts,
	}, nil
}

// GetCanDelegatedMaxSize returns the maximum resource an address can still delegate.
func (s *Server) GetCanDelegatedMaxSize(_ context.Context, in *apipb.CanDelegatedMaxSizeRequestMessage) (*apipb.CanDelegatedMaxSizeResponseMessage, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "owner address required")
	}
	addr := common.BytesToAddress(in.OwnerAddress)
	resource := corepb.ResourceCode(in.Type)
	info, err := s.backend.CanDelegateResource(addr, 0, resource)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if info == nil {
		return &apipb.CanDelegatedMaxSizeResponseMessage{}, nil
	}
	return &apipb.CanDelegatedMaxSizeResponseMessage{MaxSize: info.CanDelegateSize}, nil
}

// GetAvailableUnfreezeCount returns the number of remaining unfreeze slots.
func (s *Server) GetAvailableUnfreezeCount(_ context.Context, in *apipb.GetAvailableUnfreezeCountRequestMessage) (*apipb.GetAvailableUnfreezeCountResponseMessage, error) {
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

// GetCanWithdrawUnfreezeAmount returns the withdrawable expired-unfreeze amount.
func (s *Server) GetCanWithdrawUnfreezeAmount(_ context.Context, in *apipb.CanWithdrawUnfreezeAmountRequestMessage) (*apipb.CanWithdrawUnfreezeAmountResponseMessage, error) {
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

// GetRewardInfo returns the unclaimed witness reward for the given address.
func (s *Server) GetRewardInfo(_ context.Context, in *apipb.BytesMessage) (*apipb.NumberMessage, error) {
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

// GetBrokerageInfo returns the brokerage rate (0–100) for the given witness address.
func (s *Server) GetBrokerageInfo(_ context.Context, in *apipb.BytesMessage) (*apipb.NumberMessage, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	rate, err := s.backend.GetBrokerageInfo(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.NumberMessage{Num: rate}, nil
}

// GetAssetIssueById returns the TRC10 token with the given numeric ID.
func (s *Server) GetAssetIssueById(_ context.Context, in *apipb.BytesMessage) (*contractpb.AssetIssueContract, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "asset id required")
	}
	// Decode numeric ID from ASCII bytes.
	var id int64
	for _, b := range in.Value {
		if b < '0' || b > '9' {
			return nil, status.Error(codes.InvalidArgument, "asset id must be numeric")
		}
		id = id*10 + int64(b-'0')
	}
	ac, err := s.backend.GetAssetIssueByID(id)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if ac == nil {
		return nil, status.Error(codes.NotFound, "asset not found")
	}
	return ac, nil
}

func (s *Server) GetAssetIssueByName(_ context.Context, in *apipb.BytesMessage) (*contractpb.AssetIssueContract, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "asset name required")
	}
	ac, err := s.backend.GetAssetIssueByName(in.Value)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if ac == nil {
		return nil, status.Error(codes.NotFound, "asset not found")
	}
	return ac, nil
}

func (s *Server) GetAssetIssueListByName(_ context.Context, in *apipb.BytesMessage) (*apipb.AssetIssueList, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "asset name required")
	}
	ac, err := s.backend.GetAssetIssueByName(in.Value)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if ac == nil {
		return &apipb.AssetIssueList{}, nil
	}
	return &apipb.AssetIssueList{AssetIssue: []*contractpb.AssetIssueContract{ac}}, nil
}

// GetAssetIssueByAccount returns the TRC10 token created by the given account.
func (s *Server) GetAssetIssueByAccount(_ context.Context, in *corepb.Account) (*apipb.AssetIssueList, error) {
	if in == nil || len(in.Address) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Address)
	ac, err := s.backend.GetAssetIssueByAccount(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if ac == nil {
		return &apipb.AssetIssueList{}, nil
	}
	return &apipb.AssetIssueList{AssetIssue: []*contractpb.AssetIssueContract{ac}}, nil
}

// GetAssetIssueList returns all TRC10 tokens.
func (s *Server) GetAssetIssueList(_ context.Context, _ *apipb.EmptyMessage) (*apipb.AssetIssueList, error) {
	assets, err := s.backend.GetAssetIssueList()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.AssetIssueList{AssetIssue: assets}, nil
}

// GetMarketOrderById returns the market order with the given ID bytes.
func (s *Server) GetMarketOrderById(_ context.Context, in *apipb.BytesMessage) (*corepb.MarketOrder, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "order id required")
	}
	order, err := s.backend.GetMarketOrderByID(in.Value)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if order == nil {
		return nil, status.Error(codes.NotFound, "market order not found")
	}
	return order, nil
}

// GetMarketOrderByAccount returns all market orders for the given account.
func (s *Server) GetMarketOrderByAccount(_ context.Context, in *apipb.BytesMessage) (*corepb.MarketOrderList, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "address required")
	}
	addr := common.BytesToAddress(in.Value)
	orders, err := s.backend.GetMarketOrdersByAccount(addr)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &corepb.MarketOrderList{Orders: orders}, nil
}

// GetMarketPriceByPair returns the price list for a sell/buy token pair.
func (s *Server) GetMarketPriceByPair(_ context.Context, in *corepb.MarketOrderPair) (*corepb.MarketPriceList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	pl, err := s.backend.GetMarketPriceByPair(in.SellTokenId, in.BuyTokenId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if pl == nil {
		return &corepb.MarketPriceList{}, nil
	}
	return pl, nil
}

// GetMarketOrderListByPair returns all active orders for a sell/buy token pair.
func (s *Server) GetMarketOrderListByPair(_ context.Context, in *corepb.MarketOrderPair) (*corepb.MarketOrderList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	orders, err := s.backend.GetMarketOrderListByPair(in.SellTokenId, in.BuyTokenId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &corepb.MarketOrderList{Orders: orders}, nil
}

// GetMarketPairList returns all active market pairs.
func (s *Server) GetMarketPairList(_ context.Context, _ *apipb.EmptyMessage) (*corepb.MarketOrderPairList, error) {
	pairs, err := s.backend.GetMarketPairList()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if pairs == nil {
		return &corepb.MarketOrderPairList{}, nil
	}
	return pairs, nil
}

// ListNodes returns connected P2P peers as a NodeList.
func (s *Server) ListNodes(_ context.Context, _ *apipb.EmptyMessage) (*apipb.NodeList, error) {
	peers, err := s.backend.ListNodes()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	nodes := make([]*apipb.Node, len(peers))
	for i, p := range peers {
		nodes[i] = &apipb.Node{
			Address: &apipb.Address{
				Host: []byte(p.Host),
				Port: int32(p.Port),
			},
		}
	}
	return &apipb.NodeList{Nodes: nodes}, nil
}

// GetNodeInfo returns basic information about the current node.
func (s *Server) GetNodeInfo(_ context.Context, _ *apipb.EmptyMessage) (*corepb.NodeInfo, error) {
	info := s.backend.GetNodeInfo()
	return &corepb.NodeInfo{
		Block: fmt.Sprintf("Num:%d", info.CurrentBlock),
	}, nil
}

// ListProposals returns all governance proposals.
func (s *Server) ListProposals(_ context.Context, _ *apipb.EmptyMessage) (*apipb.ProposalList, error) {
	proposals, err := s.backend.ListProposals()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	result := make([]*corepb.Proposal, len(proposals))
	for i, p := range proposals {
		approvals := make([][]byte, len(p.Approvals))
		for j, a := range p.Approvals {
			approvals[j] = common.FromHex(a)
		}
		params := make(map[int64]int64, len(p.Parameters))
		for _, e := range p.Parameters {
			params[e.Key] = e.Value
		}
		result[i] = &corepb.Proposal{
			ProposalId:      p.ProposalID,
			ProposerAddress: common.FromHex(p.ProposerAddress),
			Parameters:      params,
			ExpirationTime:  p.ExpirationTime,
			CreateTime:      p.CreateTime,
			Approvals:       approvals,
		}
	}
	return &apipb.ProposalList{Proposals: result}, nil
}

// ListExchanges returns all Bancor exchanges.
func (s *Server) ListExchanges(_ context.Context, _ *apipb.EmptyMessage) (*apipb.ExchangeList, error) {
	exchanges, err := s.backend.ListExchanges()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.ExchangeList{Exchanges: exchanges}, nil
}

func (s *Server) GetExchangeById(_ context.Context, in *apipb.BytesMessage) (*corepb.Exchange, error) {
	id, err := parseExchangeIDMessage(in)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	exchange, err := s.backend.GetExchangeByID(id)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	if exchange == nil {
		return nil, status.Error(codes.NotFound, "exchange not found")
	}
	return exchange, nil
}

// GetTransactionInfoById returns the receipt and log for the given transaction hash.
func (s *Server) GetTransactionInfoById(_ context.Context, in *apipb.BytesMessage) (*corepb.TransactionInfo, error) {
	if in == nil || len(in.Value) == 0 {
		return nil, status.Error(codes.InvalidArgument, "transaction hash required")
	}
	hash := common.BytesToHash(in.Value)
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

func transactionLookupNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	return msg == "transaction not found" || msg == "transaction info not found"
}

func blockLookupNotFound(err error) bool {
	return readLookupMiss(err)
}

func accountLookupNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.TrimSpace(err.Error())
	return msg == "account not found" || strings.HasPrefix(msg, "account not found at block ")
}

func contractLookupNotFound(err error) bool {
	if err == nil {
		return false
	}
	return strings.TrimSpace(err.Error()) == "contract not found"
}

func parseExchangeIDMessage(in *apipb.BytesMessage) (int64, error) {
	if in == nil || len(in.Value) == 0 {
		return 0, fmt.Errorf("exchange id required")
	}
	if exchangeIDValueLooksText(in.Value) {
		id, err := strconv.ParseInt(strings.TrimSpace(string(in.Value)), 10, 64)
		if err != nil || id <= 0 {
			return 0, fmt.Errorf("exchange id must be numeric")
		}
		return id, nil
	}
	if len(in.Value) > 8 {
		return 0, fmt.Errorf("exchange id must be numeric")
	}
	var buf [8]byte
	copy(buf[8-len(in.Value):], in.Value)
	id := int64(binary.BigEndian.Uint64(buf[:]))
	if id <= 0 {
		return 0, fmt.Errorf("exchange id must be positive")
	}
	return id, nil
}

func exchangeIDValueLooksText(value []byte) bool {
	for _, b := range value {
		if b != '\t' && b != '\n' && b != '\r' && (b < ' ' || b > '~') {
			return false
		}
	}
	return true
}

func readLookupMiss(err error) bool {
	// Block lookup APIs keep the legacy java-tron-style empty/not-found
	// surface. Account and contract archive reads use narrower helpers so cold
	// reconstruction failures remain visible.
	return err != nil
}

// GetTransactionInfoByBlockNum returns all transaction receipts in the given block.
func (s *Server) GetTransactionInfoByBlockNum(_ context.Context, in *apipb.NumberMessage) (*apipb.TransactionInfoList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	infos, err := s.backend.GetTransactionInfoByBlockNum(uint64(in.Num))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.TransactionInfoList{TransactionInfo: infos}, nil
}

// GetTransactionListFromPending returns all transactions currently in the mempool.
func (s *Server) GetTransactionListFromPending(_ context.Context, _ *apipb.EmptyMessage) (*apipb.TransactionIdList, error) {
	txs, err := s.backend.GetTransactionListFromPending()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ids := make([]string, len(txs))
	for i, tx := range txs {
		h := common.BytesToHash(tx.RawData.GetRefBlockBytes())
		ids[i] = h.Hex()
	}
	return &apipb.TransactionIdList{TxId: ids}, nil
}

// TotalTransaction returns the total number of transactions ever processed.
func (s *Server) TotalTransaction(_ context.Context, _ *apipb.EmptyMessage) (*apipb.NumberMessage, error) {
	count, err := s.backend.TotalTransaction()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.NumberMessage{Num: count}, nil
}

// GetBurnTrx returns the amount of TRX burned by energy consumption.
func (s *Server) GetBurnTrx(_ context.Context, _ *apipb.EmptyMessage) (*apipb.NumberMessage, error) {
	burned, err := s.backend.GetBurnTrx()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.NumberMessage{Num: burned}, nil
}

// ── PR-B: Transaction building RPCs ──────────────────────────────────────────

// txToExtention wraps a built transaction as a TransactionExtention with its ID.
func txToExtention(tx *corepb.Transaction) *apipb.TransactionExtention {
	if tx == nil {
		return nil
	}
	rawBytes, _ := proto.Marshal(tx.RawData)
	txid := common.Sha256(rawBytes)
	return &apipb.TransactionExtention{
		Transaction: tx,
		Txid:        txid[:],
		Result:      &apipb.Return{Result: true},
	}
}

// CreateTransaction builds a TRX transfer transaction.
func (s *Server) CreateTransaction(_ context.Context, in *contractpb.TransferContract) (*corepb.Transaction, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	to := common.BytesToAddress(in.ToAddress)
	tx, err := s.backend.BuildTransferTransaction(owner, to, in.Amount)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return tx, nil
}

// CreateTransaction2 builds a TRX transfer transaction and returns a TransactionExtention.
func (s *Server) CreateTransaction2(_ context.Context, in *contractpb.TransferContract) (*apipb.TransactionExtention, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	to := common.BytesToAddress(in.ToAddress)
	tx, err := s.backend.BuildTransferTransaction(owner, to, in.Amount)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// BroadcastTransaction submits a signed transaction to the network.
func (s *Server) BroadcastTransaction(_ context.Context, in *corepb.Transaction) (*apipb.Return, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "transaction required")
	}
	tx := types.NewTransactionFromPB(in)
	if err := s.backend.BroadcastTransaction(tx); err != nil {
		return &apipb.Return{
			Result:  false,
			Code:    apipb.Return_SERVER_BUSY,
			Message: []byte(err.Error()),
		}, nil
	}
	return &apipb.Return{Result: true, Code: apipb.Return_SUCCESS}, nil
}

// VoteWitnessAccount2 builds a vote-witness transaction.
func (s *Server) VoteWitnessAccount2(_ context.Context, in *contractpb.VoteWitnessContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	votes := make(map[common.Address]int64, len(in.Votes))
	for _, v := range in.Votes {
		votes[common.BytesToAddress(v.VoteAddress)] = v.VoteCount
	}
	tx, err := s.backend.BuildVoteWitnessTransaction(owner, votes)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// FreezeBalanceV2 builds a Stake 2.0 freeze transaction.
func (s *Server) FreezeBalanceV2(_ context.Context, in *contractpb.FreezeBalanceV2Contract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	tx, err := s.backend.BuildFreezeBalanceV2Transaction(owner, in.FrozenBalance, in.Resource)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// UnfreezeBalanceV2 builds a Stake 2.0 unfreeze transaction.
func (s *Server) UnfreezeBalanceV2(_ context.Context, in *contractpb.UnfreezeBalanceV2Contract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	tx, err := s.backend.BuildUnfreezeBalanceV2Transaction(owner, in.UnfreezeBalance, in.Resource)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// DelegateResource builds a resource delegation transaction.
func (s *Server) DelegateResource(_ context.Context, in *contractpb.DelegateResourceContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	receiver := common.BytesToAddress(in.ReceiverAddress)
	tx, err := s.backend.BuildDelegateResourceTransaction(owner, receiver, in.Balance, in.Resource, in.Lock)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// UnDelegateResource builds a resource undelegation transaction.
func (s *Server) UnDelegateResource(_ context.Context, in *contractpb.UnDelegateResourceContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	receiver := common.BytesToAddress(in.ReceiverAddress)
	tx, err := s.backend.BuildUnDelegateResourceTransaction(owner, receiver, in.Balance, in.Resource)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// CancelAllUnfreezeV2 builds a cancel-all-unfreeze transaction.
func (s *Server) CancelAllUnfreezeV2(_ context.Context, in *contractpb.CancelAllUnfreezeV2Contract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	tx, err := s.backend.BuildCancelAllUnfreezeV2Transaction(owner)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// WithdrawExpireUnfreeze builds a withdraw-expired-unfreeze transaction.
func (s *Server) WithdrawExpireUnfreeze(_ context.Context, in *contractpb.WithdrawExpireUnfreezeContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	tx, err := s.backend.BuildWithdrawExpireUnfreezeTransaction(owner)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// ProposalCreate builds a governance proposal creation transaction.
func (s *Server) ProposalCreate(_ context.Context, in *contractpb.ProposalCreateContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	tx, err := s.backend.BuildProposalCreateTransaction(owner, in.Parameters)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// ProposalApprove builds a proposal approval transaction.
func (s *Server) ProposalApprove(_ context.Context, in *contractpb.ProposalApproveContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	tx, err := s.backend.BuildProposalApproveTransaction(owner, in.ProposalId, in.IsAddApproval)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// ProposalDelete builds a proposal deletion transaction.
func (s *Server) ProposalDelete(_ context.Context, in *contractpb.ProposalDeleteContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	tx, err := s.backend.BuildProposalDeleteTransaction(owner, in.ProposalId)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// DeployContract builds a smart-contract deployment transaction.
func (s *Server) DeployContract(_ context.Context, in *contractpb.CreateSmartContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	newContract := in.NewContract
	if newContract == nil {
		return nil, status.Error(codes.InvalidArgument, "new_contract required")
	}
	tx, err := s.backend.BuildDeployContractTransaction(
		owner,
		"", // ABI JSON — passed via NewContract.Abi in the caller
		newContract.Bytecode,
		0, // feeLimit set by client before signing
		newContract.CallValue,
		newContract.Name,
		newContract.ConsumeUserResourcePercent,
	)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return txToExtention(tx), nil
}

// TriggerContract builds a smart-contract trigger transaction (with simulation).
func (s *Server) TriggerContract(_ context.Context, in *contractpb.TriggerSmartContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	contract := common.BytesToAddress(in.ContractAddress)
	tx, result, err := s.backend.BuildTriggerContractTransaction(owner, contract, in.Data, 0, in.CallValue)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	ext := txToExtention(tx)
	if result != nil {
		ext.EnergyUsed = result.EnergyUsed
		if len(result.Result) > 0 {
			ext.ConstantResult = [][]byte{result.Result}
		}
	}
	return ext, nil
}

// TriggerConstantContract simulates a smart-contract call (read-only).
func (s *Server) TriggerConstantContract(_ context.Context, in *contractpb.TriggerSmartContract) (*apipb.TransactionExtention, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	contract := common.BytesToAddress(in.ContractAddress)
	result, err := s.backend.TriggerConstantContract(owner, contract, in.Data, 0)
	ret := &apipb.Return{Result: true, Code: apipb.Return_SUCCESS}
	if err != nil {
		ret = &apipb.Return{Result: false, Code: apipb.Return_SIGERROR, Message: []byte(err.Error())}
	}
	ext := &apipb.TransactionExtention{Result: ret}
	if result != nil {
		ext.EnergyUsed = result.EnergyUsed
		if len(result.Result) > 0 {
			ext.ConstantResult = [][]byte{result.Result}
		}
	}
	return ext, nil
}

// EstimateEnergy estimates the energy required for a smart-contract call.
func (s *Server) EstimateEnergy(_ context.Context, in *contractpb.TriggerSmartContract) (*apipb.EstimateEnergyMessage, error) {
	if in == nil || len(in.OwnerAddress) == 0 {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	owner := common.BytesToAddress(in.OwnerAddress)
	contract := common.BytesToAddress(in.ContractAddress)
	energy, err := s.backend.EstimateEnergy(owner, contract, in.Data)
	if err != nil {
		return &apipb.EstimateEnergyMessage{
			Result:         &apipb.Return{Result: false, Message: []byte(err.Error())},
			EnergyRequired: 0,
		}, nil
	}
	return &apipb.EstimateEnergyMessage{
		Result:         &apipb.Return{Result: true, Code: apipb.Return_SUCCESS},
		EnergyRequired: energy,
	}, nil
}

// ── PR-C: Sign weight helper ──────────────────────────────────────────────────

// GetTransactionSignWeight returns multi-sig weight info for a transaction.
func (s *Server) GetTransactionSignWeight(_ context.Context, in *corepb.Transaction) (*apipb.TransactionSignWeight, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "transaction required")
	}
	return &apipb.TransactionSignWeight{
		Transaction: txToExtention(in),
		Result: &apipb.TransactionSignWeight_Result{
			Code: apipb.TransactionSignWeight_Result_ENOUGH_PERMISSION,
		},
	}, nil
}

// ── PR-E: Monitor/Node — paginated + price history ────────────────────────────

// GetPaginatedProposalList returns a page of governance proposals.
func (s *Server) GetPaginatedProposalList(_ context.Context, in *apipb.PaginatedMessage) (*apipb.ProposalList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	proposals, err := s.backend.ListProposalsPaginated(int(in.Offset), int(in.Limit))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	result := make([]*corepb.Proposal, len(proposals))
	for i, p := range proposals {
		approvals := make([][]byte, len(p.Approvals))
		for j, a := range p.Approvals {
			approvals[j] = common.FromHex(a)
		}
		params := make(map[int64]int64, len(p.Parameters))
		for _, e := range p.Parameters {
			params[e.Key] = e.Value
		}
		result[i] = &corepb.Proposal{
			ProposalId:      p.ProposalID,
			ProposerAddress: common.FromHex(p.ProposerAddress),
			Parameters:      params,
			ExpirationTime:  p.ExpirationTime,
			CreateTime:      p.CreateTime,
			Approvals:       approvals,
		}
	}
	return &apipb.ProposalList{Proposals: result}, nil
}

// GetPaginatedAssetIssueList returns a page of TRC10 assets.
func (s *Server) GetPaginatedAssetIssueList(_ context.Context, in *apipb.PaginatedMessage) (*apipb.AssetIssueList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	assets, err := s.backend.GetAssetIssueListPaginated(int(in.Offset), int(in.Limit))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.AssetIssueList{AssetIssue: assets}, nil
}

// GetPaginatedExchangeList returns a page of Bancor exchanges.
func (s *Server) GetPaginatedExchangeList(_ context.Context, in *apipb.PaginatedMessage) (*apipb.ExchangeList, error) {
	if in == nil {
		return nil, status.Error(codes.InvalidArgument, "request required")
	}
	exchanges, err := s.backend.ListExchangesPaginated(int(in.Offset), int(in.Limit))
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.ExchangeList{Exchanges: exchanges}, nil
}

// GetBandwidthPrices returns the historical bandwidth price string.
func (s *Server) GetBandwidthPrices(_ context.Context, _ *apipb.EmptyMessage) (*apipb.PricesResponseMessage, error) {
	prices, err := s.backend.GetBandwidthPrices()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.PricesResponseMessage{Prices: prices}, nil
}

// GetEnergyPrices returns the historical energy price string.
func (s *Server) GetEnergyPrices(_ context.Context, _ *apipb.EmptyMessage) (*apipb.PricesResponseMessage, error) {
	prices, err := s.backend.GetEnergyPrices()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &apipb.PricesResponseMessage{Prices: prices}, nil
}
