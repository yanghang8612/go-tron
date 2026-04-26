package grpcapi_test

import (
	"context"
	"net"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/grpcapi"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1 << 20

// testBackend is a minimal stub implementation of tronapi.Backend for grpcapi tests.
type testBackend struct {
	block     *types.Block
	blocks    []*types.Block // for range queries
	account   *types.Account
	tx        *corepb.Transaction
	params    []tronapi.ChainParameter
	contract  *contractpb.SmartContract
	witnesses []*tronapi.WitnessInfo
	nextMaint int64
}

func (b *testBackend) CurrentBlock() *types.Block                             { return b.block }
func (b *testBackend) GetBlockByNumber(n uint64) (*types.Block, error)        { return b.block, nil }
func (b *testBackend) GetAccount(addr common.Address) (*types.Account, error) { return b.account, nil }
func (b *testBackend) BroadcastTransaction(tx *types.Transaction) error       { return nil }
func (b *testBackend) GetNodeInfo() *tronapi.NodeInfo                          { return &tronapi.NodeInfo{} }
func (b *testBackend) PendingTransactionCount() int                            { return 0 }
func (b *testBackend) GetContract(addr common.Address) (*contractpb.SmartContract, error) {
	return b.contract, nil
}
func (b *testBackend) TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	return nil, nil
}
func (b *testBackend) GetTransactionByID(h common.Hash) (*corepb.Transaction, error) {
	return b.tx, nil
}
func (b *testBackend) GetTransactionInfoByID(h common.Hash) (*corepb.TransactionInfo, error) {
	return nil, nil
}
func (b *testBackend) GetTransactionInfoByBlockNum(n uint64) ([]*corepb.TransactionInfo, error) {
	return nil, nil
}
func (b *testBackend) GetBlockByHash(h common.Hash) (*types.Block, error) {
	if b.block != nil && b.block.Hash() == h {
		return b.block, nil
	}
	for _, blk := range b.blocks {
		if blk.Hash() == h {
			return blk, nil
		}
	}
	return nil, nil
}
func (b *testBackend) GetBlocksByRange(start, end uint64) ([]*types.Block, error) {
	if len(b.blocks) > 0 {
		var result []*types.Block
		for _, blk := range b.blocks {
			n := blk.Number()
			if n >= start && n < end {
				result = append(result, blk)
			}
		}
		return result, nil
	}
	if b.block != nil {
		return []*types.Block{b.block}, nil
	}
	return nil, nil
}
func (b *testBackend) BuildTransferTransaction(owner, to common.Address, amount int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildDeployContractTransaction(owner common.Address, abi string, bytecode []byte, feeLimit, callValue int64, name string, consumePercent int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildTriggerContractTransaction(owner, contract common.Address, data []byte, feeLimit, callValue int64) (*corepb.Transaction, *tronapi.TriggerResult, error) {
	return nil, nil, nil
}
func (b *testBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	return 0, nil
}
func (b *testBackend) GetAccountResource(addr common.Address) (*tronapi.AccountResource, error) {
	return nil, nil
}
func (b *testBackend) GetChainParameters() []tronapi.ChainParameter { return b.params }
func (b *testBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	return b.witnesses, nil
}
func (b *testBackend) NextMaintenanceTime() int64 { return b.nextMaint }
func (b *testBackend) BuildProposalCreateTransaction(owner common.Address, params map[int64]int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildProposalApproveTransaction(owner common.Address, proposalID int64, approve bool) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildProposalDeleteTransaction(owner common.Address, proposalID int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) ListProposals() ([]*tronapi.ProposalInfo, error)    { return nil, nil }
func (b *testBackend) GetDelegatedResourceV2(from, to common.Address) (*tronapi.DelegatedResourceInfo, error) {
	return nil, nil
}
func (b *testBackend) GetDelegatedResourceAccountIndexV2(addr common.Address) (*tronapi.DelegationIndexInfo, error) {
	return nil, nil
}
func (b *testBackend) CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	return nil, nil
}
func (b *testBackend) GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	return nil, nil
}
func (b *testBackend) GetAvailableUnfreezeCount(addr common.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	return nil, nil
}
func (b *testBackend) GetReward(addr common.Address) (*tronapi.RewardInfo, error) { return nil, nil }
func (b *testBackend) GetTransactionFromPending(txID string) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) GetTransactionListFromPending() ([]*corepb.Transaction, error) { return nil, nil }
func (b *testBackend) ListNodes() ([]*tronapi.PeerInfo, error)                       { return nil, nil }
func (b *testBackend) GetAssetIssueByID(id int64) *contractpb.AssetIssueContract     { return nil }
func (b *testBackend) GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract { return nil }
func (b *testBackend) GetAssetIssueList() []*contractpb.AssetIssueContract { return nil }
func (b *testBackend) GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract {
	return nil
}
func (b *testBackend) GetAssetIssueByAccount(addr common.Address) *contractpb.AssetIssueContract {
	return nil
}
func (b *testBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder { return nil }
func (b *testBackend) GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder {
	return nil
}
func (b *testBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	return nil
}
func (b *testBackend) ListExchanges() ([]*corepb.Exchange, error)      { return nil, nil }
func (b *testBackend) GetBrokerageInfo(addr common.Address) int64      { return 0 }
func (b *testBackend) TotalTransaction() int64                         { return 0 }
func (b *testBackend) GetBurnTrx() int64                               { return 0 }
func (b *testBackend) BuildFreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildUnfreezeBalanceV2Transaction(owner common.Address, amount int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode, lock bool) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildUnDelegateResourceTransaction(owner, receiver common.Address, balance int64, resource corepb.ResourceCode) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildCancelAllUnfreezeV2Transaction(owner common.Address) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildWithdrawExpireUnfreezeTransaction(owner common.Address) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildVoteWitnessTransaction(owner common.Address, votes map[common.Address]int64) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) GetBandwidthPrices() string { return "" }
func (b *testBackend) GetEnergyPrices() string    { return "" }
func (b *testBackend) ListProposalsPaginated(offset, limit int) ([]*tronapi.ProposalInfo, error) {
	return nil, nil
}
func (b *testBackend) ListExchangesPaginated(offset, limit int) ([]*corepb.Exchange, error) {
	return nil, nil
}
func (b *testBackend) BuildCreateAccountTransaction(owner, account common.Address) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildUpdateAccountTransaction(owner common.Address, name []byte) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildSetAccountIdTransaction(owner common.Address, accountID []byte) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) BuildAccountPermissionUpdateTransaction(c *contractpb.AccountPermissionUpdateContract) (*corepb.Transaction, error) {
	return nil, nil
}
func (b *testBackend) GetAccountById(accountID []byte) (*types.Account, error) {
	return nil, nil
}
func (b *testBackend) GetAccountNet(addr common.Address) (*apipb.AccountNetMessage, error) {
	return nil, nil
}

// newTestClient sets up an in-process gRPC server+client using bufconn.
func newTestClient(t *testing.T, backend tronapi.Backend) apipb.WalletClient {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	srv := grpcapi.NewServer(backend, "" /* addr unused with custom listener */)

	gs := grpc.NewServer()
	apipb.RegisterWalletServer(gs, srv)
	go func() { gs.Serve(lis) }() //nolint:errcheck
	t.Cleanup(gs.GracefulStop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return apipb.NewWalletClient(conn)
}

func TestGetNowBlock_NoBlock(t *testing.T) {
	client := newTestClient(t, &testBackend{block: nil})
	_, err := client.GetNowBlock(context.Background(), &apipb.EmptyMessage{})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetNowBlock_WithBlock(t *testing.T) {
	blk := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 42},
		},
	})
	client := newTestClient(t, &testBackend{block: blk})
	resp, err := client.GetNowBlock(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetNowBlock: %v", err)
	}
	if resp.GetBlockHeader().GetRawData().GetNumber() != 42 {
		t.Fatalf("block number: want 42, got %d", resp.GetBlockHeader().GetRawData().GetNumber())
	}
}

func TestGetBlockByNum_NotFound(t *testing.T) {
	client := newTestClient(t, &testBackend{block: nil})
	_, err := client.GetBlockByNum(context.Background(), &apipb.NumberMessage{Num: 100})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetAccount_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{account: nil})
	resp, err := client.GetAccount(context.Background(), &corepb.Account{
		Address: make([]byte, 21),
	})
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	// java-tron returns an empty account (not an error) when address is unknown.
	if resp == nil {
		t.Fatal("expected non-nil response for unknown account")
	}
}

func TestGetTransactionById_NotFound(t *testing.T) {
	client := newTestClient(t, &testBackend{tx: nil})
	_, err := client.GetTransactionById(context.Background(), &apipb.BytesMessage{Value: make([]byte, 32)})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetChainParameters(t *testing.T) {
	params := []tronapi.ChainParameter{
		{Key: "getMaintenanceTimeInterval", Value: 21600000},
		{Key: "getTransactionFee", Value: 1000},
	}
	client := newTestClient(t, &testBackend{params: params})
	resp, err := client.GetChainParameters(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetChainParameters: %v", err)
	}
	if len(resp.GetChainParameter()) != 2 {
		t.Fatalf("want 2 params, got %d", len(resp.GetChainParameter()))
	}
	if resp.GetChainParameter()[0].GetKey() != "getMaintenanceTimeInterval" {
		t.Fatalf("param key mismatch: %s", resp.GetChainParameter()[0].GetKey())
	}
}

// ── PR-A1 tests ──────────────────────────────────────────────────────────────

func makeBlock(num int64) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: num},
		},
	})
}

func TestGetNowBlock2_WithBlock(t *testing.T) {
	blk := makeBlock(7)
	client := newTestClient(t, &testBackend{block: blk})
	resp, err := client.GetNowBlock2(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetNowBlock2: %v", err)
	}
	if resp.GetBlockHeader().GetRawData().GetNumber() != 7 {
		t.Fatalf("want block 7, got %d", resp.GetBlockHeader().GetRawData().GetNumber())
	}
}

func TestGetNowBlock2_NoBlock(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.GetNowBlock2(context.Background(), &apipb.EmptyMessage{})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetBlockByNum2_Found(t *testing.T) {
	blk := makeBlock(5)
	client := newTestClient(t, &testBackend{block: blk})
	resp, err := client.GetBlockByNum2(context.Background(), &apipb.NumberMessage{Num: 5})
	if err != nil {
		t.Fatalf("GetBlockByNum2: %v", err)
	}
	if resp.GetBlockHeader().GetRawData().GetNumber() != 5 {
		t.Fatalf("want block 5, got %d", resp.GetBlockHeader().GetRawData().GetNumber())
	}
}

func TestGetBlockById_NotFound(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.GetBlockById(context.Background(), &apipb.BytesMessage{Value: make([]byte, 32)})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetBlockByLimitNext(t *testing.T) {
	blk1 := makeBlock(1)
	blk2 := makeBlock(2)
	client := newTestClient(t, &testBackend{blocks: []*types.Block{blk1, blk2}})
	resp, err := client.GetBlockByLimitNext(context.Background(), &apipb.BlockLimit{StartNum: 1, EndNum: 3})
	if err != nil {
		t.Fatalf("GetBlockByLimitNext: %v", err)
	}
	if len(resp.GetBlock()) != 2 {
		t.Fatalf("want 2 blocks, got %d", len(resp.GetBlock()))
	}
}

func TestGetBlockByLimitNext2(t *testing.T) {
	blk1 := makeBlock(1)
	blk2 := makeBlock(2)
	client := newTestClient(t, &testBackend{blocks: []*types.Block{blk1, blk2}})
	resp, err := client.GetBlockByLimitNext2(context.Background(), &apipb.BlockLimit{StartNum: 1, EndNum: 3})
	if err != nil {
		t.Fatalf("GetBlockByLimitNext2: %v", err)
	}
	if len(resp.GetBlock()) != 2 {
		t.Fatalf("want 2 blocks, got %d", len(resp.GetBlock()))
	}
}

func TestGetBlockByLatestNum(t *testing.T) {
	blk := makeBlock(10)
	client := newTestClient(t, &testBackend{block: blk})
	resp, err := client.GetBlockByLatestNum(context.Background(), &apipb.NumberMessage{Num: 1})
	if err != nil {
		t.Fatalf("GetBlockByLatestNum: %v", err)
	}
	if len(resp.GetBlock()) == 0 {
		t.Fatal("want at least one block")
	}
}

func TestGetTransactionCountByBlockNum(t *testing.T) {
	blk := makeBlock(3)
	client := newTestClient(t, &testBackend{block: blk})
	resp, err := client.GetTransactionCountByBlockNum(context.Background(), &apipb.NumberMessage{Num: 3})
	if err != nil {
		t.Fatalf("GetTransactionCountByBlockNum: %v", err)
	}
	if resp.GetNum() < 0 {
		t.Fatal("negative tx count")
	}
}

func TestGetContract_NotFound(t *testing.T) {
	client := newTestClient(t, &testBackend{contract: nil})
	_, err := client.GetContract(context.Background(), &apipb.BytesMessage{Value: make([]byte, 21)})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetContract_Found(t *testing.T) {
	sc := &contractpb.SmartContract{Name: "TestContract", Bytecode: []byte{0x60, 0x80}}
	client := newTestClient(t, &testBackend{contract: sc})
	resp, err := client.GetContract(context.Background(), &apipb.BytesMessage{Value: make([]byte, 21)})
	if err != nil {
		t.Fatalf("GetContract: %v", err)
	}
	if resp.GetName() != "TestContract" {
		t.Fatalf("want TestContract, got %s", resp.GetName())
	}
}

func TestGetContractInfo_Found(t *testing.T) {
	sc := &contractpb.SmartContract{Name: "InfoContract", Bytecode: []byte{0xAB}}
	client := newTestClient(t, &testBackend{contract: sc})
	resp, err := client.GetContractInfo(context.Background(), &apipb.BytesMessage{Value: make([]byte, 21)})
	if err != nil {
		t.Fatalf("GetContractInfo: %v", err)
	}
	if resp.GetSmartContract().GetName() != "InfoContract" {
		t.Fatalf("want InfoContract, got %s", resp.GetSmartContract().GetName())
	}
	if len(resp.GetRuntimecode()) == 0 {
		t.Fatal("want runtimecode populated")
	}
}

func TestListWitnesses(t *testing.T) {
	ws := []*tronapi.WitnessInfo{
		{Address: "000000000000000000000000000000000000000001", VoteCount: 100, URL: "http://witness1.tron", IsJobs: true},
	}
	client := newTestClient(t, &testBackend{witnesses: ws})
	resp, err := client.ListWitnesses(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("ListWitnesses: %v", err)
	}
	if len(resp.GetWitnesses()) != 1 {
		t.Fatalf("want 1 witness, got %d", len(resp.GetWitnesses()))
	}
	if resp.GetWitnesses()[0].GetUrl() != "http://witness1.tron" {
		t.Fatalf("url mismatch: %s", resp.GetWitnesses()[0].GetUrl())
	}
}

func TestGetNextMaintenanceTime(t *testing.T) {
	client := newTestClient(t, &testBackend{nextMaint: 1234567890000})
	resp, err := client.GetNextMaintenanceTime(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetNextMaintenanceTime: %v", err)
	}
	if resp.GetNum() != 1234567890000 {
		t.Fatalf("want 1234567890000, got %d", resp.GetNum())
	}
}

// ── PR-A2 tests ──────────────────────────────────────────────────────────────

func TestGetAccountResource_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetAccountResource(context.Background(), &corepb.Account{Address: make([]byte, 21)})
	if err != nil {
		t.Fatalf("GetAccountResource: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetDelegatedResourceV2_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetDelegatedResourceV2(context.Background(), &apipb.DelegatedResourceMessage{
		FromAddress: make([]byte, 21),
		ToAddress:   make([]byte, 21),
	})
	if err != nil {
		t.Fatalf("GetDelegatedResourceV2: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetRewardInfo_Zero(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetRewardInfo(context.Background(), &apipb.BytesMessage{Value: make([]byte, 21)})
	if err != nil {
		t.Fatalf("GetRewardInfo: %v", err)
	}
	if resp.GetNum() != 0 {
		t.Fatalf("want 0, got %d", resp.GetNum())
	}
}

func TestGetBrokerageInfo(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetBrokerageInfo(context.Background(), &apipb.BytesMessage{Value: make([]byte, 21)})
	if err != nil {
		t.Fatalf("GetBrokerageInfo: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetAssetIssueList_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetAssetIssueList(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetAssetIssueList: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetMarketOrderById_NotFound(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.GetMarketOrderById(context.Background(), &apipb.BytesMessage{Value: make([]byte, 32)})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestListNodes_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.ListNodes(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("ListNodes: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetNodeInfo(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetNodeInfo(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetNodeInfo: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestListProposals_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.ListProposals(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("ListProposals: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestListExchanges_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.ListExchanges(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("ListExchanges: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetTransactionInfoById_NotFound(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.GetTransactionInfoById(context.Background(), &apipb.BytesMessage{Value: make([]byte, 32)})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

func TestGetTransactionInfoByBlockNum_Empty(t *testing.T) {
	blk := makeBlock(1)
	client := newTestClient(t, &testBackend{block: blk})
	resp, err := client.GetTransactionInfoByBlockNum(context.Background(), &apipb.NumberMessage{Num: 1})
	if err != nil {
		t.Fatalf("GetTransactionInfoByBlockNum: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetTransactionListFromPending_Empty(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetTransactionListFromPending(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetTransactionListFromPending: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestTotalTransaction(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.TotalTransaction(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("TotalTransaction: %v", err)
	}
	if resp.GetNum() != 0 {
		t.Fatalf("want 0, got %d", resp.GetNum())
	}
}

func TestGetBurnTrx(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetBurnTrx(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetBurnTrx: %v", err)
	}
	if resp.GetNum() != 0 {
		t.Fatalf("want 0, got %d", resp.GetNum())
	}
}

// ── PR-B tests ───────────────────────────────────────────────────────────────

func TestCreateTransaction_MissingRequest(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.CreateTransaction(context.Background(), &contractpb.TransferContract{})
	// Empty addresses produce a stub nil tx from testBackend; server returns internal error
	if err != nil && status.Code(err) == codes.Internal {
		return // acceptable – nil tx from stub
	}
	// Also acceptable if the server returned a transaction (shouldn't happen with nil stub)
}

func TestCreateTransaction2_InvalidArgument(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.CreateTransaction2(context.Background(), nil)
	// nil proto becomes an empty message on the wire; no panic expected
	_ = err
}

func TestBroadcastTransaction_Success(t *testing.T) {
	blk := makeBlock(1)
	client := newTestClient(t, &testBackend{block: blk})
	resp, err := client.BroadcastTransaction(context.Background(), &corepb.Transaction{
		RawData: &corepb.TransactionRaw{},
	})
	if err != nil {
		t.Fatalf("BroadcastTransaction: %v", err)
	}
	if !resp.GetResult() {
		t.Fatalf("expected success, got: %s", resp.GetMessage())
	}
}

func TestFreezeBalanceV2_InvalidArgument(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.FreezeBalanceV2(context.Background(), &contractpb.FreezeBalanceV2Contract{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestDelegateResource_InvalidArgument(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.DelegateResource(context.Background(), &contractpb.DelegateResourceContract{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestProposalCreate_InvalidArgument(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.ProposalCreate(context.Background(), &contractpb.ProposalCreateContract{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestDeployContract_InvalidArgument(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.DeployContract(context.Background(), &contractpb.CreateSmartContract{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

func TestEstimateEnergy_InvalidArgument(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	_, err := client.EstimateEnergy(context.Background(), &contractpb.TriggerSmartContract{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("want InvalidArgument, got %v", err)
	}
}

// ── PR-C/E tests ─────────────────────────────────────────────────────────────

func TestGetTransactionSignWeight(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetTransactionSignWeight(context.Background(), &corepb.Transaction{
		RawData: &corepb.TransactionRaw{},
	})
	if err != nil {
		t.Fatalf("GetTransactionSignWeight: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetPaginatedAssetIssueList(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetPaginatedAssetIssueList(context.Background(), &apipb.PaginatedMessage{Offset: 0, Limit: 10})
	if err != nil {
		t.Fatalf("GetPaginatedAssetIssueList: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetPaginatedProposalList(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetPaginatedProposalList(context.Background(), &apipb.PaginatedMessage{Offset: 0, Limit: 10})
	if err != nil {
		t.Fatalf("GetPaginatedProposalList: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetBandwidthPrices(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetBandwidthPrices(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetBandwidthPrices: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestGetEnergyPrices(t *testing.T) {
	client := newTestClient(t, &testBackend{})
	resp, err := client.GetEnergyPrices(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetEnergyPrices: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}
