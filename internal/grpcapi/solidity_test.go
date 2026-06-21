package grpcapi_test

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
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

// solidTestBackend wraps testBackend with controllable solid/pbft numbers.
type solidTestBackend struct {
	testBackend
	solidNum           uint64
	blockNumCalls      int
	lastNumQueried     uint64
	hashCalls          int
	lastHashQueried    common.Hash
	lastAccountAt      uint64
	lastAccountIDAt    uint64
	lastRewardAt       uint64
	lastDelegatedAt    uint64
	lastDelegIndexAt   uint64
	lastAssetIDAt      uint64
	lastAssetNameAt    uint64
	lastAssetListAt    uint64
	lastAssetPageAt    uint64
	lastMarketOrderAt  uint64
	lastMarketOrdersAt uint64
	lastMarketPriceAt  uint64
	lastExchangesAt    uint64
	lastCanDelegateAt  uint64
	lastAvailableAt    uint64
	lastCanWithdrawAt  uint64
	lastBurnAt         uint64
	lastBandwidthAt    uint64
	lastEnergyAt       uint64
	lastWitnessesAt    uint64
	lastBrokerageAt    uint64
	lastConstantAt     uint64
	lastEstimateAt     uint64
	txBlockNum         uint64
	txBlockOK          bool
	txBlockCalls       int
	txCalls            int
	txInfoCalls        int
	liveAccountCalls   int
	liveAccountIDCalls int
	liveRewardCalls    int
	liveDelegCalls     int
	liveIndexCalls     int
	liveAssetID        int
	liveAssetName      int
	liveAssetList      int
	liveAssetPage      int
	liveMarketOrder    int
	liveMarketOrders   int
	liveMarketPrice    int
	liveExchanges      int
	liveCanDelegate    int
	liveAvailable      int
	liveCanWithdraw    int
	liveBurn           int
	liveBandwidth      int
	liveEnergy         int
	liveWitnesses      int
	liveBrokerage      int
	liveConstant       int
	liveEstimate       int
	accountAt          *types.Account
	accountIDAt        *types.Account
	rewardAt           *tronapi.RewardInfo
	delegatedAt        []*tronapi.DelegatedResourceInfo
	delegIndexAt       *tronapi.DelegationIndexInfo
	assetIDAt          *contractpb.AssetIssueContract
	assetNameAt        *contractpb.AssetIssueContract
	assetListAt        []*contractpb.AssetIssueContract
	assetPageAt        []*contractpb.AssetIssueContract
	marketOrderAt      *corepb.MarketOrder
	marketOrdersAt     []*corepb.MarketOrder
	marketPriceAt      *corepb.MarketPriceList
	exchangesAt        []*corepb.Exchange
	canDelegateAt      *tronapi.CanDelegateInfo
	availableAt        *tronapi.AvailableUnfreezeCountInfo
	canWithdrawAt      *tronapi.CanWithdrawUnfreezeInfo
	burnAt             int64
	bandwidthAt        string
	energyAt           string
	witnessesAt        []*tronapi.WitnessInfo
	brokerageAt        int64
	constantAt         *tronapi.TriggerResult
	estimateAt         int64
	txAt               *corepb.Transaction
	txInfoAt           *corepb.TransactionInfo
	txErr              error
	txInfoErr          error
}

func (b *solidTestBackend) SolidifiedBlockNum() uint64 { return b.solidNum }

func (b *solidTestBackend) GetBlockByNumber(n uint64) (*types.Block, error) {
	b.blockNumCalls++
	b.lastNumQueried = n
	return b.testBackend.GetBlockByNumber(n)
}

func (b *solidTestBackend) GetBlockByHash(h common.Hash) (*types.Block, error) {
	b.hashCalls++
	b.lastHashQueried = h
	return b.testBackend.GetBlockByHash(h)
}

func (b *solidTestBackend) GetTransactionBlockNumByID(hash common.Hash) (uint64, bool, error) {
	b.txBlockCalls++
	return b.txBlockNum, b.txBlockOK, nil
}

func (b *solidTestBackend) GetTransactionByID(hash common.Hash) (*corepb.Transaction, error) {
	b.txCalls++
	if b.txErr != nil {
		return nil, b.txErr
	}
	if b.txAt != nil {
		return b.txAt, nil
	}
	return b.testBackend.GetTransactionByID(hash)
}

func (b *solidTestBackend) GetTransactionInfoByID(hash common.Hash) (*corepb.TransactionInfo, error) {
	b.txInfoCalls++
	if b.txInfoErr != nil {
		return nil, b.txInfoErr
	}
	if b.txInfoAt != nil {
		return b.txInfoAt, nil
	}
	return b.testBackend.GetTransactionInfoByID(hash)
}

func (b *solidTestBackend) GetAccount(addr common.Address) (*types.Account, error) {
	b.liveAccountCalls++
	return b.testBackend.GetAccount(addr)
}

func (b *solidTestBackend) GetAccountAt(addr common.Address, blockNum uint64) (*types.Account, error) {
	b.lastAccountAt = blockNum
	if b.accountAt != nil {
		return b.accountAt, nil
	}
	return b.testBackend.GetAccountAt(addr, blockNum)
}

func (b *solidTestBackend) GetAccountById(accountID []byte) (*types.Account, error) {
	b.liveAccountIDCalls++
	return b.testBackend.GetAccountById(accountID)
}

func (b *solidTestBackend) GetAccountByIdAt(accountID []byte, blockNum uint64) (*types.Account, error) {
	b.lastAccountIDAt = blockNum
	if b.accountIDAt != nil {
		return b.accountIDAt, nil
	}
	return b.testBackend.GetAccountByIdAt(accountID, blockNum)
}

func (b *solidTestBackend) GetReward(addr common.Address) (*tronapi.RewardInfo, error) {
	b.liveRewardCalls++
	return b.testBackend.GetReward(addr)
}

func (b *solidTestBackend) GetRewardAt(addr common.Address, blockNum uint64) (*tronapi.RewardInfo, error) {
	b.lastRewardAt = blockNum
	if b.rewardAt != nil {
		return b.rewardAt, nil
	}
	return b.testBackend.GetRewardAt(addr, blockNum)
}

func (b *solidTestBackend) GetDelegatedResourceV2(from, to common.Address) ([]*tronapi.DelegatedResourceInfo, error) {
	b.liveDelegCalls++
	return b.testBackend.GetDelegatedResourceV2(from, to)
}

func (b *solidTestBackend) GetDelegatedResourceV2At(from, to common.Address, blockNum uint64) ([]*tronapi.DelegatedResourceInfo, error) {
	b.lastDelegatedAt = blockNum
	if b.delegatedAt != nil {
		return b.delegatedAt, nil
	}
	return b.testBackend.GetDelegatedResourceV2At(from, to, blockNum)
}

func (b *solidTestBackend) GetDelegatedResourceAccountIndexV2(addr common.Address) (*tronapi.DelegationIndexInfo, error) {
	b.liveIndexCalls++
	return b.testBackend.GetDelegatedResourceAccountIndexV2(addr)
}

func (b *solidTestBackend) GetDelegatedResourceAccountIndexV2At(addr common.Address, blockNum uint64) (*tronapi.DelegationIndexInfo, error) {
	b.lastDelegIndexAt = blockNum
	if b.delegIndexAt != nil {
		return b.delegIndexAt, nil
	}
	return b.testBackend.GetDelegatedResourceAccountIndexV2At(addr, blockNum)
}

func (b *solidTestBackend) GetAssetIssueByID(id int64) *contractpb.AssetIssueContract {
	b.liveAssetID++
	return b.testBackend.GetAssetIssueByID(id)
}

func (b *solidTestBackend) GetAssetIssueByIDAt(id int64, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	b.lastAssetIDAt = blockNum
	if b.assetIDAt != nil {
		return b.assetIDAt, nil
	}
	return b.testBackend.GetAssetIssueByIDAt(id, blockNum)
}

func (b *solidTestBackend) GetAssetIssueByName(name []byte) *contractpb.AssetIssueContract {
	b.liveAssetName++
	return b.testBackend.GetAssetIssueByName(name)
}

func (b *solidTestBackend) GetAssetIssueByNameAt(name []byte, blockNum uint64) (*contractpb.AssetIssueContract, error) {
	b.lastAssetNameAt = blockNum
	if b.assetNameAt != nil {
		return b.assetNameAt, nil
	}
	return b.testBackend.GetAssetIssueByNameAt(name, blockNum)
}

func (b *solidTestBackend) GetAssetIssueList() []*contractpb.AssetIssueContract {
	b.liveAssetList++
	return b.testBackend.GetAssetIssueList()
}

func (b *solidTestBackend) GetAssetIssueListAt(blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	b.lastAssetListAt = blockNum
	if b.assetListAt != nil {
		return b.assetListAt, nil
	}
	return b.testBackend.GetAssetIssueListAt(blockNum)
}

func (b *solidTestBackend) GetAssetIssueListPaginated(offset, limit int) []*contractpb.AssetIssueContract {
	b.liveAssetPage++
	return b.testBackend.GetAssetIssueListPaginated(offset, limit)
}

func (b *solidTestBackend) GetAssetIssueListPaginatedAt(offset, limit int, blockNum uint64) ([]*contractpb.AssetIssueContract, error) {
	b.lastAssetPageAt = blockNum
	if b.assetPageAt != nil {
		return b.assetPageAt, nil
	}
	return b.testBackend.GetAssetIssueListPaginatedAt(offset, limit, blockNum)
}

func (b *solidTestBackend) GetMarketOrderByID(orderID []byte) *corepb.MarketOrder {
	b.liveMarketOrder++
	return b.testBackend.GetMarketOrderByID(orderID)
}

func (b *solidTestBackend) GetMarketOrderByIDAt(orderID []byte, blockNum uint64) (*corepb.MarketOrder, error) {
	b.lastMarketOrderAt = blockNum
	if b.marketOrderAt != nil {
		return b.marketOrderAt, nil
	}
	return b.testBackend.GetMarketOrderByIDAt(orderID, blockNum)
}

func (b *solidTestBackend) GetMarketOrdersByAccount(addr common.Address) []*corepb.MarketOrder {
	b.liveMarketOrders++
	return b.testBackend.GetMarketOrdersByAccount(addr)
}

func (b *solidTestBackend) GetMarketOrdersByAccountAt(addr common.Address, blockNum uint64) ([]*corepb.MarketOrder, error) {
	b.lastMarketOrdersAt = blockNum
	if b.marketOrdersAt != nil {
		return b.marketOrdersAt, nil
	}
	return b.testBackend.GetMarketOrdersByAccountAt(addr, blockNum)
}

func (b *solidTestBackend) GetMarketPriceByPair(sellTokenID, buyTokenID []byte) *corepb.MarketPriceList {
	b.liveMarketPrice++
	return b.testBackend.GetMarketPriceByPair(sellTokenID, buyTokenID)
}

func (b *solidTestBackend) GetMarketPriceByPairAt(sellTokenID, buyTokenID []byte, blockNum uint64) (*corepb.MarketPriceList, error) {
	b.lastMarketPriceAt = blockNum
	if b.marketPriceAt != nil {
		return b.marketPriceAt, nil
	}
	return b.testBackend.GetMarketPriceByPairAt(sellTokenID, buyTokenID, blockNum)
}

func (b *solidTestBackend) ListExchanges() ([]*corepb.Exchange, error) {
	b.liveExchanges++
	return b.testBackend.ListExchanges()
}

func (b *solidTestBackend) ListExchangesAt(blockNum uint64) ([]*corepb.Exchange, error) {
	b.lastExchangesAt = blockNum
	if b.exchangesAt != nil {
		return b.exchangesAt, nil
	}
	return b.testBackend.ListExchangesAt(blockNum)
}

func (b *solidTestBackend) CanDelegateResource(addr common.Address, amount int64, resource corepb.ResourceCode) (*tronapi.CanDelegateInfo, error) {
	b.liveCanDelegate++
	return b.testBackend.CanDelegateResource(addr, amount, resource)
}

func (b *solidTestBackend) CanDelegateResourceAt(addr common.Address, amount int64, resource corepb.ResourceCode, blockNum uint64) (*tronapi.CanDelegateInfo, error) {
	b.lastCanDelegateAt = blockNum
	if b.canDelegateAt != nil {
		return b.canDelegateAt, nil
	}
	return b.testBackend.CanDelegateResourceAt(addr, amount, resource, blockNum)
}

func (b *solidTestBackend) GetAvailableUnfreezeCount(addr common.Address) (*tronapi.AvailableUnfreezeCountInfo, error) {
	b.liveAvailable++
	return b.testBackend.GetAvailableUnfreezeCount(addr)
}

func (b *solidTestBackend) GetAvailableUnfreezeCountAt(addr common.Address, blockNum uint64) (*tronapi.AvailableUnfreezeCountInfo, error) {
	b.lastAvailableAt = blockNum
	if b.availableAt != nil {
		return b.availableAt, nil
	}
	return b.testBackend.GetAvailableUnfreezeCountAt(addr, blockNum)
}

func (b *solidTestBackend) GetCanWithdrawUnfreezeAmount(addr common.Address, timestamp int64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	b.liveCanWithdraw++
	return b.testBackend.GetCanWithdrawUnfreezeAmount(addr, timestamp)
}

func (b *solidTestBackend) GetCanWithdrawUnfreezeAmountAt(addr common.Address, timestamp int64, blockNum uint64) (*tronapi.CanWithdrawUnfreezeInfo, error) {
	b.lastCanWithdrawAt = blockNum
	if b.canWithdrawAt != nil {
		return b.canWithdrawAt, nil
	}
	return b.testBackend.GetCanWithdrawUnfreezeAmountAt(addr, timestamp, blockNum)
}

func (b *solidTestBackend) GetBurnTrx() int64 {
	b.liveBurn++
	return b.testBackend.GetBurnTrx()
}

func (b *solidTestBackend) GetBurnTrxAt(blockNum uint64) (int64, error) {
	b.lastBurnAt = blockNum
	if b.burnAt != 0 {
		return b.burnAt, nil
	}
	return b.testBackend.GetBurnTrxAt(blockNum)
}

func (b *solidTestBackend) GetBandwidthPrices() string {
	b.liveBandwidth++
	return b.testBackend.GetBandwidthPrices()
}

func (b *solidTestBackend) GetBandwidthPricesAt(blockNum uint64) (string, error) {
	b.lastBandwidthAt = blockNum
	if b.bandwidthAt != "" {
		return b.bandwidthAt, nil
	}
	return b.testBackend.GetBandwidthPricesAt(blockNum)
}

func (b *solidTestBackend) GetEnergyPrices() string {
	b.liveEnergy++
	return b.testBackend.GetEnergyPrices()
}

func (b *solidTestBackend) GetEnergyPricesAt(blockNum uint64) (string, error) {
	b.lastEnergyAt = blockNum
	if b.energyAt != "" {
		return b.energyAt, nil
	}
	return b.testBackend.GetEnergyPricesAt(blockNum)
}

func (b *solidTestBackend) ListWitnesses() ([]*tronapi.WitnessInfo, error) {
	b.liveWitnesses++
	return b.testBackend.ListWitnesses()
}

func (b *solidTestBackend) ListWitnessesAt(blockNum uint64) ([]*tronapi.WitnessInfo, error) {
	b.lastWitnessesAt = blockNum
	if b.witnessesAt != nil {
		return b.witnessesAt, nil
	}
	return b.testBackend.ListWitnessesAt(blockNum)
}

func (b *solidTestBackend) GetBrokerageInfo(addr common.Address) int64 {
	b.liveBrokerage++
	return b.testBackend.GetBrokerageInfo(addr)
}

func (b *solidTestBackend) GetBrokerageInfoAt(addr common.Address, blockNum uint64) (int64, error) {
	b.lastBrokerageAt = blockNum
	if b.brokerageAt != 0 {
		return b.brokerageAt, nil
	}
	return b.testBackend.GetBrokerageInfoAt(addr, blockNum)
}

func (b *solidTestBackend) TriggerConstantContract(owner, contract common.Address, data []byte, energyLimit int64) (*tronapi.TriggerResult, error) {
	b.liveConstant++
	return b.testBackend.TriggerConstantContract(owner, contract, data, energyLimit)
}

func (b *solidTestBackend) TriggerConstantContractAt(owner, contract common.Address, data []byte, energyLimit int64, blockNum uint64) (*tronapi.TriggerResult, error) {
	b.lastConstantAt = blockNum
	if b.constantAt != nil {
		return b.constantAt, nil
	}
	return b.testBackend.TriggerConstantContractAt(owner, contract, data, energyLimit, blockNum)
}

func (b *solidTestBackend) EstimateEnergy(owner, contract common.Address, data []byte) (int64, error) {
	b.liveEstimate++
	return b.testBackend.EstimateEnergy(owner, contract, data)
}

func (b *solidTestBackend) EstimateEnergyAt(owner, contract common.Address, data []byte, blockNum uint64) (int64, error) {
	b.lastEstimateAt = blockNum
	if b.estimateAt != 0 {
		return b.estimateAt, nil
	}
	return b.testBackend.EstimateEnergyAt(owner, contract, data, blockNum)
}

func newSolidityClient(t *testing.T, backend tronapi.Backend) apipb.WalletSolidityClient {
	t.Helper()
	lis := bufconn.Listen(bufSize)
	gs := grpc.NewServer()
	apipb.RegisterWalletSolidityServer(gs, grpcapi.NewSolidityServer(backend))
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
	return apipb.NewWalletSolidityClient(conn)
}

// TestSolidity_GetNowBlock_NoSolidBlock checks that GetNowBlock returns NotFound
// when the solid block does not exist in the stub chain.
func TestSolidity_GetNowBlock_NoSolidBlock(t *testing.T) {
	backend := &solidTestBackend{solidNum: 0} // stub GetBlockByNumber returns b.block (nil)
	client := newSolidityClient(t, backend)

	_, err := client.GetNowBlock(context.Background(), &apipb.EmptyMessage{})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound, got %v", err)
	}
}

// TestSolidity_GetNowBlock_ReturnsSolidBlock verifies that GetNowBlock returns
// the block at solidNum, not the current head.
func TestSolidity_GetNowBlock_ReturnsSolidBlock(t *testing.T) {
	solidBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 10},
		},
	})
	backend := &solidTestBackend{
		testBackend: testBackend{block: solidBlock},
		solidNum:    10,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetNowBlock(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetNowBlock: %v", err)
	}
	if resp.GetBlockHeader().GetRawData().GetNumber() != 10 {
		t.Fatalf("expected block 10, got %d", resp.GetBlockHeader().GetRawData().GetNumber())
	}
	// Verify the server actually looked up solidNum, not some other block number.
	if backend.lastNumQueried != backend.solidNum {
		t.Fatalf("expected lookup of solidNum %d, got %d", backend.solidNum, backend.lastNumQueried)
	}
}

// TestSolidity_GetBlockByNum_AboveSolid verifies that requesting a block
// number above the solid boundary returns NotFound.
func TestSolidity_GetBlockByNum_AboveSolid(t *testing.T) {
	backend := &solidTestBackend{solidNum: 5}
	client := newSolidityClient(t, backend)

	_, err := client.GetBlockByNum(context.Background(), &apipb.NumberMessage{Num: 10})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound for block above solid, got %v", err)
	}
}

func TestSolidity_GetBlockByNumSurfacesBackendError(t *testing.T) {
	backendErr := errors.New("rawdb: block 4 decode: corrupt")
	backend := &solidTestBackend{
		testBackend: testBackend{blockErr: backendErr},
		solidNum:    5,
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetBlockByNum(context.Background(), &apipb.NumberMessage{Num: 4})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal for backend block read error, got %v", err)
	}
}

func TestSolidity_GetBlockLatestUsesSolidBound(t *testing.T) {
	solidBlock := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 10},
		},
	})
	backend := &solidTestBackend{
		testBackend: testBackend{block: solidBlock},
		solidNum:    10,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetBlock(context.Background(), &apipb.BlockReq{IdOrNum: "latest", Detail: true})
	if err != nil {
		t.Fatalf("GetBlock latest: %v", err)
	}
	if resp.GetBlockHeader().GetRawData().GetNumber() != 10 {
		t.Fatalf("block number = %d, want 10", resp.GetBlockHeader().GetRawData().GetNumber())
	}
	if backend.lastNumQueried != 10 {
		t.Fatalf("queried block %d, want solid block 10", backend.lastNumQueried)
	}
}

func TestSolidity_GetBlockNumberRejectsAboveSolidBeforeBackend(t *testing.T) {
	backend := &solidTestBackend{solidNum: 5}
	client := newSolidityClient(t, backend)

	_, err := client.GetBlock(context.Background(), &apipb.BlockReq{IdOrNum: "10", Detail: true})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound for block above solid, got %v", err)
	}
	if backend.blockNumCalls != 0 {
		t.Fatalf("GetBlock queried backend %d times for unsolidified number, want 0", backend.blockNumCalls)
	}
}

func TestSolidity_GetBlockIDRejectsAboveSolidBeforeBackend(t *testing.T) {
	hash := solidityBlockIDWithNumber(10)
	backend := &solidTestBackend{solidNum: 5}
	client := newSolidityClient(t, backend)

	_, err := client.GetBlock(context.Background(), &apipb.BlockReq{IdOrNum: hash.Hex(), Detail: true})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound for block id above solid, got %v", err)
	}
	if backend.hashCalls != 0 {
		t.Fatalf("GetBlock queried hash backend %d times for unsolidified id, want 0", backend.hashCalls)
	}
}

func TestSolidity_GetBlockIDWithinSolidUsesHashLookup(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 5},
		},
	})
	backend := &solidTestBackend{
		testBackend: testBackend{block: block},
		solidNum:    5,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetBlock(context.Background(), &apipb.BlockReq{IdOrNum: block.Hash().Hex(), Detail: true})
	if err != nil {
		t.Fatalf("GetBlock by id: %v", err)
	}
	if resp.GetBlockHeader().GetRawData().GetNumber() != 5 {
		t.Fatalf("block number = %d, want 5", resp.GetBlockHeader().GetRawData().GetNumber())
	}
	if backend.hashCalls != 1 || backend.lastHashQueried != block.Hash() {
		t.Fatalf("hash lookup calls/hash = %d/%s, want 1/%s", backend.hashCalls, backend.lastHashQueried.Hex(), block.Hash().Hex())
	}
}

func TestSolidity_GetBlockDetailFalseOmitsTransactionBody(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 5},
		},
		Transactions: []*corepb.Transaction{{
			RawData: &corepb.TransactionRaw{Timestamp: 9},
		}},
	})
	backend := &solidTestBackend{
		testBackend: testBackend{block: block},
		solidNum:    5,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetBlock(context.Background(), &apipb.BlockReq{IdOrNum: "5"})
	if err != nil {
		t.Fatalf("GetBlock detail=false: %v", err)
	}
	txs := resp.GetTransactions()
	if len(txs) != 1 {
		t.Fatalf("transaction count = %d, want 1", len(txs))
	}
	if txs[0].GetTransaction() != nil {
		t.Fatal("detail=false returned full transaction body")
	}
	if len(txs[0].GetTxid()) != common.HashLength {
		t.Fatalf("txid length = %d, want %d", len(txs[0].GetTxid()), common.HashLength)
	}
}

func TestSolidity_GetTransactionCountByBlockNum_AboveSolid(t *testing.T) {
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{Number: 10},
		},
	})
	backend := &solidTestBackend{
		testBackend: testBackend{block: block},
		solidNum:    5,
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetTransactionCountByBlockNum(context.Background(), &apipb.NumberMessage{Num: 10})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound for tx count above solid, got %v", err)
	}
	if backend.lastNumQueried != 0 {
		t.Fatalf("backend queried block %d for unsolidified tx count, want no lookup", backend.lastNumQueried)
	}
}

func TestSolidity_GetTransactionByIdRejectsAboveSolidBeforeBackend(t *testing.T) {
	hash := solidityTestHash(0x01)
	backend := &solidTestBackend{
		solidNum:   5,
		txBlockNum: 10,
		txBlockOK:  true,
		txAt:       &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 1}},
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetTransactionById(context.Background(), &apipb.BytesMessage{Value: hash[:]})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound for tx above solid, got %v", err)
	}
	if backend.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", backend.txBlockCalls)
	}
	if backend.txCalls != 0 {
		t.Fatalf("GetTransactionByID called %d times for unsolidified tx, want 0", backend.txCalls)
	}
}

func TestSolidity_GetTransactionByIdWithinSolidReadsBackend(t *testing.T) {
	hash := solidityTestHash(0x02)
	backend := &solidTestBackend{
		solidNum:   10,
		txBlockNum: 10,
		txBlockOK:  true,
		txAt:       &corepb.Transaction{RawData: &corepb.TransactionRaw{Timestamp: 2}},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetTransactionById(context.Background(), &apipb.BytesMessage{Value: hash[:]})
	if err != nil {
		t.Fatalf("GetTransactionById: %v", err)
	}
	if resp.GetRawData().GetTimestamp() != 2 {
		t.Fatalf("GetTransactionById timestamp = %d, want 2", resp.GetRawData().GetTimestamp())
	}
	if backend.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", backend.txBlockCalls)
	}
	if backend.txCalls != 1 {
		t.Fatalf("GetTransactionByID called %d times, want 1", backend.txCalls)
	}
}

func TestSolidity_GetTransactionByIdSurfacesBackendError(t *testing.T) {
	hash := solidityTestHash(0x12)
	backendErr := errors.New("rawdb: block 1 decode: corrupt")
	backend := &solidTestBackend{
		solidNum:   10,
		txBlockNum: 10,
		txBlockOK:  true,
		txErr:      backendErr,
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetTransactionById(context.Background(), &apipb.BytesMessage{Value: hash[:]})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal for backend tx read error, got %v", err)
	}
	if backend.txCalls != 1 {
		t.Fatalf("GetTransactionByID called %d times, want 1", backend.txCalls)
	}
}

func TestSolidity_GetTransactionInfoByIdRejectsAboveSolidBeforeBackend(t *testing.T) {
	hash := solidityTestHash(0x03)
	backend := &solidTestBackend{
		solidNum:   7,
		txBlockNum: 8,
		txBlockOK:  true,
		txInfoAt:   &corepb.TransactionInfo{Id: hash[:]},
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetTransactionInfoById(context.Background(), &apipb.BytesMessage{Value: hash[:]})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("want NotFound for tx info above solid, got %v", err)
	}
	if backend.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", backend.txBlockCalls)
	}
	if backend.txInfoCalls != 0 {
		t.Fatalf("GetTransactionInfoByID called %d times for unsolidified tx info, want 0", backend.txInfoCalls)
	}
}

func TestSolidity_GetTransactionInfoByIdWithinSolidReadsBackend(t *testing.T) {
	hash := solidityTestHash(0x04)
	backend := &solidTestBackend{
		solidNum:   9,
		txBlockNum: 9,
		txBlockOK:  true,
		txInfoAt:   &corepb.TransactionInfo{Id: hash[:], BlockNumber: 9},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetTransactionInfoById(context.Background(), &apipb.BytesMessage{Value: hash[:]})
	if err != nil {
		t.Fatalf("GetTransactionInfoById: %v", err)
	}
	if resp.GetBlockNumber() != 9 {
		t.Fatalf("GetTransactionInfoById block = %d, want 9", resp.GetBlockNumber())
	}
	if backend.txBlockCalls != 1 {
		t.Fatalf("GetTransactionBlockNumByID called %d times, want 1", backend.txBlockCalls)
	}
	if backend.txInfoCalls != 1 {
		t.Fatalf("GetTransactionInfoByID called %d times, want 1", backend.txInfoCalls)
	}
}

func TestSolidity_GetTransactionInfoByIdSurfacesBackendError(t *testing.T) {
	hash := solidityTestHash(0x13)
	backendErr := errors.New("rawdb: cold tx lookup corrupt")
	backend := &solidTestBackend{
		solidNum:   9,
		txBlockNum: 9,
		txBlockOK:  true,
		txInfoErr:  backendErr,
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetTransactionInfoById(context.Background(), &apipb.BytesMessage{Value: hash[:]})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal for backend tx-info read error, got %v", err)
	}
	if backend.txInfoCalls != 1 {
		t.Fatalf("GetTransactionInfoByID called %d times, want 1", backend.txInfoCalls)
	}
}

// TestSolidity_GetAccount_ReturnsEmpty verifies GetAccount returns an empty account
// when the stub has no account.
func TestSolidity_GetAccount_ReturnsEmpty(t *testing.T) {
	client := newSolidityClient(t, &solidTestBackend{})

	resp, err := client.GetAccount(context.Background(), &corepb.Account{
		Address: make([]byte, 21),
	})
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestSolidity_GetAccountUsesSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x11)
	accountAt := types.NewAccount(common.BytesToAddress(addr), corepb.AccountType_Normal)
	accountAt.SetBalance(200)
	backend := &solidTestBackend{
		solidNum:  42,
		accountAt: accountAt,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetAccount(context.Background(), &corepb.Account{Address: addr})
	if err != nil {
		t.Fatalf("GetAccount: %v", err)
	}
	if resp.GetBalance() != 200 {
		t.Fatalf("GetAccount balance = %d, want 200", resp.GetBalance())
	}
	if backend.lastAccountAt != 42 {
		t.Fatalf("GetAccountAt block = %d, want solid block 42", backend.lastAccountAt)
	}
	if backend.liveAccountCalls != 0 {
		t.Fatalf("live GetAccount called %d times, want 0", backend.liveAccountCalls)
	}
}

func TestSolidity_GetAccountSurfacesBackendError(t *testing.T) {
	addr := solidityTestAddress(0x15)
	backendErr := errors.New("state history: cold account segment corrupt")
	backend := &solidTestBackend{
		testBackend: testBackend{accountAtErr: backendErr},
		solidNum:    42,
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetAccount(context.Background(), &corepb.Account{Address: addr})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal for backend account read error, got %v", err)
	}
	if backend.lastAccountAt != 42 {
		t.Fatalf("GetAccountAt block = %d, want solid block 42", backend.lastAccountAt)
	}
}

func TestSolidity_GetAccountByIdAddressUsesSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x22)
	accountAt := types.NewAccount(common.BytesToAddress(addr), corepb.AccountType_Normal)
	accountAt.SetBalance(300)
	backend := &solidTestBackend{
		solidNum:  77,
		accountAt: accountAt,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetAccountById(context.Background(), &corepb.Account{Address: addr})
	if err != nil {
		t.Fatalf("GetAccountById: %v", err)
	}
	if resp.GetBalance() != 300 {
		t.Fatalf("GetAccountById balance = %d, want 300", resp.GetBalance())
	}
	if backend.lastAccountAt != 77 {
		t.Fatalf("GetAccountAt block = %d, want solid block 77", backend.lastAccountAt)
	}
	if backend.liveAccountCalls != 0 {
		t.Fatalf("live GetAccount called %d times, want 0", backend.liveAccountCalls)
	}
}

func TestSolidity_GetAccountByIdAccountIDUsesSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x24)
	accountAt := types.NewAccount(common.BytesToAddress(addr), corepb.AccountType_Normal)
	accountAt.SetBalance(350)
	backend := &solidTestBackend{
		solidNum:    79,
		accountIDAt: accountAt,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetAccountById(context.Background(), &corepb.Account{AccountId: []byte("user1234")})
	if err != nil {
		t.Fatalf("GetAccountById: %v", err)
	}
	if resp.GetBalance() != 350 {
		t.Fatalf("GetAccountById balance = %d, want 350", resp.GetBalance())
	}
	if backend.lastAccountIDAt != 79 {
		t.Fatalf("GetAccountByIdAt block = %d, want solid block 79", backend.lastAccountIDAt)
	}
	if backend.liveAccountIDCalls != 0 {
		t.Fatalf("live GetAccountById called %d times, want 0", backend.liveAccountIDCalls)
	}
}

func TestSolidity_GetAccountByIdSurfacesBackendError(t *testing.T) {
	backendErr := errors.New("state history: cold account-id index corrupt")
	backend := &solidTestBackend{
		testBackend: testBackend{accountIDAtErr: backendErr},
		solidNum:    79,
	}
	client := newSolidityClient(t, backend)

	_, err := client.GetAccountById(context.Background(), &corepb.Account{AccountId: []byte("user1234")})
	if status.Code(err) != codes.Internal {
		t.Fatalf("want Internal for backend account-id read error, got %v", err)
	}
	if backend.lastAccountIDAt != 79 {
		t.Fatalf("GetAccountByIdAt block = %d, want solid block 79", backend.lastAccountIDAt)
	}
}

func TestSolidity_GetRewardInfoUsesSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x33)
	backend := &solidTestBackend{
		solidNum: 88,
		rewardAt: &tronapi.RewardInfo{
			Reward: 456,
		},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetRewardInfo(context.Background(), &apipb.BytesMessage{Value: addr})
	if err != nil {
		t.Fatalf("GetRewardInfo: %v", err)
	}
	if resp.GetNum() != 456 {
		t.Fatalf("GetRewardInfo = %d, want 456", resp.GetNum())
	}
	if backend.lastRewardAt != 88 {
		t.Fatalf("GetRewardAt block = %d, want solid block 88", backend.lastRewardAt)
	}
	if backend.liveRewardCalls != 0 {
		t.Fatalf("live GetReward called %d times, want 0", backend.liveRewardCalls)
	}
}

func TestSolidity_GetBrokerageInfoUsesSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x34)
	backend := &solidTestBackend{
		solidNum:    89,
		brokerageAt: 77,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetBrokerageInfo(context.Background(), &apipb.BytesMessage{Value: addr})
	if err != nil {
		t.Fatalf("GetBrokerageInfo: %v", err)
	}
	if resp.GetNum() != 77 {
		t.Fatalf("GetBrokerageInfo = %d, want 77", resp.GetNum())
	}
	if backend.lastBrokerageAt != 89 {
		t.Fatalf("GetBrokerageInfoAt block = %d, want solid block 89", backend.lastBrokerageAt)
	}
	if backend.liveBrokerage != 0 {
		t.Fatalf("live GetBrokerageInfo called %d times, want 0", backend.liveBrokerage)
	}
}

func TestSolidity_TriggerConstantContractUsesSolidBoundArchivePath(t *testing.T) {
	backend := &solidTestBackend{
		solidNum:   91,
		constantAt: &tronapi.TriggerResult{Result: []byte("bound"), EnergyUsed: 123},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.TriggerConstantContract(context.Background(), &contractpb.TriggerSmartContract{
		OwnerAddress:    solidityTestAddress(0x35),
		ContractAddress: solidityTestAddress(0x36),
		Data:            []byte{0x01},
	})
	if err != nil {
		t.Fatalf("TriggerConstantContract: %v", err)
	}
	if resp.GetEnergyUsed() != 123 || len(resp.GetConstantResult()) != 1 || string(resp.GetConstantResult()[0]) != "bound" {
		t.Fatalf("TriggerConstantContract = %+v, want solid-bound sentinel", resp)
	}
	if backend.lastConstantAt != 91 {
		t.Fatalf("TriggerConstantContractAt block = %d, want solid block 91", backend.lastConstantAt)
	}
	if backend.liveConstant != 0 {
		t.Fatalf("live TriggerConstantContract called %d times, want 0", backend.liveConstant)
	}
}

func TestSolidity_EstimateEnergyUsesSolidBoundArchivePath(t *testing.T) {
	backend := &solidTestBackend{
		solidNum:   92,
		estimateAt: 456,
	}
	client := newSolidityClient(t, backend)

	resp, err := client.EstimateEnergy(context.Background(), &contractpb.TriggerSmartContract{
		OwnerAddress:    solidityTestAddress(0x37),
		ContractAddress: solidityTestAddress(0x38),
		Data:            []byte{0x02},
	})
	if err != nil {
		t.Fatalf("EstimateEnergy: %v", err)
	}
	if resp.GetEnergyRequired() != 456 || !resp.GetResult().GetResult() {
		t.Fatalf("EstimateEnergy = %+v, want solid-bound sentinel", resp)
	}
	if backend.lastEstimateAt != 92 {
		t.Fatalf("EstimateEnergyAt block = %d, want solid block 92", backend.lastEstimateAt)
	}
	if backend.liveEstimate != 0 {
		t.Fatalf("live EstimateEnergy called %d times, want 0", backend.liveEstimate)
	}
}

func TestSolidity_GetDelegatedResourceV2UsesSolidBoundArchivePath(t *testing.T) {
	from := solidityTestAddress(0x44)
	to := solidityTestAddress(0x55)
	backend := &solidTestBackend{
		solidNum: 66,
		delegatedAt: []*tronapi.DelegatedResourceInfo{{
			FrozenBalanceForEnergy:    700,
			ExpireTimeForEnergy:       800,
			FrozenBalanceForBandwidth: 900,
		}},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetDelegatedResourceV2(context.Background(), &apipb.DelegatedResourceMessage{
		FromAddress: from,
		ToAddress:   to,
	})
	if err != nil {
		t.Fatalf("GetDelegatedResourceV2: %v", err)
	}
	if len(resp.GetDelegatedResource()) != 1 || resp.GetDelegatedResource()[0].GetFrozenBalanceForEnergy() != 700 {
		t.Fatalf("GetDelegatedResourceV2 = %+v, want solid-bound sentinel", resp.GetDelegatedResource())
	}
	if backend.lastDelegatedAt != 66 {
		t.Fatalf("GetDelegatedResourceV2At block = %d, want solid block 66", backend.lastDelegatedAt)
	}
	if backend.liveDelegCalls != 0 {
		t.Fatalf("live GetDelegatedResourceV2 called %d times, want 0", backend.liveDelegCalls)
	}
}

func TestSolidity_GetDelegatedResourceAccountIndexV2UsesSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x66)
	to := solidityTestAddress(0x77)
	backend := &solidTestBackend{
		solidNum: 77,
		delegIndexAt: &tronapi.DelegationIndexInfo{
			ToAddresses: []string{hex.EncodeToString(to)},
		},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.GetDelegatedResourceAccountIndexV2(context.Background(), &apipb.BytesMessage{Value: addr})
	if err != nil {
		t.Fatalf("GetDelegatedResourceAccountIndexV2: %v", err)
	}
	if len(resp.GetToAccounts()) != 1 || hex.EncodeToString(resp.GetToAccounts()[0]) != hex.EncodeToString(to) {
		t.Fatalf("GetDelegatedResourceAccountIndexV2 = %+v, want solid-bound sentinel %x", resp.GetToAccounts(), to)
	}
	if backend.lastDelegIndexAt != 77 {
		t.Fatalf("GetDelegatedResourceAccountIndexV2At block = %d, want solid block 77", backend.lastDelegIndexAt)
	}
	if backend.liveIndexCalls != 0 {
		t.Fatalf("live GetDelegatedResourceAccountIndexV2 called %d times, want 0", backend.liveIndexCalls)
	}
}

func TestSolidity_StakeResourceQueriesUseSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x78)
	backend := &solidTestBackend{
		solidNum:      87,
		canDelegateAt: &tronapi.CanDelegateInfo{CanDelegateSize: 700},
		availableAt:   &tronapi.AvailableUnfreezeCountInfo{Count: 29},
		canWithdrawAt: &tronapi.CanWithdrawUnfreezeInfo{Amount: 5000},
	}
	client := newSolidityClient(t, backend)

	maxSize, err := client.GetCanDelegatedMaxSize(context.Background(), &apipb.CanDelegatedMaxSizeRequestMessage{
		OwnerAddress: addr,
		Type:         int32(corepb.ResourceCode_BANDWIDTH),
	})
	if err != nil {
		t.Fatalf("GetCanDelegatedMaxSize: %v", err)
	}
	if maxSize.GetMaxSize() != 700 {
		t.Fatalf("GetCanDelegatedMaxSize = %d, want solid-bound sentinel 700", maxSize.GetMaxSize())
	}
	if backend.lastCanDelegateAt != 87 {
		t.Fatalf("CanDelegateResourceAt block = %d, want solid block 87", backend.lastCanDelegateAt)
	}
	if backend.liveCanDelegate != 0 {
		t.Fatalf("live CanDelegateResource called %d times, want 0", backend.liveCanDelegate)
	}

	available, err := client.GetAvailableUnfreezeCount(context.Background(), &apipb.GetAvailableUnfreezeCountRequestMessage{
		OwnerAddress: addr,
	})
	if err != nil {
		t.Fatalf("GetAvailableUnfreezeCount: %v", err)
	}
	if available.GetCount() != 29 {
		t.Fatalf("GetAvailableUnfreezeCount = %d, want solid-bound sentinel 29", available.GetCount())
	}
	if backend.lastAvailableAt != 87 {
		t.Fatalf("GetAvailableUnfreezeCountAt block = %d, want solid block 87", backend.lastAvailableAt)
	}
	if backend.liveAvailable != 0 {
		t.Fatalf("live GetAvailableUnfreezeCount called %d times, want 0", backend.liveAvailable)
	}

	withdrawable, err := client.GetCanWithdrawUnfreezeAmount(context.Background(), &apipb.CanWithdrawUnfreezeAmountRequestMessage{
		OwnerAddress: addr,
		Timestamp:    12345,
	})
	if err != nil {
		t.Fatalf("GetCanWithdrawUnfreezeAmount: %v", err)
	}
	if withdrawable.GetAmount() != 5000 {
		t.Fatalf("GetCanWithdrawUnfreezeAmount = %d, want solid-bound sentinel 5000", withdrawable.GetAmount())
	}
	if backend.lastCanWithdrawAt != 87 {
		t.Fatalf("GetCanWithdrawUnfreezeAmountAt block = %d, want solid block 87", backend.lastCanWithdrawAt)
	}
	if backend.liveCanWithdraw != 0 {
		t.Fatalf("live GetCanWithdrawUnfreezeAmount called %d times, want 0", backend.liveCanWithdraw)
	}
}

func TestSolidity_AssetQueriesUseSolidBoundArchivePath(t *testing.T) {
	asset := func(id string, supply int64) *contractpb.AssetIssueContract {
		return &contractpb.AssetIssueContract{
			Id:           id,
			OwnerAddress: solidityTestAddress(0x81),
			Name:         []byte(id),
			TotalSupply:  supply,
		}
	}
	backend := &solidTestBackend{
		solidNum:    89,
		assetIDAt:   asset("bound-id", 9),
		assetNameAt: asset("bound-name", 99),
		assetListAt: []*contractpb.AssetIssueContract{
			asset("bound-list", 11),
		},
		assetPageAt: []*contractpb.AssetIssueContract{
			asset("bound-page", 12),
		},
	}
	client := newSolidityClient(t, backend)

	byID, err := client.GetAssetIssueById(context.Background(), &apipb.BytesMessage{Value: []byte("1000001")})
	if err != nil {
		t.Fatalf("GetAssetIssueById: %v", err)
	}
	if byID.GetId() != "bound-id" || byID.GetTotalSupply() != 9 {
		t.Fatalf("GetAssetIssueById = %+v, want solid-bound sentinel", byID)
	}
	if backend.lastAssetIDAt != 89 {
		t.Fatalf("GetAssetIssueByIDAt block = %d, want solid block 89", backend.lastAssetIDAt)
	}
	if backend.liveAssetID != 0 {
		t.Fatalf("live GetAssetIssueByID called %d times, want 0", backend.liveAssetID)
	}

	byName, err := client.GetAssetIssueByName(context.Background(), &apipb.BytesMessage{Value: []byte("TOKEN")})
	if err != nil {
		t.Fatalf("GetAssetIssueByName: %v", err)
	}
	if byName.GetId() != "bound-name" || byName.GetTotalSupply() != 99 {
		t.Fatalf("GetAssetIssueByName = %+v, want solid-bound sentinel", byName)
	}
	if backend.lastAssetNameAt != 89 {
		t.Fatalf("GetAssetIssueByNameAt block = %d, want solid block 89", backend.lastAssetNameAt)
	}
	if backend.liveAssetName != 0 {
		t.Fatalf("live GetAssetIssueByName called %d times, want 0", backend.liveAssetName)
	}

	list, err := client.GetAssetIssueList(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetAssetIssueList: %v", err)
	}
	if len(list.GetAssetIssue()) != 1 || list.GetAssetIssue()[0].GetId() != "bound-list" {
		t.Fatalf("GetAssetIssueList = %+v, want solid-bound sentinel", list.GetAssetIssue())
	}
	if backend.lastAssetListAt != 89 {
		t.Fatalf("GetAssetIssueListAt block = %d, want solid block 89", backend.lastAssetListAt)
	}
	if backend.liveAssetList != 0 {
		t.Fatalf("live GetAssetIssueList called %d times, want 0", backend.liveAssetList)
	}

	page, err := client.GetPaginatedAssetIssueList(context.Background(), &apipb.PaginatedMessage{Offset: 0, Limit: 10})
	if err != nil {
		t.Fatalf("GetPaginatedAssetIssueList: %v", err)
	}
	if len(page.GetAssetIssue()) != 1 || page.GetAssetIssue()[0].GetId() != "bound-page" {
		t.Fatalf("GetPaginatedAssetIssueList = %+v, want solid-bound sentinel", page.GetAssetIssue())
	}
	if backend.lastAssetPageAt != 89 {
		t.Fatalf("GetAssetIssueListPaginatedAt block = %d, want solid block 89", backend.lastAssetPageAt)
	}
	if backend.liveAssetPage != 0 {
		t.Fatalf("live GetAssetIssueListPaginated called %d times, want 0", backend.liveAssetPage)
	}
}

func TestSolidity_MarketQueriesUseSolidBoundArchivePath(t *testing.T) {
	addr := solidityTestAddress(0x88)
	backend := &solidTestBackend{
		solidNum: 91,
		marketOrderAt: &corepb.MarketOrder{
			OrderId:           []byte("order"),
			SellTokenQuantity: 9,
			BuyTokenQuantity:  99,
		},
		marketOrdersAt: []*corepb.MarketOrder{{
			OrderId:           []byte("account-order"),
			OwnerAddress:      addr,
			SellTokenQuantity: 11,
			BuyTokenQuantity:  111,
		}},
		marketPriceAt: &corepb.MarketPriceList{
			SellTokenId: []byte("sell"),
			BuyTokenId:  []byte("buy"),
			Prices:      []*corepb.MarketPrice{{SellTokenQuantity: 12, BuyTokenQuantity: 120}},
		},
	}
	client := newSolidityClient(t, backend)

	order, err := client.GetMarketOrderById(context.Background(), &apipb.BytesMessage{Value: []byte("order")})
	if err != nil {
		t.Fatalf("GetMarketOrderById: %v", err)
	}
	if order.GetSellTokenQuantity() != 9 || order.GetBuyTokenQuantity() != 99 {
		t.Fatalf("GetMarketOrderById = %+v, want solid-bound sentinel", order)
	}
	if backend.lastMarketOrderAt != 91 {
		t.Fatalf("GetMarketOrderByIDAt block = %d, want solid block 91", backend.lastMarketOrderAt)
	}
	if backend.liveMarketOrder != 0 {
		t.Fatalf("live GetMarketOrderByID called %d times, want 0", backend.liveMarketOrder)
	}

	orders, err := client.GetMarketOrderByAccount(context.Background(), &apipb.BytesMessage{Value: addr})
	if err != nil {
		t.Fatalf("GetMarketOrderByAccount: %v", err)
	}
	if len(orders.GetOrders()) != 1 || orders.GetOrders()[0].GetSellTokenQuantity() != 11 {
		t.Fatalf("GetMarketOrderByAccount = %+v, want solid-bound sentinel", orders.GetOrders())
	}
	if backend.lastMarketOrdersAt != 91 {
		t.Fatalf("GetMarketOrdersByAccountAt block = %d, want solid block 91", backend.lastMarketOrdersAt)
	}
	if backend.liveMarketOrders != 0 {
		t.Fatalf("live GetMarketOrdersByAccount called %d times, want 0", backend.liveMarketOrders)
	}

	price, err := client.GetMarketPriceByPair(context.Background(), &corepb.MarketOrderPair{
		SellTokenId: []byte("sell"),
		BuyTokenId:  []byte("buy"),
	})
	if err != nil {
		t.Fatalf("GetMarketPriceByPair: %v", err)
	}
	if len(price.GetPrices()) != 1 || price.GetPrices()[0].GetSellTokenQuantity() != 12 {
		t.Fatalf("GetMarketPriceByPair = %+v, want solid-bound sentinel", price.GetPrices())
	}
	if backend.lastMarketPriceAt != 91 {
		t.Fatalf("GetMarketPriceByPairAt block = %d, want solid block 91", backend.lastMarketPriceAt)
	}
	if backend.liveMarketPrice != 0 {
		t.Fatalf("live GetMarketPriceByPair called %d times, want 0", backend.liveMarketPrice)
	}
}

func TestSolidity_ListExchangesUsesSolidBoundArchivePath(t *testing.T) {
	backend := &solidTestBackend{
		solidNum: 93,
		exchangesAt: []*corepb.Exchange{{
			ExchangeId:         7,
			FirstTokenId:       []byte("solid"),
			FirstTokenBalance:  70,
			SecondTokenId:      []byte("_"),
			SecondTokenBalance: 700,
		}},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.ListExchanges(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("ListExchanges: %v", err)
	}
	if len(resp.GetExchanges()) != 1 || resp.GetExchanges()[0].GetExchangeId() != 7 || resp.GetExchanges()[0].GetFirstTokenBalance() != 70 {
		t.Fatalf("ListExchanges = %+v, want solid-bound sentinel", resp.GetExchanges())
	}
	if backend.lastExchangesAt != 93 {
		t.Fatalf("ListExchangesAt block = %d, want solid block 93", backend.lastExchangesAt)
	}
	if backend.liveExchanges != 0 {
		t.Fatalf("live ListExchanges called %d times, want 0", backend.liveExchanges)
	}
}

func TestSolidity_DynamicPropertyQueriesUseSolidBoundArchivePath(t *testing.T) {
	backend := &solidTestBackend{
		solidNum:    95,
		burnAt:      123456,
		bandwidthAt: "0:10,95:20",
		energyAt:    "0:100,95:200",
	}
	client := newSolidityClient(t, backend)

	burn, err := client.GetBurnTrx(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetBurnTrx: %v", err)
	}
	if burn.GetNum() != 123456 {
		t.Fatalf("GetBurnTrx = %d, want solid-bound sentinel 123456", burn.GetNum())
	}
	if backend.lastBurnAt != 95 {
		t.Fatalf("GetBurnTrxAt block = %d, want solid block 95", backend.lastBurnAt)
	}
	if backend.liveBurn != 0 {
		t.Fatalf("live GetBurnTrx called %d times, want 0", backend.liveBurn)
	}

	bandwidth, err := client.GetBandwidthPrices(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetBandwidthPrices: %v", err)
	}
	if bandwidth.GetPrices() != "0:10,95:20" {
		t.Fatalf("GetBandwidthPrices = %q, want solid-bound sentinel", bandwidth.GetPrices())
	}
	if backend.lastBandwidthAt != 95 {
		t.Fatalf("GetBandwidthPricesAt block = %d, want solid block 95", backend.lastBandwidthAt)
	}
	if backend.liveBandwidth != 0 {
		t.Fatalf("live GetBandwidthPrices called %d times, want 0", backend.liveBandwidth)
	}

	energy, err := client.GetEnergyPrices(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("GetEnergyPrices: %v", err)
	}
	if energy.GetPrices() != "0:100,95:200" {
		t.Fatalf("GetEnergyPrices = %q, want solid-bound sentinel", energy.GetPrices())
	}
	if backend.lastEnergyAt != 95 {
		t.Fatalf("GetEnergyPricesAt block = %d, want solid block 95", backend.lastEnergyAt)
	}
	if backend.liveEnergy != 0 {
		t.Fatalf("live GetEnergyPrices called %d times, want 0", backend.liveEnergy)
	}
}

// TestSolidity_ListWitnesses_Empty checks ListWitnesses with an empty stub.
func TestSolidity_ListWitnesses_Empty(t *testing.T) {
	client := newSolidityClient(t, &solidTestBackend{})

	resp, err := client.ListWitnesses(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("ListWitnesses: %v", err)
	}
	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestSolidity_ListWitnessesUsesSolidBoundArchivePath(t *testing.T) {
	addr := common.Address{0x41, 0x33}
	backend := &solidTestBackend{
		solidNum: 97,
		witnessesAt: []*tronapi.WitnessInfo{{
			Address:   hex.EncodeToString(addr.Bytes()),
			VoteCount: 77,
			URL:       "solid-witness",
			IsJobs:    true,
		}},
	}
	client := newSolidityClient(t, backend)

	resp, err := client.ListWitnesses(context.Background(), &apipb.EmptyMessage{})
	if err != nil {
		t.Fatalf("ListWitnesses: %v", err)
	}
	if len(resp.GetWitnesses()) != 1 {
		t.Fatalf("witness count = %d, want 1", len(resp.GetWitnesses()))
	}
	got := resp.GetWitnesses()[0]
	if string(got.GetUrl()) != "solid-witness" || got.GetVoteCount() != 77 || !got.GetIsJobs() {
		t.Fatalf("witness = %+v, want solid-bound sentinel", got)
	}
	if common.BytesToAddress(got.GetAddress()) != addr {
		t.Fatalf("witness address = %x, want %x", got.GetAddress(), addr.Bytes())
	}
	if backend.lastWitnessesAt != 97 {
		t.Fatalf("ListWitnessesAt block = %d, want solid block 97", backend.lastWitnessesAt)
	}
	if backend.liveWitnesses != 0 {
		t.Fatalf("live ListWitnesses called %d times, want 0", backend.liveWitnesses)
	}
}

func solidityTestAddress(fill byte) []byte {
	addr := make([]byte, common.AddressLength)
	addr[0] = common.AddressPrefixMainnet
	for i := 1; i < len(addr); i++ {
		addr[i] = fill
	}
	return addr
}

func solidityTestHash(fill byte) common.Hash {
	var hash common.Hash
	for i := range hash {
		hash[i] = fill
	}
	return hash
}

func solidityBlockIDWithNumber(num uint64) common.Hash {
	hash := solidityTestHash(0xaa)
	binary.BigEndian.PutUint64(hash[:8], num)
	return hash
}
