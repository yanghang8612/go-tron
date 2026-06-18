package grpcapi_test

import (
	"context"
	"encoding/hex"
	"net"
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/internal/grpcapi"
	"github.com/tronprotocol/go-tron/internal/tronapi"
	apipb "github.com/tronprotocol/go-tron/proto/api"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	lastNumQueried     uint64
	lastAccountAt      uint64
	lastAccountIDAt    uint64
	lastRewardAt       uint64
	lastDelegatedAt    uint64
	lastDelegIndexAt   uint64
	liveAccountCalls   int
	liveAccountIDCalls int
	liveRewardCalls    int
	liveDelegCalls     int
	liveIndexCalls     int
	accountAt          *types.Account
	accountIDAt        *types.Account
	rewardAt           *tronapi.RewardInfo
	delegatedAt        []*tronapi.DelegatedResourceInfo
	delegIndexAt       *tronapi.DelegationIndexInfo
}

func (b *solidTestBackend) SolidifiedBlockNum() uint64 { return b.solidNum }

func (b *solidTestBackend) GetBlockByNumber(n uint64) (*types.Block, error) {
	b.lastNumQueried = n
	return b.testBackend.GetBlockByNumber(n)
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

func solidityTestAddress(fill byte) []byte {
	addr := make([]byte, common.AddressLength)
	addr[0] = common.AddressPrefixMainnet
	for i := 1; i < len(addr); i++ {
		addr[i] = fill
	}
	return addr
}
