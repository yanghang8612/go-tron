package grpcapi_test

import (
	"context"
	"net"
	"testing"

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
	solidNum       uint64
	lastNumQueried uint64
}

func (b *solidTestBackend) SolidifiedBlockNum() uint64 { return b.solidNum }

func (b *solidTestBackend) GetBlockByNumber(n uint64) (*types.Block, error) {
	b.lastNumQueried = n
	return b.testBackend.GetBlockByNumber(n)
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
