package txpool

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeTx(from byte, amount int64) *types.Transaction {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = from
	tc := &contractpb.TransferContract{
		OwnerAddress: addr.Bytes(),
		ToAddress:    addr.Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Timestamp: int64(from)*1000 + amount, // unique per combo
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	})
}

func TestTxPool_AddAndGet(t *testing.T) {
	pool := New()
	tx := makeTx(1, 100)
	if err := pool.Add(tx); err != nil {
		t.Fatal(err)
	}
	if pool.Count() != 1 {
		t.Fatalf("count: got %d, want 1", pool.Count())
	}
	got := pool.Get(tx.Hash())
	if got == nil {
		t.Fatal("transaction not found")
	}
}

func TestTxPool_DuplicateReject(t *testing.T) {
	pool := New()
	tx := makeTx(1, 100)
	pool.Add(tx)
	if err := pool.Add(tx); err != ErrAlreadyKnown {
		t.Fatalf("expected ErrAlreadyKnown, got %v", err)
	}
}

func TestTxPool_NoContractReject(t *testing.T) {
	pool := New()
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{},
	})
	if err := pool.Add(tx); err != ErrNoContract {
		t.Fatalf("expected ErrNoContract, got %v", err)
	}
}

// TestTxPool_RejectsExchangeTransaction locks down master PR #6507's
// unconditional mempool reject for ExchangeTransactionContract — independent
// of fork state, since java-tron's pushTransaction has no version gate.
func TestTxPool_RejectsExchangeTransaction(t *testing.T) {
	pool := New()
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = 0x01
	exchange := &contractpb.ExchangeTransactionContract{
		OwnerAddress: addr.Bytes(),
		ExchangeId:   1,
		TokenId:      []byte("_"),
		Quant:        1,
		Expected:     1,
	}
	param, err := anypb.New(exchange)
	if err != nil {
		t.Fatalf("anypb.New: %v", err)
	}
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_ExchangeTransactionContract,
				Parameter: param,
			}},
		},
	})
	if err := pool.Add(tx); err != ErrExchangeRejected {
		t.Fatalf("expected ErrExchangeRejected, got %v", err)
	}
}

func TestTxPool_Remove(t *testing.T) {
	pool := New()
	tx := makeTx(1, 100)
	pool.Add(tx)
	pool.Remove(tx.Hash())
	if pool.Count() != 0 {
		t.Fatalf("count after remove: got %d, want 0", pool.Count())
	}
}

func TestTxPool_Pending(t *testing.T) {
	pool := New()
	pool.Add(makeTx(1, 100))
	pool.Add(makeTx(2, 200))
	pool.Add(makeTx(3, 300))

	pending := pool.Pending()
	if len(pending) != 3 {
		t.Fatalf("pending: got %d, want 3", len(pending))
	}
}

func TestTxPool_PoolFull(t *testing.T) {
	pool := New()
	pool.maxSize = 2
	pool.Add(makeTx(1, 100))
	pool.Add(makeTx(2, 200))
	if err := pool.Add(makeTx(3, 300)); err != ErrPoolFull {
		t.Fatalf("expected ErrPoolFull, got %v", err)
	}
}
