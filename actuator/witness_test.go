package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWitnessCreateTx(owner common.Address, url string) *types.Transaction {
	contract := &contractpb.WitnessCreateContract{
		OwnerAddress: owner.Bytes(),
		Url:          []byte(url),
	}
	anyParam, _ := anypb.New(contract)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_WitnessCreateContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestWitnessCreateExecute(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 100_000_000_000})

	tx := makeWitnessCreateTx(owner, "http://test.com")
	ctx := &Context{DB: db, Tx: tx}
	act := &WitnessCreateActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	w := rawdb.ReadWitness(db, owner)
	if w == nil {
		t.Fatal("witness should exist after creation")
	}
	if w.URL() != "http://test.com" {
		t.Fatalf("expected url http://test.com, got %s", w.URL())
	}
	if w.VoteCount() != 0 {
		t.Fatalf("initial vote count should be 0, got %d", w.VoteCount())
	}
}

func TestWitnessCreateValidate_AlreadyWitness(t *testing.T) {
	owner := common.BytesToAddress([]byte{0x41, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20})
	db := setupDB(map[common.Address]int64{owner: 100_000_000_000})

	w := types.NewWitness(owner, "http://existing.com")
	rawdb.WriteWitness(db, owner, w)

	tx := makeWitnessCreateTx(owner, "http://new.com")
	ctx := &Context{DB: db, Tx: tx}
	act := &WitnessCreateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject duplicate witness")
	}
}
