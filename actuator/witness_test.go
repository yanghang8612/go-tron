package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeWitnessCreateTx(ownerByte byte, url string) *types.Transaction {
	owner := makeTestAddr(ownerByte)
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
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 100_000_000_000)

	tx := makeWitnessCreateTx(1, "http://test.com")
	ctx := setupContext(t, statedb, tx)
	act := &WitnessCreateActuator{}

	_, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}

	w := statedb.GetWitness(owner)
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
	statedb := setupStateDB(t)
	owner := makeTestAddr(1)
	seedAccount(statedb, owner, 100_000_000_000)

	// Pre-register as witness
	statedb.PutWitness(owner, "http://existing.com")

	tx := makeWitnessCreateTx(1, "http://new.com")
	ctx := setupContext(t, statedb, tx)
	act := &WitnessCreateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("validate should reject duplicate witness")
	}
}
