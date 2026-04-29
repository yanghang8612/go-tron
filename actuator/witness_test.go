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

func TestWitnessCreateExecute_TotalCreateWitnessCostAccumulates(t *testing.T) {
	statedb := setupStateDB(t)
	owner1 := makeTestAddr(1)
	owner2 := makeTestAddr(2)
	seedAccount(statedb, owner1, 100_000_000_000)
	seedAccount(statedb, owner2, 100_000_000_000)

	ctx1 := setupContext(t, statedb, makeWitnessCreateTx(1, "http://a.com"))
	if before := ctx1.DynProps.TotalCreateWitnessCost(); before != 0 {
		t.Fatalf("initial TotalCreateWitnessCost: want 0, got %d", before)
	}

	fee := ctx1.DynProps.AccountUpgradeCost()
	if _, err := (&WitnessCreateActuator{}).Execute(ctx1); err != nil {
		t.Fatalf("first execute: %v", err)
	}
	if got := ctx1.DynProps.TotalCreateWitnessCost(); got != fee {
		t.Errorf("after first witness: want %d, got %d", fee, got)
	}

	ctx2 := setupContext(t, statedb, makeWitnessCreateTx(2, "http://b.com"))
	// Reuse the same DP — production path holds a single DP per block.
	ctx2.DynProps = ctx1.DynProps
	if _, err := (&WitnessCreateActuator{}).Execute(ctx2); err != nil {
		t.Fatalf("second execute: %v", err)
	}
	if got := ctx2.DynProps.TotalCreateWitnessCost(); got != 2*fee {
		t.Errorf("after second witness: want %d, got %d", 2*fee, got)
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
