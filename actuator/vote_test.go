package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeVoteTx(owner common.Address, votes []*contractpb.VoteWitnessContract_Vote) *types.Transaction {
	vc := &contractpb.VoteWitnessContract{
		OwnerAddress: owner.Bytes(),
		Votes:        votes,
	}
	any, _ := anypb.New(vc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_VoteWitnessContract, Parameter: any},
			},
		},
	})
}

func TestVoteWitnessValidate(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(10)
	witness1 := makeTestAddr(20)

	votes := []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 10},
	}
	tx := makeVoteTx(owner, votes)
	act := &VoteWitnessActuator{}
	ctx := &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.AddFreezeV2(corepb.ResourceCode_TRON_POWER, 100*int64(params.TRXPrecision))
	rawdb.WriteAccount(db, owner, acc)

	w := types.NewWitness(witness1, "http://test.com")
	rawdb.WriteWitness(db, witness1, w)

	manyVotes := make([]*contractpb.VoteWitnessContract_Vote, params.MaxVoteNumber+1)
	for i := range manyVotes {
		a := makeTestAddr(byte(100 + i))
		manyVotes[i] = &contractpb.VoteWitnessContract_Vote{VoteAddress: a.Bytes(), VoteCount: 1}
	}
	tx = makeVoteTx(owner, manyVotes)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for too many votes")
	}

	nonWitness := makeTestAddr(30)
	votes = []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: nonWitness.Bytes(), VoteCount: 10},
	}
	tx = makeVoteTx(owner, votes)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-witness vote target")
	}

	votes = []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 50},
	}
	tx = makeVoteTx(owner, votes)
	ctx = &Context{DB: db, Tx: tx}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVoteWitnessExecute(t *testing.T) {
	db := rawdb.NewMemoryDatabase()
	owner := makeTestAddr(10)
	witness1 := makeTestAddr(20)

	acc := types.NewAccount(owner, corepb.AccountType_Normal)
	acc.AddFreezeV2(corepb.ResourceCode_TRON_POWER, 100*int64(params.TRXPrecision))
	rawdb.WriteAccount(db, owner, acc)

	w := types.NewWitness(witness1, "http://test.com")
	rawdb.WriteWitness(db, witness1, w)

	votes := []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 50},
	}
	tx := makeVoteTx(owner, votes)
	act := &VoteWitnessActuator{}
	ctx := &Context{DB: db, Tx: tx}

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	updated := rawdb.ReadAccount(db, owner)
	if len(updated.Votes()) != 1 {
		t.Fatalf("vote count: want 1, got %d", len(updated.Votes()))
	}
	if updated.Votes()[0].VoteCount != 50 {
		t.Fatalf("vote amount: want 50, got %d", updated.Votes()[0].VoteCount)
	}

	updatedW := rawdb.ReadWitness(db, witness1)
	if updatedW.VoteCount() != 50 {
		t.Fatalf("witness vote count: want 50, got %d", updatedW.VoteCount())
	}
}
