package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func makeVoteTx(ownerByte byte, votes []*contractpb.VoteWitnessContract_Vote) *types.Transaction {
	owner := makeTestAddr(ownerByte)
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
	statedb := setupStateDB(t)
	owner := makeTestAddr(10)
	witness1 := makeTestAddr(20)

	// Missing account
	votes := []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 10},
	}
	tx := makeVoteTx(10, votes)
	act := &VoteWitnessActuator{}
	ctx := setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for missing account")
	}

	// Set up account with frozen TRON power (TotalFrozenV2 is used for tron power calc)
	seedAccount(statedb, owner, 0)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100*int64(params.TRXPrecision))

	// Set up witness (account must exist for vote target validation)
	statedb.CreateAccount(witness1, corepb.AccountType_Normal)
	statedb.PutWitness(witness1, "http://test.com")

	// Too many votes
	manyVotes := make([]*contractpb.VoteWitnessContract_Vote, params.MaxVoteNumber+1)
	for i := range manyVotes {
		a := makeTestAddr(byte(100 + i))
		manyVotes[i] = &contractpb.VoteWitnessContract_Vote{VoteAddress: a.Bytes(), VoteCount: 1}
	}
	tx = makeVoteTx(10, manyVotes)
	ctx = setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for too many votes")
	}

	// Non-witness target
	nonWitness := makeTestAddr(30)
	votes = []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: nonWitness.Bytes(), VoteCount: 10},
	}
	tx = makeVoteTx(10, votes)
	ctx = setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-witness vote target")
	}

	// Success
	votes = []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 50},
	}
	tx = makeVoteTx(10, votes)
	ctx = setupContext(t, statedb, tx)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVoteWitnessExecute(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(10)
	witness1 := makeTestAddr(20)

	seedAccount(statedb, owner, 0)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100*int64(params.TRXPrecision))
	statedb.CreateAccount(witness1, corepb.AccountType_Normal)
	statedb.PutWitness(witness1, "http://test.com")

	votes := []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness1.Bytes(), VoteCount: 50},
	}
	tx := makeVoteTx(10, votes)
	act := &VoteWitnessActuator{}
	ctx := setupContext(t, statedb, tx)

	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: want 0, got %d", result.Fee)
	}

	accVotes := statedb.GetVotes(owner)
	if len(accVotes) != 1 {
		t.Fatalf("vote count: want 1, got %d", len(accVotes))
	}
	if accVotes[0].VoteCount != 50 {
		t.Fatalf("vote amount: want 50, got %d", accVotes[0].VoteCount)
	}

	updatedW := statedb.GetWitness(witness1)
	if updatedW.VoteCount() != 50 {
		t.Fatalf("witness vote count: want 50, got %d", updatedW.VoteCount())
	}
}
