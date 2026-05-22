package actuator

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func setAllowNewResourceModel(ctx *Context, on bool) {
	ctx.DynProps.SetAllowNewResourceModel(on)
}

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
	if updatedW.VoteCount() != 0 {
		t.Fatalf("witness vote count before maintenance: want 0, got %d", updatedW.VoteCount())
	}
	pending := statedb.ReadVotes(owner)
	if pending == nil || len(pending.OldVotes) != 0 || len(pending.NewVotes) != 1 || pending.NewVotes[0].VoteCount != 50 {
		t.Fatalf("pending votes not recorded like java-tron VotesStore: %+v", pending)
	}
}

func TestVoteWitnessDuplicateTargetsAllowed(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(10)
	witness := makeTestAddr(20)

	seedAccount(statedb, owner, 0)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100*int64(params.TRXPrecision))
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	statedb.PutWitness(witness, "http://test.com")

	votes := []*contractpb.VoteWitnessContract_Vote{
		{VoteAddress: witness.Bytes(), VoteCount: 40},
		{VoteAddress: witness.Bytes(), VoteCount: 40},
	}
	tx := makeVoteTx(10, votes)
	act := &VoteWitnessActuator{}
	ctx := setupContext(t, statedb, tx)

	if err := act.Validate(ctx); err != nil {
		t.Fatalf("duplicate vote targets should be accepted like java-tron: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatal(err)
	}
	if got := statedb.GetWitness(witness).VoteCount(); got != 0 {
		t.Fatalf("witness vote count before maintenance: got %d, want 0", got)
	}
	pending := statedb.ReadVotes(owner)
	if pending == nil || len(pending.NewVotes) != 2 {
		t.Fatalf("duplicate vote targets should be retained in VotesStore: %+v", pending)
	}
}

// TestVoteWitness_PreFork: without AllowNewResourceModel, non-TRON_POWER V2 frozen
// counts as voting power (LegacyTronPower path).
func TestVoteWitness_PreFork(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(10)
	witness := makeTestAddr(20)
	seedAccount(statedb, owner, 0)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100*int64(params.TRXPrecision))
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	statedb.PutWitness(witness, "http://test.com")

	votes := []*contractpb.VoteWitnessContract_Vote{{VoteAddress: witness.Bytes(), VoteCount: 100}}
	tx := makeVoteTx(10, votes)
	ctx := setupContext(t, statedb, tx)
	setAllowNewResourceModel(ctx, false) // pre-fork

	if err := (&VoteWitnessActuator{}).Validate(ctx); err != nil {
		t.Fatalf("pre-fork validate: %v", err)
	}
	if _, err := (&VoteWitnessActuator{}).Execute(ctx); err != nil {
		t.Fatalf("pre-fork execute: %v", err)
	}
	// old_tron_power must NOT be set in pre-fork mode
	acc := statedb.GetAccount(owner)
	if got := acc.OldTronPower(); got != 0 {
		t.Errorf("old_tron_power after pre-fork vote: want 0, got %d", got)
	}
}

// TestVoteWitness_PostFork_Uninitialized: AllowNewResourceModel active, old_tron_power=0.
// AllTronPower = LegacyTronPower + 0 + 0; after Execute, old_tron_power is snapshotted.
func TestVoteWitness_PostFork_Uninitialized(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(11)
	witness := makeTestAddr(21)
	seedAccount(statedb, owner, 0)
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100*int64(params.TRXPrecision))
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	statedb.PutWitness(witness, "http://test.com")

	votes := []*contractpb.VoteWitnessContract_Vote{{VoteAddress: witness.Bytes(), VoteCount: 100}}
	tx := makeVoteTx(11, votes)
	ctx := setupContext(t, statedb, tx)
	setAllowNewResourceModel(ctx, true)

	if err := (&VoteWitnessActuator{}).Validate(ctx); err != nil {
		t.Fatalf("validate: %v", err)
	}
	if _, err := (&VoteWitnessActuator{}).Execute(ctx); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// After first vote with fork active, old_tron_power is set to LegacyTronPower.
	acc := statedb.GetAccount(owner)
	want := int64(100 * params.TRXPrecision)
	if got := acc.OldTronPower(); got != want {
		t.Errorf("old_tron_power: want %d, got %d", want, got)
	}
}

// TestVoteWitness_PostFork_Initialized: old_tron_power>0 was set during a prior vote.
// New V2 BANDWIDTH frozen does NOT increase voting power; only TRON_POWER-typed does.
func TestVoteWitness_PostFork_Initialized(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(12)
	witness := makeTestAddr(22)
	seedAccount(statedb, owner, 0)
	// 50 TRX BANDWIDTH frozen and snapshot already taken (old_tron_power = 50 TRX).
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 50*int64(params.TRXPrecision))
	acc := statedb.GetAccount(owner)
	acc.SetOldTronPower(50 * int64(params.TRXPrecision))
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	statedb.PutWitness(witness, "http://test.com")

	dp := state.NewDynamicProperties()
	dp.SetAllowNewResourceModel(true)

	// 51 votes — should exceed AllTronPower (old=50 + V2_TP=0 = 50 TRX).
	votes51 := []*contractpb.VoteWitnessContract_Vote{{VoteAddress: witness.Bytes(), VoteCount: 51}}
	tx := makeVoteTx(12, votes51)
	ctx := &Context{State: statedb, DynProps: dp, Tx: tx, BlockTime: 1000000, BlockNumber: 1}
	if err := (&VoteWitnessActuator{}).Validate(ctx); err == nil {
		t.Fatal("expected error: 51 votes > 50 TRX AllTronPower")
	}

	// 50 votes — exactly equals AllTronPower.
	votes50 := []*contractpb.VoteWitnessContract_Vote{{VoteAddress: witness.Bytes(), VoteCount: 50}}
	tx = makeVoteTx(12, votes50)
	ctx = &Context{State: statedb, DynProps: dp, Tx: tx, BlockTime: 1000000, BlockNumber: 1}
	if err := (&VoteWitnessActuator{}).Validate(ctx); err != nil {
		t.Fatalf("50 votes should be valid: %v", err)
	}
}

// TestVoteWitness_PostFork_Invalid: old_tron_power=-1 means legacy contribution is zero.
// Only explicit V2 TRON_POWER-typed frozen counts.
func TestVoteWitness_PostFork_Invalid(t *testing.T) {
	statedb := setupStateDB(t)
	owner := makeTestAddr(13)
	witness := makeTestAddr(23)
	seedAccount(statedb, owner, 0)
	// 100 TRX BANDWIDTH frozen but old_tron_power=-1 (consumed by prior unfreeze).
	statedb.AddFreezeV2(owner, corepb.ResourceCode_BANDWIDTH, 100*int64(params.TRXPrecision))
	// 30 TRX TRON_POWER typed frozen — this is the only voting power now.
	statedb.AddFreezeV2(owner, corepb.ResourceCode_TRON_POWER, 30*int64(params.TRXPrecision))
	acc := statedb.GetAccount(owner)
	acc.SetOldTronPower(-1) // invalid: legacy snapshot consumed
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	statedb.PutWitness(witness, "http://test.com")

	dp := state.NewDynamicProperties()
	dp.SetAllowNewResourceModel(true)

	// 31 votes > 30 TRX TRON_POWER — should fail.
	votes31 := []*contractpb.VoteWitnessContract_Vote{{VoteAddress: witness.Bytes(), VoteCount: 31}}
	tx := makeVoteTx(13, votes31)
	ctx := &Context{State: statedb, DynProps: dp, Tx: tx, BlockTime: 1000000, BlockNumber: 1}
	if err := (&VoteWitnessActuator{}).Validate(ctx); err == nil {
		t.Fatal("expected error: 31 votes > 30 TRX from TRON_POWER typed")
	}

	// 30 votes — exactly V2 TRON_POWER typed.
	votes30 := []*contractpb.VoteWitnessContract_Vote{{VoteAddress: witness.Bytes(), VoteCount: 30}}
	tx = makeVoteTx(13, votes30)
	ctx = &Context{State: statedb, DynProps: dp, Tx: tx, BlockTime: 1000000, BlockNumber: 1}
	if err := (&VoteWitnessActuator{}).Validate(ctx); err != nil {
		t.Fatalf("30 votes should be valid: %v", err)
	}
}
