package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestProposalCreateValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.ActiveWitnesses = []tcommon.Address{owner}
	act := &ProposalCreateActuator{}

	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-existent owner")
	}

	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalCreateNotWitness(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = nil // no active witnesses

	act := &ProposalCreateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-active witness")
	}
}

func TestProposalCreateEmptyParams(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	act := &ProposalCreateActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for empty parameters")
	}
}

func TestProposalCreateExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalCreateContract{
		OwnerAddress: owner[:],
		Parameters:   map[int64]int64{6: 200},
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalCreateContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db

	act := &ProposalCreateActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	// java-tron parity: first proposal_id == 1 (pre-increment of latest=0).
	p := rawdb.ReadProposal(db, 1)
	if p == nil {
		t.Fatal("proposal not stored at id=1")
	}
	if p.ID != 1 || p.Proposer != owner || p.State != rawdb.ProposalStatePending {
		t.Fatalf("unexpected proposal: %+v", p)
	}
	if rawdb.ReadProposal(db, 0) != nil {
		t.Fatal("no proposal should be stored at id=0")
	}
	if ctx.DynProps.NextProposalID() != 2 {
		t.Fatalf("next_proposal_id=%d, want 2", ctx.DynProps.NextProposalID())
	}

	// Second proposal must get id=2; counter advances to 3.
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("second execute failed: %v", err)
	}
	if p2 := rawdb.ReadProposal(db, 2); p2 == nil || p2.ID != 2 {
		t.Fatalf("second proposal not stored at id=2: %+v", p2)
	}
	if ctx.DynProps.NextProposalID() != 3 {
		t.Fatalf("next_proposal_id=%d, want 3", ctx.DynProps.NextProposalID())
	}
}
