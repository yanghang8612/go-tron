package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func setupProposalForApprove(t *testing.T, sdb *state.StateDB, proposer tcommon.Address) {
	t.Helper()
	p := &rawdb.Proposal{
		ID:             1,
		Proposer:       proposer,
		Parameters:     map[int64]int64{6: 200},
		CreateTime:     500,
		ExpirationTime: 500 + 259200000,
		State:          rawdb.ProposalStatePending,
	}
	if err := sdb.WriteProposal(1, p); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteProposalIndex([]int64{1}); err != nil {
		t.Fatal(err)
	}
}

func TestProposalApproveValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    1,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	setupProposalForApprove(t, ctx.State, owner)
	ctx.DynProps.SetLatestProposalNum(1)

	act := &ProposalApproveActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalApproveExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    1,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	proposer := tcommon.Address{0x41, 0x02}
	setupProposalForApprove(t, ctx.State, proposer)

	act := &ProposalApproveActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	p := ctx.State.ReadProposal(1)
	if len(p.Approvals) != 1 || p.Approvals[0] != owner {
		t.Fatalf("approval not recorded: %+v", p.Approvals)
	}
}

func TestProposalApproveDoubleApprove(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    1,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	p := &rawdb.Proposal{
		ID: 1, ExpirationTime: 999999999, State: rawdb.ProposalStatePending,
		Approvals: []tcommon.Address{owner},
	}
	if err := ctx.State.WriteProposal(1, p); err != nil {
		t.Fatal(err)
	}
	ctx.DynProps.SetLatestProposalNum(1)

	act := &ProposalApproveActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for double approve")
	}
}

func TestProposalApproveRejectsOnlyCanceledState(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    1,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.State.PutWitness(owner, "http://w.com")
	ctx.ActiveWitnesses = []tcommon.Address{owner}
	ctx.DynProps.SetLatestProposalNum(1)
	if err := ctx.State.WriteProposal(1, &rawdb.Proposal{
		ID:             1,
		ExpirationTime: 999999999,
		State:          rawdb.ProposalStateApproved,
	}); err != nil {
		t.Fatal(err)
	}

	act := &ProposalApproveActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("approved state should not be rejected before expiration: %v", err)
	}

	p := ctx.State.ReadProposal(1)
	p.State = rawdb.ProposalStateCanceled
	if err := ctx.State.WriteProposal(1, p); err != nil {
		t.Fatal(err)
	}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected canceled proposal to be rejected")
	}
}
