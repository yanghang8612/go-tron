package actuator

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestProposalDeleteValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   1,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	p := &rawdb.Proposal{ID: 1, Proposer: owner, ExpirationTime: 999999999, State: rawdb.ProposalStatePending}
	if err := ctx.State.WriteProposal(1, p); err != nil {
		t.Fatal(err)
	}
	ctx.DynProps.SetLatestProposalNum(1)

	act := &ProposalDeleteActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalDeleteNotProposer(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	other := tcommon.Address{0x41, 0x02}
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   1,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	p := &rawdb.Proposal{ID: 1, Proposer: other, ExpirationTime: 999999999, State: rawdb.ProposalStatePending}
	if err := ctx.State.WriteProposal(1, p); err != nil {
		t.Fatal(err)
	}
	ctx.DynProps.SetLatestProposalNum(1)

	act := &ProposalDeleteActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for non-proposer")
	}
}

func TestProposalDeleteExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   1,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)

	p := &rawdb.Proposal{ID: 1, Proposer: owner, ExpirationTime: 999999999, State: rawdb.ProposalStatePending}
	if err := ctx.State.WriteProposal(1, p); err != nil {
		t.Fatal(err)
	}
	ctx.DynProps.SetLatestProposalNum(1)

	act := &ProposalDeleteActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	got := ctx.State.ReadProposal(1)
	if got.State != rawdb.ProposalStateCanceled {
		t.Fatalf("expected CANCELED, got %d", got.State)
	}
}

func TestProposalDeleteRejectsOnlyCanceledState(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalDeleteContract{
		OwnerAddress: owner[:],
		ProposalId:   1,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalDeleteContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.DynProps.SetLatestProposalNum(1)
	if err := ctx.State.WriteProposal(1, &rawdb.Proposal{
		ID:             1,
		Proposer:       owner,
		ExpirationTime: 999999999,
		State:          rawdb.ProposalStateApproved,
	}); err != nil {
		t.Fatal(err)
	}

	act := &ProposalDeleteActuator{}
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
