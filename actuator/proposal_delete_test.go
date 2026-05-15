package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
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

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{ID: 1, Proposer: owner, ExpirationTime: 999999999, State: rawdb.ProposalStatePending}
	rawdb.WriteProposal(db, 1, p)
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

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{ID: 1, Proposer: other, ExpirationTime: 999999999, State: rawdb.ProposalStatePending}
	rawdb.WriteProposal(db, 1, p)
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

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{ID: 1, Proposer: owner, ExpirationTime: 999999999, State: rawdb.ProposalStatePending}
	rawdb.WriteProposal(db, 1, p)
	ctx.DynProps.SetLatestProposalNum(1)

	act := &ProposalDeleteActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	got := rawdb.ReadProposal(db, 1)
	if got.State != rawdb.ProposalStateCanceled {
		t.Fatalf("expected CANCELED, got %d", got.State)
	}
}
