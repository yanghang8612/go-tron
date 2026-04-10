package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func setupProposalForApprove(t *testing.T, db ethdb.Database, proposer tcommon.Address) {
	t.Helper()
	p := &rawdb.Proposal{
		ID:             0,
		Proposer:       proposer,
		Parameters:     map[int64]int64{6: 200},
		CreateTime:     500,
		ExpirationTime: 500 + 259200000,
		State:          rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})
}

func TestProposalApproveValidate(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    0,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	setupProposalForApprove(t, db, owner)

	act := &ProposalApproveActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate failed: %v", err)
	}
}

func TestProposalApproveExecute(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    0,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	proposer := tcommon.Address{0x41, 0x02}
	setupProposalForApprove(t, db, proposer)

	act := &ProposalApproveActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1")
	}

	p := rawdb.ReadProposal(db, 0)
	if len(p.Approvals) != 1 || p.Approvals[0] != owner {
		t.Fatalf("approval not recorded: %+v", p.Approvals)
	}
}

func TestProposalApproveDoubleApprove(t *testing.T) {
	owner := tcommon.Address{0x41, 0x01}
	c := &contractpb.ProposalApproveContract{
		OwnerAddress:  owner[:],
		ProposalId:    0,
		IsAddApproval: true,
	}
	ctx := newTestContext(t, corepb.Transaction_Contract_ProposalApproveContract, c, 0)
	ctx.State.CreateAccount(owner, corepb.AccountType_Normal)
	ctx.ActiveWitnesses = []tcommon.Address{owner}

	db := ethrawdb.NewMemoryDatabase()
	ctx.DB = db
	p := &rawdb.Proposal{
		ID: 0, ExpirationTime: 999999999, State: rawdb.ProposalStatePending,
		Approvals: []tcommon.Address{owner},
	}
	rawdb.WriteProposal(db, 0, p)

	act := &ProposalApproveActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for double approve")
	}
}
