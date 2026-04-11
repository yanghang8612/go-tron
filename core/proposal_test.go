package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestProcessProposals_Approved(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	dynProps := state.NewDynamicProperties()

	// Create proposal to change witness_pay_per_block (ID 5) to 32000000
	p := &rawdb.Proposal{
		ID:             0,
		Proposer:       tcommon.Address{0x41, 0x01},
		Parameters:     map[int64]int64{5: 32000000},
		CreateTime:     1000,
		ExpirationTime: 2000,
		Approvals: []tcommon.Address{
			{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03},
		},
		State: rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})

	// 3 approvals out of 4 SRs = 75% >= 70%
	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
	if err := ProcessProposals(db, dynProps, active, 3000); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := rawdb.ReadProposal(db, 0)
	if got.State != rawdb.ProposalStateApproved {
		t.Fatalf("expected APPROVED, got %d", got.State)
	}
	if dynProps.WitnessPayPerBlock() != 32000000 {
		t.Fatalf("parameter not applied: %d", dynProps.WitnessPayPerBlock())
	}
}

func TestProcessProposals_Canceled(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	dynProps := state.NewDynamicProperties()

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{5: 32000000},
		ExpirationTime: 2000,
		Approvals:      []tcommon.Address{{0x41, 0x01}}, // 1 of 4 = 25%
		State:          rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})

	active4 := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
	if err := ProcessProposals(db, dynProps, active4, 3000); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := rawdb.ReadProposal(db, 0)
	if got.State != rawdb.ProposalStateCanceled {
		t.Fatalf("expected CANCELED, got %d", got.State)
	}
	// Parameter should NOT have changed (default is 16000000)
	if dynProps.WitnessPayPerBlock() != 16000000 {
		t.Fatalf("parameter should not change: %d", dynProps.WitnessPayPerBlock())
	}
}

func TestProcessProposals_NotExpired(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	dynProps := state.NewDynamicProperties()

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{5: 32000000},
		ExpirationTime: 9999999,
		Approvals:      []tcommon.Address{{0x41, 0x01}},
		State:          rawdb.ProposalStatePending,
	}
	rawdb.WriteProposal(db, 0, p)
	rawdb.WriteProposalIndex(db, []int64{0})

	if err := ProcessProposals(db, dynProps, []tcommon.Address{{0x41, 0x01}}, 1000); err != nil { // maintenance time < expiration
		t.Fatalf("unexpected error: %v", err)
	}

	got := rawdb.ReadProposal(db, 0)
	if got.State != rawdb.ProposalStatePending {
		t.Fatalf("expected still PENDING, got %d", got.State)
	}
}
