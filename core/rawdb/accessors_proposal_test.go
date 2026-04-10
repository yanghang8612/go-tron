package rawdb

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/common"
)

func TestProposalWriteRead(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	p := &Proposal{
		ID:             1,
		Proposer:       common.Address{0x41, 0x01},
		Parameters:     map[int64]int64{6: 200},
		CreateTime:     1000,
		ExpirationTime: 260200000,
		State:          ProposalStatePending,
	}
	if err := WriteProposal(db, 1, p); err != nil {
		t.Fatal(err)
	}
	got := ReadProposal(db, 1)
	if got == nil {
		t.Fatal("expected proposal")
	}
	if got.ID != 1 || got.Parameters[6] != 200 || got.State != ProposalStatePending {
		t.Fatalf("unexpected proposal: %+v", got)
	}
}

func TestProposalNotFound(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if ReadProposal(db, 999) != nil {
		t.Fatal("expected nil for missing proposal")
	}
}

func TestProposalIndex(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	ids := []int64{1, 2, 3}
	if err := WriteProposalIndex(db, ids); err != nil {
		t.Fatal(err)
	}
	got := ReadProposalIndex(db)
	if len(got) != 3 || got[0] != 1 || got[2] != 3 {
		t.Fatalf("unexpected index: %v", got)
	}
}

func TestProposalIndexEmpty(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	if ReadProposalIndex(db) != nil {
		t.Fatal("expected nil for empty index")
	}
}
