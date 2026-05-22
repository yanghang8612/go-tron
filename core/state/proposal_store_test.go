package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

func propAddr(tag byte) tcommon.Address {
	raw := make([]byte, tcommon.AddressLength)
	raw[0] = 0x41
	raw[tcommon.AddressLength-1] = tag
	return tcommon.BytesToAddress(raw)
}

func sameInt64s(a, b []int64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A proposal round-trips through the system-KV with its JSON fields intact, and
// the index decodes back to the written ids.
func TestProposalStoreReadWrite(t *testing.T) {
	sdb := newTestStateDB(t)
	p := &rawdb.Proposal{
		ID:             7,
		Proposer:       propAddr(1),
		Parameters:     map[int64]int64{6: 200, 9: 1},
		CreateTime:     1000,
		ExpirationTime: 2000,
		Approvals:      []tcommon.Address{propAddr(1), propAddr(2)},
		State:          rawdb.ProposalStatePending,
	}
	if err := sdb.WriteProposal(7, p); err != nil {
		t.Fatal(err)
	}
	got := sdb.ReadProposal(7)
	if got == nil {
		t.Fatal("expected proposal 7")
	}
	if got.ID != 7 || got.Proposer != propAddr(1) || got.State != rawdb.ProposalStatePending {
		t.Fatalf("unexpected proposal: %+v", got)
	}
	if got.Parameters[6] != 200 || got.Parameters[9] != 1 {
		t.Fatalf("parameters lost: %+v", got.Parameters)
	}
	if len(got.Approvals) != 2 || got.Approvals[0] != propAddr(1) || got.Approvals[1] != propAddr(2) {
		t.Fatalf("approvals lost: %+v", got.Approvals)
	}
	if sdb.ReadProposal(999) != nil {
		t.Fatal("missing proposal should read nil")
	}
}

// AppendProposalIndex grows the index in order; an unset index reads nil.
func TestProposalIndexAppend(t *testing.T) {
	sdb := newTestStateDB(t)
	if got := sdb.ReadProposalIndex(); got != nil {
		t.Fatalf("empty index should be nil, got %v", got)
	}
	for _, id := range []int64{1, 2, 5} {
		if err := sdb.AppendProposalIndex(id); err != nil {
			t.Fatal(err)
		}
	}
	if got := sdb.ReadProposalIndex(); !sameInt64s(got, []int64{1, 2, 5}) {
		t.Fatalf("index mismatch: got %v", got)
	}
}

// TestProposalAnchorAndRewind is the Phase 3d state-layer gate: rooting a
// proposal change moves the state root (anchor), and reopening an old root
// recovers the old record AND index (rewind). Mirrors applyBlock's per-block
// parent-root open with a fresh StateDB per commit, and the maintenance
// settlement that flips a PENDING proposal to APPROVED.
func TestProposalAnchorAndRewind(t *testing.T) {
	sdb := newTestStateDB(t)

	// R1: proposal 1 PENDING, index = {1}.
	p1 := &rawdb.Proposal{
		ID:             1,
		Proposer:       propAddr(1),
		Parameters:     map[int64]int64{9: 1},
		ExpirationTime: 2000,
		Approvals:      []tcommon.Address{propAddr(1)},
		State:          rawdb.ProposalStatePending,
	}
	if err := sdb.WriteProposal(1, p1); err != nil {
		t.Fatal(err)
	}
	if err := sdb.AppendProposalIndex(1); err != nil {
		t.Fatal(err)
	}
	r1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit R1: %v", err)
	}

	// R2 built on R1 via a fresh StateDB: a new proposal 2 is created (index
	// {1,2}) and the maintenance settlement flips proposal 1 to APPROVED.
	sdb2, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	p1Approved := sdb2.ReadProposal(1)
	if p1Approved == nil {
		t.Fatal("R2: proposal 1 missing")
	}
	p1Approved.State = rawdb.ProposalStateApproved
	if err := sdb2.WriteProposal(1, p1Approved); err != nil {
		t.Fatal(err)
	}
	p2 := &rawdb.Proposal{ID: 2, Proposer: propAddr(2), ExpirationTime: 4000, State: rawdb.ProposalStatePending}
	if err := sdb2.WriteProposal(2, p2); err != nil {
		t.Fatal(err)
	}
	if err := sdb2.AppendProposalIndex(2); err != nil {
		t.Fatal(err)
	}
	r2, err := sdb2.Commit()
	if err != nil {
		t.Fatalf("commit R2: %v", err)
	}

	if r1 == r2 {
		t.Fatal("anchor: proposal change did not move the state root")
	}

	// Rewind: R1 recovers proposal 1 PENDING + index {1}; R2 keeps APPROVED +
	// index {1,2}.
	atR1, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR1.ReadProposal(1); got == nil || got.State != rawdb.ProposalStatePending {
		t.Fatalf("rewind R1 proposal 1 state: %+v", got)
	}
	if atR1.ReadProposal(2) != nil {
		t.Fatal("rewind R1: proposal 2 must not exist")
	}
	if got := atR1.ReadProposalIndex(); !sameInt64s(got, []int64{1}) {
		t.Fatalf("rewind R1 index: got %v", got)
	}

	atR2, err := New(r2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if got := atR2.ReadProposal(1); got == nil || got.State != rawdb.ProposalStateApproved {
		t.Fatalf("R2 proposal 1 state: %+v", got)
	}
	if got := atR2.ReadProposal(2); got == nil || got.State != rawdb.ProposalStatePending {
		t.Fatalf("R2 proposal 2 state: %+v", got)
	}
	if got := atR2.ReadProposalIndex(); !sameInt64s(got, []int64{1, 2}) {
		t.Fatalf("R2 index: got %v", got)
	}
}
