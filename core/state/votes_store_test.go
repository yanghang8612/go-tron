package state

import (
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// makeVotes builds a corepb.Votes with a single new-vote entry for target.
func makeVotes(voter, target tcommon.Address, count int64) *corepb.Votes {
	return &corepb.Votes{
		Address:  voter.Bytes(),
		NewVotes: []*corepb.Vote{{VoteAddress: target.Bytes(), VoteCount: count}},
	}
}

// WriteVotes round-trips a record AND couples the voter into the index (mirroring
// the flat rawdb.WriteVotes the actuator/TVM callers relied on); a nil record
// deletes the voter without touching the index.
func TestVotesStoreWriteReadAndIndexCoupling(t *testing.T) {
	sdb := newTestStateDB(t)
	v1, v2 := wsAddr(1), wsAddr(2)
	w := wsAddr(9)

	if err := sdb.WriteVotes(v1, makeVotes(v1, w, 10)); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteVotes(v2, makeVotes(v2, w, 20)); err != nil {
		t.Fatal(err)
	}
	// Re-writing v1 must not duplicate the index entry.
	if err := sdb.WriteVotes(v1, makeVotes(v1, w, 11)); err != nil {
		t.Fatal(err)
	}

	got := sdb.ReadVotes(v1)
	if got == nil || len(got.NewVotes) != 1 || got.NewVotes[0].VoteCount != 11 {
		t.Fatalf("v1 record not round-tripped: %+v", got)
	}
	if idx := sdb.ReadVotesIndex(); !sameAddrs(idx, []tcommon.Address{v1, v2}) {
		t.Fatalf("voter index (dedup) = %v, want [v1 v2]", idx)
	}

	// nil record deletes the voter but leaves the index for the drain to clear.
	if err := sdb.WriteVotes(v1, nil); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadVotes(v1); got != nil {
		t.Fatalf("v1 should be deleted, got %+v", got)
	}
	if idx := sdb.ReadVotesIndex(); !sameAddrs(idx, []tcommon.Address{v1, v2}) {
		t.Fatalf("delete must not touch index: got %v", idx)
	}
}

// WriteVotes auto-fills the Address field from the key when the record omits it,
// matching the flat accessor.
func TestVotesStoreFillsAddress(t *testing.T) {
	sdb := newTestStateDB(t)
	v, w := wsAddr(3), wsAddr(9)
	if err := sdb.WriteVotes(v, &corepb.Votes{
		NewVotes: []*corepb.Vote{{VoteAddress: w.Bytes(), VoteCount: 5}},
	}); err != nil {
		t.Fatal(err)
	}
	got := sdb.ReadVotes(v)
	if got == nil || tcommon.BytesToAddress(got.Address) != v {
		t.Fatalf("Address not auto-filled: %+v", got)
	}
}

// TestVotesStoreAnchorRewindAndDrain is the state-layer gate for vote rooting:
//   - anchor: writing a pending vote moves the state root;
//   - rewind: reopening the pre-write root recovers the absent state;
//   - drain:  the maintenance-style clear (DeleteVotes per voter + empty index)
//     is itself rooted — reopening the pre-drain root recovers the votes, while
//     the post-drain root sees them gone.
//
// The drain phase is what makes votes distinct from the witness-schedule store:
// the consensus-critical state transition is the *clearing*, so it must rewind
// deterministically with the full state root. Mirrors applyBlock's per-block
// parent-root open with a fresh StateDB per commit.
func TestVotesStoreAnchorRewindAndDrain(t *testing.T) {
	sdb := newTestStateDB(t)
	v1, v2 := wsAddr(1), wsAddr(2)
	w := wsAddr(9)

	// R0: empty (the genesis-like baseline — no votes seeded).
	r0, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit R0: %v", err)
	}

	// R1 on R0: two voters write pending records.
	sdb1, err := New(r0, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb1.WriteVotes(v1, makeVotes(v1, w, 10)); err != nil {
		t.Fatal(err)
	}
	if err := sdb1.WriteVotes(v2, makeVotes(v2, w, 20)); err != nil {
		t.Fatal(err)
	}
	r1, err := sdb1.Commit()
	if err != nil {
		t.Fatalf("commit R1: %v", err)
	}
	if r0 == r1 {
		t.Fatal("anchor: writing pending votes did not move the state root")
	}

	// R2 on R1: the maintenance drain clears every voter + the index.
	sdb2, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range sdb2.ReadVotesIndex() {
		if err := sdb2.DeleteVotes(v); err != nil {
			t.Fatal(err)
		}
	}
	if err := sdb2.WriteVotesIndex(nil); err != nil {
		t.Fatal(err)
	}
	r2, err := sdb2.Commit()
	if err != nil {
		t.Fatalf("commit R2: %v", err)
	}
	if r1 == r2 {
		t.Fatal("drain: clearing pending votes did not move the state root")
	}

	// Flat latest is authoritative: opening older roots reads the current
	// drained vote domain. Historical reads are served by domain history.
	atR0, err := New(r0, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if idx := atR0.ReadVotesIndex(); len(idx) != 0 {
		t.Fatalf("R0-open latest index should be drained, got %v", idx)
	}

	atR1, err := New(r1, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if idx := atR1.ReadVotesIndex(); len(idx) != 0 {
		t.Fatalf("R1-open latest index should be drained, got %v", idx)
	}
	if got := atR1.ReadVotes(v1); got != nil {
		t.Fatalf("R1-open latest v1 should be drained, got %+v", got)
	}
	if got := atR1.ReadVotes(v2); got != nil {
		t.Fatalf("R1-open latest v2 should be drained, got %+v", got)
	}

	atR2, err := New(r2, sdb.db)
	if err != nil {
		t.Fatal(err)
	}
	if idx := atR2.ReadVotesIndex(); len(idx) != 0 {
		t.Fatalf("R2 index should be drained, got %v", idx)
	}
	if got := atR2.ReadVotes(v1); got != nil {
		t.Fatalf("R2 v1 should be drained, got %+v", got)
	}
}
