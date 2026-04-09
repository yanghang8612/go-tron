package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
)

func TestGetScheduledWitness(t *testing.T) {
	witnesses := []common.Address{
		common.BytesToAddress([]byte{0x41, 1}),
		common.BytesToAddress([]byte{0x41, 2}),
		common.BytesToAddress([]byte{0x41, 3}),
	}

	genesisTime := int64(0)
	headTimestamp := int64(0)

	addr := GetScheduledWitness(1, headTimestamp, genesisTime, witnesses, false, 0)
	if addr != witnesses[1] {
		t.Fatalf("slot 1: expected witness[1], got %s", addr.Hex())
	}

	addr = GetScheduledWitness(3, headTimestamp, genesisTime, witnesses, false, 0)
	if addr != witnesses[0] {
		t.Fatalf("slot 3: expected witness[0], got %s", addr.Hex())
	}
}

func TestSortWitnesses(t *testing.T) {
	w1 := WitnessVote{Address: common.BytesToAddress([]byte{0x41, 0xaa}), Votes: 100}
	w2 := WitnessVote{Address: common.BytesToAddress([]byte{0x41, 0xbb}), Votes: 200}
	w3 := WitnessVote{Address: common.BytesToAddress([]byte{0x41, 0xcc}), Votes: 200}

	sorted := SortWitnessesByVotes([]WitnessVote{w1, w2, w3})
	if sorted[0].Votes != 200 {
		t.Fatalf("expected highest votes first, got %d", sorted[0].Votes)
	}
	if sorted[2].Votes != 100 {
		t.Fatalf("expected lowest votes last, got %d", sorted[2].Votes)
	}
}
