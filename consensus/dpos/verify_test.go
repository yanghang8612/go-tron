package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/params"
)

func TestSelectActiveWitnesses(t *testing.T) {
	witnesses := []WitnessVote{
		{Address: common.BytesToAddress([]byte{0x41, 1}), Votes: 100},
		{Address: common.BytesToAddress([]byte{0x41, 2}), Votes: 300},
		{Address: common.BytesToAddress([]byte{0x41, 3}), Votes: 200},
	}
	active := SelectActiveWitnesses(witnesses)
	if len(active) != 3 {
		t.Fatalf("active count: want 3, got %d", len(active))
	}
	if active[0] != (common.BytesToAddress([]byte{0x41, 2})) {
		t.Fatal("first witness should be address 2")
	}
	if active[1] != (common.BytesToAddress([]byte{0x41, 3})) {
		t.Fatal("second witness should be address 3")
	}
}

func TestSelectActiveWitnessesMax(t *testing.T) {
	witnesses := make([]WitnessVote, 50)
	for i := range witnesses {
		witnesses[i] = WitnessVote{
			Address: common.BytesToAddress([]byte{0x41, byte(i)}),
			Votes:   int64(1000 - i),
		}
	}
	active := SelectActiveWitnesses(witnesses)
	if len(active) != params.MaxActiveWitnessNum {
		t.Fatalf("active count: want %d, got %d", params.MaxActiveWitnessNum, len(active))
	}
}
