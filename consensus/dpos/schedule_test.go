package dpos

import (
	"encoding/hex"
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

func TestGetScheduledWitness_MaintenanceSkipDoesNotAdvanceRotation(t *testing.T) {
	witnesses := []common.Address{
		common.BytesToAddress([]byte{0x41, 1}),
		common.BytesToAddress([]byte{0x41, 2}),
		common.BytesToAddress([]byte{0x41, 3}),
	}

	// Matches java-tron DposSlot.getScheduledWitness: the previous
	// maintenance block makes getSlot/getTime skip two wall-clock slots, but
	// the witness index still uses getAbSlot(head) + relativeSlot.
	const genesisTime = int64(0)
	const headTimestamp = int64(6000)

	addr := GetScheduledWitness(1, headTimestamp, genesisTime, witnesses, true, 2)
	if addr != witnesses[0] {
		t.Fatalf("maintenance slot 1: expected witness[0], got %s", addr.Hex())
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

func TestSortWitnessesPreOptimizationUsesJavaByteStringHash(t *testing.T) {
	lowHashHighHex := mustScheduleTestAddress(t, "41ffffffffffffffffffffffffffffffffffffffff")
	highHashLowHex := mustScheduleTestAddress(t, "410000000000000000000000000000000000000100")

	witnesses := []WitnessVote{
		{Address: lowHashHighHex, Votes: 100},
		{Address: highHashLowHex, Votes: 100},
	}

	preOpt := SortWitnessesByVotesWithOptimization(witnesses, false)
	if preOpt[0].Address != highHashLowHex {
		t.Fatalf("pre-optimization tie-break: got %s, want %s", preOpt[0].Address.Hex(), highHashLowHex.Hex())
	}

	postOpt := SortWitnessesByVotesWithOptimization(witnesses, true)
	if postOpt[0].Address != lowHashHighHex {
		t.Fatalf("post-optimization tie-break: got %s, want %s", postOpt[0].Address.Hex(), lowHashHighHex.Hex())
	}
}

func mustScheduleTestAddress(t *testing.T, s string) common.Address {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatal(err)
	}
	if len(b) != common.AddressLength {
		t.Fatalf("address length: got %d, want %d", len(b), common.AddressLength)
	}
	return common.BytesToAddress(b)
}
