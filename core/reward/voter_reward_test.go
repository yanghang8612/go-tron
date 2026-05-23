package reward

import (
	"math/big"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
)

func newRewardTestStore(t *testing.T) *state.StateDB {
	t.Helper()
	db := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(db)
	statedb, err := state.New(tcommon.Hash{}, sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func TestComputeVoterReward_EmptyRange(t *testing.T) {
	store := newRewardTestStore(t)
	dp := state.NewDynamicProperties()

	got := ComputeVoterReward(store, dp, nil, 5, 5)
	if got != 0 {
		t.Fatalf("empty range: got %d, want 0", got)
	}
	got = ComputeVoterReward(store, dp, nil, 5, 3)
	if got != 0 {
		t.Fatalf("reversed range: got %d, want 0", got)
	}
}

func TestComputeVoterReward_OldAlgorithm(t *testing.T) {
	store := newRewardTestStore(t)
	dp := state.NewDynamicProperties()
	// Stay on old algorithm — effective cycle remains MaxInt64.

	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})

	// Cycle 3: witness had 1000 total votes, voter pool = 500.
	_ = store.WriteCycleVote(3, witness.Bytes(), 1000)
	_ = store.WriteCycleReward(3, witness.Bytes(), 500)

	// Voter held 200 of those 1000 votes.
	votes := []VoteEntry{{Witness: witness, Count: 200}}
	got := ComputeVoterReward(store, dp, votes, 3, 4)

	// Expected: 200/1000 * 500 = 100.
	if got != 100 {
		t.Fatalf("old algo reward: got %d, want 100", got)
	}
}

func TestComputeVoterReward_OldAlgorithm_NoVoteSnapshot(t *testing.T) {
	store := newRewardTestStore(t)
	dp := state.NewDynamicProperties()

	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})
	// reward written but no vote snapshot → should skip (REMARK sentinel).
	_ = store.WriteCycleReward(3, witness.Bytes(), 500)

	votes := []VoteEntry{{Witness: witness, Count: 200}}
	got := ComputeVoterReward(store, dp, votes, 3, 4)

	if got != 0 {
		t.Fatalf("missing snapshot should skip: got %d, want 0", got)
	}
}

func TestComputeVoterReward_NewAlgorithm(t *testing.T) {
	store := newRewardTestStore(t)
	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(0) // new algo from the start

	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})

	// VI[2] = 3 × 10^18, VI[5] = 10 × 10^18 → delta = 7 × 10^18.
	_ = store.WriteWitnessVI(2, witness.Bytes(), new(big.Int).Mul(big.NewInt(3), DecimalOfViReward))
	_ = store.WriteWitnessVI(5, witness.Bytes(), new(big.Int).Mul(big.NewInt(10), DecimalOfViReward))

	// Voter held 50 votes across cycles [3, 6).
	votes := []VoteEntry{{Witness: witness, Count: 50}}
	got := ComputeVoterReward(store, dp, votes, 3, 6)

	// Expected: delta (7e18) × 50 / 10^18 = 350.
	if got != 350 {
		t.Fatalf("new algo reward: got %d, want 350", got)
	}
}

func TestComputeVoterReward_HybridAcrossEffectiveCycle(t *testing.T) {
	store := newRewardTestStore(t)
	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(5)

	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})

	// Old-algo side: cycle 3 and 4 each have 100 votes (voter = 40) and
	// pool of 100 → voter earns 40 per cycle, 80 total.
	for c := int64(3); c < 5; c++ {
		_ = store.WriteCycleVote(c, witness.Bytes(), 100)
		_ = store.WriteCycleReward(c, witness.Bytes(), 100)
	}

	// New-algo side: VI[4] = 0, VI[6] = 2 × 10^18 → delta = 2e18 for [5, 7).
	// voter = 40 → 2e18 × 40 / 1e18 = 80.
	_ = store.WriteWitnessVI(4, witness.Bytes(), new(big.Int))
	_ = store.WriteWitnessVI(6, witness.Bytes(), new(big.Int).Mul(big.NewInt(2), DecimalOfViReward))

	votes := []VoteEntry{{Witness: witness, Count: 40}}
	got := ComputeVoterReward(store, dp, votes, 3, 7)

	// 80 (old) + 80 (new) = 160.
	if got != 160 {
		t.Fatalf("hybrid reward: got %d, want 160", got)
	}
}

func TestComputeVoterReward_MultipleWitnesses(t *testing.T) {
	store := newRewardTestStore(t)
	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(0)

	w1 := tcommon.BytesToAddress([]byte{0x41, 0x01})
	w2 := tcommon.BytesToAddress([]byte{0x41, 0x02})

	_ = store.WriteWitnessVI(0, w1.Bytes(), new(big.Int))
	_ = store.WriteWitnessVI(2, w1.Bytes(), new(big.Int).Mul(big.NewInt(4), DecimalOfViReward))
	_ = store.WriteWitnessVI(0, w2.Bytes(), new(big.Int))
	_ = store.WriteWitnessVI(2, w2.Bytes(), new(big.Int).Mul(big.NewInt(6), DecimalOfViReward))

	votes := []VoteEntry{
		{Witness: w1, Count: 10},
		{Witness: w2, Count: 20},
	}
	got := ComputeVoterReward(store, dp, votes, 1, 3)

	// w1: 4e18 × 10 / 1e18 = 40; w2: 6e18 × 20 / 1e18 = 120. Total 160.
	if got != 160 {
		t.Fatalf("multi-witness: got %d, want 160", got)
	}
}
