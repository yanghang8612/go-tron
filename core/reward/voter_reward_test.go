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

// TestComputeVoterReward_OldRewardOpt_TruncationDrift pins divergence #4: with
// allowOldRewardOpt (#79) OFF, go-tron truncates the voter share per cycle
// (java's legacy computeReward); with it ON, java telescopes the cumulative VI
// over the whole old segment and floors ONCE. reward=20/cycle, totalVote=3,
// userVote=1, two old cycles [1,3) (E=3):
//   - opt OFF: floor(20*1/3)=6 per cycle x2 = 12.
//   - opt ON:  viSum = 2*floor(20e18/3)=13333333333333333332; floor(viSum/1e18)=13.
//
// The +1 is the latent reward (balance) fork once #79 activates on Nile/mainnet.
func TestComputeVoterReward_OldRewardOpt_TruncationDrift(t *testing.T) {
	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})
	setup := func() *state.StateDB {
		store := newRewardTestStore(t)
		for c := int64(1); c < 3; c++ {
			_ = store.WriteCycleVote(c, witness.Bytes(), 3)
			_ = store.WriteCycleReward(c, witness.Bytes(), 20)
		}
		return store
	}
	votes := []VoteEntry{{Witness: witness, Count: 1}}

	dpOff := state.NewDynamicProperties()
	dpOff.SetNewRewardAlgorithmEffectiveCycle(3)
	if got := ComputeVoterReward(setup(), dpOff, votes, 1, 3); got != 12 {
		t.Fatalf("opt OFF: got %d, want 12 (legacy per-cycle truncate)", got)
	}

	dpOn := state.NewDynamicProperties()
	dpOn.SetNewRewardAlgorithmEffectiveCycle(3)
	dpOn.SetAllowOldRewardOpt(true)
	if got := ComputeVoterReward(setup(), dpOn, votes, 1, 3); got != 13 {
		t.Fatalf("opt ON: got %d, want 13 (java VI telescoping); +1 vs legacy is the fork", got)
	}
}

// TestComputeVoterReward_OldRewardOpt_ThreeCycleDrift — three old cycles [1,4):
// opt ON viSum = 3*floor(20e18/3)=19999999999999999998 -> floor(/1e18)=19 (one
// floor at the end); legacy floors each cycle -> 6*3 = 18.
func TestComputeVoterReward_OldRewardOpt_ThreeCycleDrift(t *testing.T) {
	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})
	setup := func() *state.StateDB {
		store := newRewardTestStore(t)
		for c := int64(1); c < 4; c++ {
			_ = store.WriteCycleVote(c, witness.Bytes(), 3)
			_ = store.WriteCycleReward(c, witness.Bytes(), 20)
		}
		return store
	}
	votes := []VoteEntry{{Witness: witness, Count: 1}}

	dpOff := state.NewDynamicProperties()
	dpOff.SetNewRewardAlgorithmEffectiveCycle(4)
	if got := ComputeVoterReward(setup(), dpOff, votes, 1, 4); got != 18 {
		t.Fatalf("opt OFF three cycles: got %d, want 18", got)
	}
	dpOn := state.NewDynamicProperties()
	dpOn.SetNewRewardAlgorithmEffectiveCycle(4)
	dpOn.SetAllowOldRewardOpt(true)
	if got := ComputeVoterReward(setup(), dpOn, votes, 1, 4); got != 19 {
		t.Fatalf("opt ON three cycles: got %d, want 19", got)
	}
}

// TestComputeVoterReward_OldRewardOpt_AcrossEffectiveCycle proves the opt old
// segment composes with the unchanged new (stored-VI) segment across E. E=3,
// voter span [1,5): old [1,3) telescopes to 13 (vs legacy 12); new [3,5) reads
// stored VI[2]=0,VI[4]=7e18 -> 7. opt total = 20 (legacy would be 19), and the
// per-segment floors stay separate exactly as java does.
func TestComputeVoterReward_OldRewardOpt_AcrossEffectiveCycle(t *testing.T) {
	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})
	store := newRewardTestStore(t)
	for c := int64(1); c < 3; c++ {
		_ = store.WriteCycleVote(c, witness.Bytes(), 3)
		_ = store.WriteCycleReward(c, witness.Bytes(), 20)
	}
	_ = store.WriteWitnessVI(2, witness.Bytes(), new(big.Int))
	_ = store.WriteWitnessVI(4, witness.Bytes(), new(big.Int).Mul(big.NewInt(7), DecimalOfViReward))

	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(3)
	dp.SetAllowOldRewardOpt(true)

	votes := []VoteEntry{{Witness: witness, Count: 1}}
	if got := ComputeVoterReward(store, dp, votes, 1, 5); got != 20 {
		t.Fatalf("opt across E: got %d, want 20 (13 telescoped old + 7 new)", got)
	}
}

// TestComputeVoterRewardTVM_NoOldCycleSplit pins the Nile 34,621,401 fix: the TVM
// reward path (rewardBalance precompile 0x05 + WITHDRAWREWARD 0xD9) must mirror
// java VoteRewardUtil.computeReward — a PURE VI difference with NO old-cycle
// pro-rata — whereas the actuator/block path (ComputeVoterReward, java
// MortgageService.computeReward) DOES include the old segment. For a pre-fork
// voter both are reachable; go previously reused the split version on the TVM
// path, over-counting the reward by the old-cycle term and inflating any contract
// (e.g. a staked-TRX market) that reads the reward → a liquidate solvency check
// wrongly passed (REVERT "liquidate condition not met") where java liquidated.
func TestComputeVoterRewardTVM_NoOldCycleSplit(t *testing.T) {
	store := newRewardTestStore(t)
	dp := state.NewDynamicProperties()
	dp.SetNewRewardAlgorithmEffectiveCycle(5)
	witness := tcommon.BytesToAddress([]byte{0x41, 0x01})

	// Old cycles 3,4: 100 votes each, voter 40, pool 100 → 40/cycle → 80 old total.
	for c := int64(3); c < 5; c++ {
		_ = store.WriteCycleVote(c, witness.Bytes(), 100)
		_ = store.WriteCycleReward(c, witness.Bytes(), 100)
	}
	// New side: VI[2]=unwritten(0), VI[6]=2e18 → delta 2e18, voter 40 → 80.
	_ = store.WriteWitnessVI(6, witness.Bytes(), new(big.Int).Mul(big.NewInt(2), DecimalOfViReward))

	votes := []VoteEntry{{Witness: witness, Count: 40}}

	// Actuator/native path: old (80) + new VI[3..7) (VI[6]-VI[4]=2e18 → 80) = 160.
	if got := ComputeVoterReward(store, dp, votes, 3, 7); got != 160 {
		t.Fatalf("actuator (split) reward: got %d, want 160 (80 old + 80 new)", got)
	}
	// TVM path: pure VI over [3,7) = VI[6]-VI[2] = 2e18-0 → 80. NO old-cycle term.
	if got := ComputeVoterRewardTVM(store, votes, 3, 7); got != 80 {
		t.Fatalf("TVM (no-split) reward: got %d, want 80 (pure VI, no old pro-rata)", got)
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
