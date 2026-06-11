package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestProcessProposals_AdaptiveEnergySideEffect(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetTotalEnergyLimit(50_000_000_000)
	fc := forks.NewForkControllerFromState(statedb)

	// Simulate VERSION_3_6_5 (value=9) having passed with all slots voting upgrade.
	stats := make([]byte, 27)
	for i := range stats {
		stats[i] = forks.VoteUpgrade
	}
	statedb.WriteForkStats(9, stats)

	// Create proposal to enable adaptive energy (proposal ID 21).
	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{21: 1},
		ExpirationTime: 2000,
		Approvals: []tcommon.Address{
			{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03},
		},
		State: rawdb.ProposalStatePending,
	}
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})

	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
	if err := ProcessProposals(db, statedb, dynProps, active, 3000, fc, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !dynProps.AllowAdaptiveEnergy() {
		t.Fatal("adaptive energy should be enabled")
	}
	if got := dynProps.AdaptiveResourceLimitTargetRatio(); got != 2880 {
		t.Fatalf("targetRatio: got %d, want 2880", got)
	}
	if got := dynProps.AdaptiveResourceLimitMultiplier(); got != 50 {
		t.Fatalf("multiplier: got %d, want 50", got)
	}
	if got := dynProps.TotalEnergyTargetLimit(); got != 50_000_000_000/2880 {
		t.Fatalf("targetLimit: got %d, want %d", got, 50_000_000_000/2880)
	}
}

func TestProcessProposals_AllowNewRewardActivates(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetCurrentCycleNumber(42)

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{67: 1}, // ALLOW_NEW_REWARD
		ExpirationTime: 2000,
		Approvals: []tcommon.Address{
			{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03},
		},
		State: rawdb.ProposalStatePending,
	}
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})

	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
	if err := ProcessProposals(db, statedb, dynProps, active, 3000, nil, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !dynProps.AllowNewReward() {
		t.Fatal("allow_new_reward should be on")
	}
	if got := dynProps.NewRewardAlgorithmEffectiveCycle(); got != 43 {
		t.Fatalf("effective cycle: got %d, want 43 (currentCycle+1)", got)
	}
	if !dynProps.UseNewRewardAlgorithm() {
		t.Fatal("UseNewRewardAlgorithm should report true after activation")
	}
}

func TestProcessProposals_AdaptiveEnergyNoSideEffectWithoutVersion(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetAdaptiveResourceLimitTargetRatio(10)
	dynProps.SetAdaptiveResourceLimitMultiplier(1000)
	fc := forks.NewForkControllerFromState(statedb)
	// No VERSION_3_6_5 fork stats → fc.Pass(9, ...) returns false.

	p := &rawdb.Proposal{
		ID:             0,
		Parameters:     map[int64]int64{21: 1},
		ExpirationTime: 2000,
		Approvals: []tcommon.Address{
			{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03},
		},
		State: rawdb.ProposalStatePending,
	}
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})

	active := []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
	if err := ProcessProposals(db, statedb, dynProps, active, 3000, fc, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Adaptive energy enabled, but side-effects NOT applied (version gate not met).
	if !dynProps.AllowAdaptiveEnergy() {
		t.Fatal("adaptive energy should be enabled")
	}
	if got := dynProps.AdaptiveResourceLimitTargetRatio(); got != 10 {
		t.Fatalf("targetRatio should be unchanged: got %d, want 10", got)
	}
	if got := dynProps.AdaptiveResourceLimitMultiplier(); got != 1000 {
		t.Fatalf("multiplier should be unchanged: got %d, want 1000", got)
	}
}
