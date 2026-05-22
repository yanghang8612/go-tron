package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
)

// Tests covering the proposal-side fixes from
// docs/dev/proposal-hardfork-audit-2026-05-18.md.

// approvedProposal returns a 3-of-4 approved Proposal with the given
// parameters. Helper to keep the tests below readable.
func approvedProposal(id int64, params map[int64]int64) *rawdb.Proposal {
	return &rawdb.Proposal{
		ID:             id,
		Parameters:     params,
		ExpirationTime: 2000,
		Approvals: []tcommon.Address{
			{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03},
		},
		State: rawdb.ProposalStatePending,
	}
}

func auditActiveSet() []tcommon.Address {
	return []tcommon.Address{{0x41, 0x01}, {0x41, 0x02}, {0x41, 0x03}, {0x41, 0x04}}
}

// C2: proposal #33 stores 24 * 60 * value and recomputes target_limit
// against the new ratio.
func TestProcessProposals_C2_AdaptiveRatioMultiplier(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()

	// Pre-state: default total_energy_limit = 50_000_000_000.
	if got := dp.TotalEnergyLimit(); got != 50_000_000_000 {
		t.Fatalf("precondition: total_energy_limit = %d, want 50_000_000_000", got)
	}

	// Propose target_ratio = 20. java stores ratio = 24*60*20 = 28800 and
	// target_limit = 50_000_000_000 / 28800 = 1_736_111.
	p := approvedProposal(0, map[int64]int64{33: 20})
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})

	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	if got := dp.AdaptiveResourceLimitTargetRatio(); got != 28800 {
		t.Fatalf("ratio: got %d, want 28800 (24*60*20)", got)
	}
	if got := dp.TotalEnergyTargetLimit(); got != 50_000_000_000/28800 {
		t.Fatalf("target_limit: got %d, want %d", got, int64(50_000_000_000/28800))
	}
}

// C3+C4: proposal #59 locks the new-reward effective cycle. A subsequent
// #67 must NOT overwrite it (idempotent saveX).
func TestProcessProposals_C3C4_TvmVoteLocksEffectiveCycle(t *testing.T) {
	const maxInt64 = int64(9_223_372_036_854_775_807)

	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	dp.SetCurrentCycleNumber(42)

	// Sanity: default disables UseNewRewardAlgorithm.
	if dp.UseNewRewardAlgorithm() {
		t.Fatal("UseNewRewardAlgorithm should be false at genesis")
	}
	if got := dp.NewRewardAlgorithmEffectiveCycle(); got != maxInt64 {
		t.Fatalf("effective cycle: got %d, want MaxInt64", got)
	}

	// Proposal #59 (ALLOW_TVM_VOTE) → should set cycle = 43.
	p1 := approvedProposal(0, map[int64]int64{59: 1})
	statedb.WriteProposal(0, p1)
	statedb.WriteProposalIndex([]int64{0})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got := dp.NewRewardAlgorithmEffectiveCycle(); got != 43 {
		t.Fatalf("after #59: effective cycle = %d, want 43", got)
	}

	// Advance the cycle, then approve #67 (ALLOW_NEW_REWARD). The guarded
	// SaveNewRewardAlgorithmEffectiveCycle must be a no-op — effective cycle
	// stays at 43, not 51 (= 50+1).
	dp.SetCurrentCycleNumber(50)
	p2 := approvedProposal(1, map[int64]int64{67: 1})
	statedb.WriteProposal(1, p2)
	statedb.WriteProposalIndex([]int64{0, 1})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3001, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got := dp.NewRewardAlgorithmEffectiveCycle(); got != 43 {
		t.Fatalf("after #67: effective cycle = %d, want 43 (locked by #59)", got)
	}
}

// C5: proposal #19 routes to SetTotalEnergyLimit (v2 semantics) — both
// target_limit and current_limit get refreshed when adaptive energy is off.
func TestProcessProposals_C5_TotalCurrentEnergyLimitRoutes(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()

	// Adaptive energy OFF (default) → current_limit follows total_limit.
	p := approvedProposal(0, map[int64]int64{19: 80_000_000_000})
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	if got := dp.TotalEnergyLimit(); got != 80_000_000_000 {
		t.Fatalf("total_limit: got %d, want 80_000_000_000", got)
	}
	wantTarget := int64(80_000_000_000) / dp.AdaptiveResourceLimitTargetRatio()
	if got := dp.TotalEnergyTargetLimit(); got != wantTarget {
		t.Fatalf("target_limit: got %d, want %d", got, wantTarget)
	}
	if got := dp.TotalEnergyCurrentLimit(); got != 80_000_000_000 {
		t.Fatalf("current_limit: got %d, want 80_000_000_000 (v2 path)", got)
	}
}

// M1: re-approving #21 when AllowAdaptiveEnergy is already on must NOT
// overwrite a target_ratio/multiplier that has since been moved by #33.
func TestProcessProposals_M1_AdaptiveEnergyReapprovalNoop(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()
	// Pretend #21 + #33 already ran: AllowAdaptiveEnergy=1, ratio=43200 (#33
	// with value=30 → 24*60*30).
	dp.SetAllowAdaptiveEnergy(true)
	dp.SetAdaptiveResourceLimitTargetRatio(43200)
	dp.SetAdaptiveResourceLimitMultiplier(123)

	// Now re-approve #21. java would short-circuit the whole block; go must
	// match — ratio/multiplier must stay at the post-#33 values.
	p := approvedProposal(0, map[int64]int64{21: 1})
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got := dp.AdaptiveResourceLimitTargetRatio(); got != 43200 {
		t.Fatalf("ratio overwritten by re-approval: got %d, want 43200", got)
	}
	if got := dp.AdaptiveResourceLimitMultiplier(); got != 123 {
		t.Fatalf("multiplier overwritten by re-approval: got %d, want 123", got)
	}
}

// C5: proposal #17 routes to SetTotalEnergyLimitV1 (target_limit
// refreshed, current_limit NOT touched). #17 is fork-locked on mainnet
// but still reachable for early replay / private chains.
func TestProcessProposals_C5_TotalEnergyLimitV1Routes(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()

	// Baseline currentLimit must NOT change after the proposal even though
	// java's saveTotalEnergyLimit (v1) refreshes target_limit. Pre-seed
	// current_limit to a sentinel so we can detect any accidental overwrite.
	dp.SetTotalEnergyCurrentLimit(123_456)

	p := approvedProposal(0, map[int64]int64{17: 80_000_000_000})
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	if got := dp.TotalEnergyLimit(); got != 80_000_000_000 {
		t.Fatalf("total_limit: got %d, want 80_000_000_000", got)
	}
	wantTarget := int64(80_000_000_000) / dp.AdaptiveResourceLimitTargetRatio()
	if got := dp.TotalEnergyTargetLimit(); got != wantTarget {
		t.Fatalf("target_limit: got %d, want %d", got, wantTarget)
	}
	if got := dp.TotalEnergyCurrentLimit(); got != 123_456 {
		t.Fatalf("current_limit: got %d, want 123_456 (v1 path must not touch)", got)
	}
}

// M1: same guard semantics for #20 (ALLOW_MULTI_SIGN). Once enabled, a
// later proposal trying to re-approve must NOT write — java's
// `if (getAllowMultiSign()==0)` short-circuits.
func TestProcessProposals_M1_MultiSignReapprovalNoop(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()

	// Pre-state: flag is already on with a sentinel value.
	dp.Set("allow_multi_sign", 42) // non-zero sentinel

	p := approvedProposal(0, map[int64]int64{20: 1})
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	got, _ := dp.Get("allow_multi_sign")
	if got != 42 {
		t.Fatalf("allow_multi_sign overwritten on re-approval: got %d, want 42", got)
	}
}

// M3: proposal #44 with value=1 adds permission bits 52 (MarketSellAsset)
// and 53 (MarketCancelOrder). Together with M1 this is the only path
// adding these bits — must run on first activation.
func TestProcessProposals_M3_MarketTransactionAddsBits52_53(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()

	beforeOps := dp.ActiveDefaultOperations()
	if (beforeOps[52/8]>>(52%8))&1 != 0 {
		t.Fatalf("precondition: bit 52 should be unset")
	}
	if (beforeOps[53/8]>>(53%8))&1 != 0 {
		t.Fatalf("precondition: bit 53 should be unset")
	}

	p := approvedProposal(0, map[int64]int64{44: 1})
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	afterOps := dp.ActiveDefaultOperations()
	if bit := (afterOps[52/8] >> (52 % 8)) & 1; bit != 1 {
		t.Fatalf("bit 52 should be set after #44 v=1, got %08b at byte 6", afterOps[6])
	}
	if bit := (afterOps[53/8] >> (53 % 8)) & 1; bit != 1 {
		t.Fatalf("bit 53 should be set after #44 v=1, got %08b at byte 6", afterOps[6])
	}
}

// M3: proposal #30 with value=0 still adds permission bit 49 (java's
// ALLOW_CHANGE_DELEGATION case is unguarded).
func TestProcessProposals_M3_ChangeDelegationValueZeroStillAddsBit49(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	statedb := newTestStateDB(t)
	dp := state.NewDynamicProperties()

	// Snapshot the default active_default_operations bitmap; bit 49 should
	// not yet be set on a fresh DP.
	beforeOps := dp.ActiveDefaultOperations()
	beforeBit := (beforeOps[49/8] >> (49 % 8)) & 1
	if beforeBit != 0 {
		t.Fatalf("precondition: bit 49 should be unset, beforeOps[6]=%08b", beforeOps[6])
	}

	p := approvedProposal(0, map[int64]int64{30: 0})
	statedb.WriteProposal(0, p)
	statedb.WriteProposalIndex([]int64{0})
	if err := ProcessProposals(db, statedb, dp, auditActiveSet(), 3000, nil); err != nil {
		t.Fatalf("unexpected: %v", err)
	}

	afterOps := dp.ActiveDefaultOperations()
	afterBit := (afterOps[49/8] >> (49 % 8)) & 1
	if afterBit != 1 {
		t.Fatalf("bit 49 should be set after #30 (even with value=0), got %08b at byte 6", afterOps[6])
	}
}
