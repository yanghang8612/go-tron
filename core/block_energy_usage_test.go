package core

import (
	"testing"

	"github.com/tronprotocol/go-tron/actuator"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

// Tests for the M4 fix in docs/dev/proposal-hardfork-audit-2026-05-18.md:
// java-tron unconditionally bumps block_energy_usage by the stake-paid
// portion when adaptive is on, and only adds the balance-paid overflow
// after VERSION_3_6_5 passes. Go must match both tiers.

type forkStatsMem map[int32][]byte

func (m forkStatsMem) ReadForkStats(version int32) []byte {
	return m[version]
}

func (m forkStatsMem) WriteForkStats(version int32, stats []byte) {
	m[version] = append([]byte(nil), stats...)
}

// passVersion3_6_5 marks SR-vote tallies so that VERSION_3_6_5 passes for the
// supplied fork-stats store.
// VERSION_3_6_5 (value 9) uses the legacy "strict all-upgrade" check:
// every slot in the bitmap must read VoteUpgrade.
func passVersion3_6_5(store forks.ForkStatsStore, witnessCount int) {
	fc := forks.NewForkControllerFromStore(store)
	for slot := 0; slot < witnessCount; slot++ {
		fc.Update(9, slot, witnessCount)
	}
}

func TestAccumulateBlockEnergyUsage_AdaptiveOff_NoOp(t *testing.T) {
	stats := forkStatsMem{}
	dp := state.NewDynamicProperties()
	dp.SetAllowAdaptiveEnergy(false)
	dp.SetBlockEnergyUsage(7)

	accumulateBlockEnergyUsage(dp, stats, 0, &actuator.Result{
		EnergyUsageTotal:  1000,
		EnergyUsed:        600,
		OriginEnergyUsage: 100,
	})

	if got := dp.BlockEnergyUsage(); got != 7 {
		t.Fatalf("adaptive off: block_energy_usage = %d, want 7", got)
	}
}

func TestAccumulateBlockEnergyUsage_PreV3_6_5_StakeOnly(t *testing.T) {
	stats := forkStatsMem{}
	dp := state.NewDynamicProperties()
	dp.SetAllowAdaptiveEnergy(true)
	// No fork stats written → PassVersion(9) returns false.

	accumulateBlockEnergyUsage(dp, stats, 0, &actuator.Result{
		EnergyUsageTotal:  1000,
		EnergyUsed:        600,
		OriginEnergyUsage: 100,
	})

	// Only the stake portion (600+100) accumulates.
	if got := dp.BlockEnergyUsage(); got != 700 {
		t.Fatalf("pre-3_6_5: block_energy_usage = %d, want 700 (stake only)", got)
	}
}

func TestAccumulateBlockEnergyUsage_PostV3_6_5_FullUsage(t *testing.T) {
	stats := forkStatsMem{}
	dp := state.NewDynamicProperties()
	dp.SetAllowAdaptiveEnergy(true)
	passVersion3_6_5(stats, 27)

	accumulateBlockEnergyUsage(dp, stats, 0, &actuator.Result{
		EnergyUsageTotal:  1000,
		EnergyUsed:        600,
		OriginEnergyUsage: 100,
	})

	if got := dp.BlockEnergyUsage(); got != 1000 {
		t.Fatalf("post-3_6_5: block_energy_usage = %d, want 1000 (full)", got)
	}
}

func TestAccumulateBlockEnergyUsage_PostV3_6_5_StakeOnlyEqualsTotal(t *testing.T) {
	// Stake fully covered the tx: EnergyUsed == EnergyUsageTotal.
	// Pre- and post-3_6_5 must both add EnergyUsageTotal (no overflow).
	stats := forkStatsMem{}
	dp := state.NewDynamicProperties()
	dp.SetAllowAdaptiveEnergy(true)
	passVersion3_6_5(stats, 27)

	accumulateBlockEnergyUsage(dp, stats, 0, &actuator.Result{
		EnergyUsageTotal: 1000,
		EnergyUsed:       1000,
	})
	if got := dp.BlockEnergyUsage(); got != 1000 {
		t.Fatalf("stake-only: block_energy_usage = %d, want 1000", got)
	}
}

func TestAccumulateBlockEnergyUsage_ZeroUsage_NoOp(t *testing.T) {
	stats := forkStatsMem{}
	dp := state.NewDynamicProperties()
	dp.SetAllowAdaptiveEnergy(true)
	dp.SetBlockEnergyUsage(42)

	accumulateBlockEnergyUsage(dp, stats, 0, &actuator.Result{
		EnergyUsageTotal: 0,
	})
	if got := dp.BlockEnergyUsage(); got != 42 {
		t.Fatalf("zero usage tx: block_energy_usage = %d, want 42", got)
	}
}
