package hardfork_test

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/hardfork"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestIsActive_FalseByDefault(t *testing.T) {
	dp := state.NewDynamicProperties()
	if hardfork.IsActive(hardfork.AllowMarketTransaction, 0, dp) {
		t.Fatal("expected false when flag is 0")
	}
}

func TestIsActive_TrueAfterSet(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowMarketTransaction(true)
	if !hardfork.IsActive(hardfork.AllowMarketTransaction, 0, dp) {
		t.Fatal("expected true after enabling flag")
	}
}

func TestIsActive_NilDynProps(t *testing.T) {
	if hardfork.IsActive(hardfork.AllowMarketTransaction, 0, nil) {
		t.Fatal("expected false with nil DynProps")
	}
}

func TestIsActive_AllFlags(t *testing.T) {
	dp := state.NewDynamicProperties()
	testCases := []struct {
		flag   hardfork.AllowFlag
		setter func(bool)
	}{
		{hardfork.AllowSameTokenName, dp.SetAllowSameTokenName},
		{hardfork.AllowDelegateResource, dp.SetAllowDelegateResource},
		{hardfork.AllowAdaptiveEnergyLimit, dp.SetAllowAdaptiveEnergyLimit},
		{hardfork.AllowMultiSign, dp.SetAllowMultiSign},
		{hardfork.AllowChangeDelegation, dp.SetAllowChangeDelegation},
		{hardfork.AllowTvmTransferTrc10, dp.SetAllowTvmTransferTrc10},
		{hardfork.AllowTvmConstantinople, dp.SetAllowTvmConstantinople},
		{hardfork.AllowTvmSolidity059, dp.SetAllowTvmSolidity059},
		{hardfork.AllowTvmSolidity058, dp.SetAllowTvmSolidity058},
		{hardfork.AllowTvmIstanbul, dp.SetAllowTvmIstanbul},
		{hardfork.AllowMarketTransaction, dp.SetAllowMarketTransaction},
		{hardfork.AllowTvmFreeze, dp.SetAllowTvmFreeze},
		{hardfork.AllowTvmShieldedToken, dp.SetAllowTvmShieldedToken},
		{hardfork.AllowTvmVote, dp.SetAllowTvmVote},
		{hardfork.AllowAccountHistory, dp.SetAllowAccountHistory},
		{hardfork.AllowPbft, dp.SetAllowPbft},
		{hardfork.AllowStakingV2, dp.SetAllowStakingV2},
		{hardfork.AllowTvmLondon, dp.SetAllowTvmLondon},
		{hardfork.AllowTvmCompatibility, dp.SetAllowTvmCompatibility},
		{hardfork.AllowDynamicEnergy, dp.SetAllowDynamicEnergy},
		// AllowNewResourceModel skipped — no setter on DynamicProperties
		{hardfork.AllowEnergyAdjustment, dp.SetAllowEnergyAdjustment},
		{hardfork.AllowTvmBigInteger, dp.SetAllowTvmBigInteger},
		{hardfork.AllowTvmBlob, dp.SetAllowTvmBlob},
		{hardfork.AllowTvmCancun, dp.SetAllowTvmCancun},
	}
	for _, tc := range testCases {
		tc.setter(true)
		if !hardfork.IsActive(tc.flag, 0, dp) {
			t.Fatalf("IsActive(%v) should be true after enabling", tc.flag)
		}
		tc.setter(false)
		if hardfork.IsActive(tc.flag, 0, dp) {
			t.Fatalf("IsActive(%v) should be false after disabling", tc.flag)
		}
	}
}
