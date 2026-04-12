package forks_test

import (
	"testing"

	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
)

func TestIsActive_FalseByDefault(t *testing.T) {
	dp := state.NewDynamicProperties()
	if forks.IsActive(forks.AllowMarketTransaction, 0, dp) {
		t.Fatal("expected false when flag is 0")
	}
}

func TestIsActive_TrueAfterSet(t *testing.T) {
	dp := state.NewDynamicProperties()
	dp.SetAllowMarketTransaction(true)
	if !forks.IsActive(forks.AllowMarketTransaction, 0, dp) {
		t.Fatal("expected true after enabling flag")
	}
}

func TestIsActive_NilDynProps(t *testing.T) {
	if forks.IsActive(forks.AllowMarketTransaction, 0, nil) {
		t.Fatal("expected false with nil DynProps")
	}
}

func TestIsActive_AllFlags(t *testing.T) {
	dp := state.NewDynamicProperties()
	testCases := []struct {
		flag   forks.AllowFlag
		setter func(bool)
	}{
		{forks.AllowSameTokenName, dp.SetAllowSameTokenName},
		{forks.AllowDelegateResource, dp.SetAllowDelegateResource},
		{forks.AllowAdaptiveEnergyLimit, dp.SetAllowAdaptiveEnergyLimit},
		{forks.AllowMultiSign, dp.SetAllowMultiSign},
		{forks.AllowChangeDelegation, dp.SetAllowChangeDelegation},
		{forks.AllowTvmTransferTrc10, dp.SetAllowTvmTransferTrc10},
		{forks.AllowTvmConstantinople, dp.SetAllowTvmConstantinople},
		{forks.AllowTvmSolidity059, dp.SetAllowTvmSolidity059},
		{forks.AllowTvmSolidity058, dp.SetAllowTvmSolidity058},
		{forks.AllowTvmIstanbul, dp.SetAllowTvmIstanbul},
		{forks.AllowMarketTransaction, dp.SetAllowMarketTransaction},
		{forks.AllowTvmFreeze, dp.SetAllowTvmFreeze},
		{forks.AllowTvmShieldedToken, dp.SetAllowTvmShieldedToken},
		{forks.AllowTvmVote, dp.SetAllowTvmVote},
		{forks.AllowAccountHistory, dp.SetAllowAccountHistory},
		{forks.AllowPbft, dp.SetAllowPbft},
		{forks.AllowStakingV2, dp.SetAllowStakingV2},
		{forks.AllowTvmLondon, dp.SetAllowTvmLondon},
		{forks.AllowTvmCompatibility, dp.SetAllowTvmCompatibility},
		{forks.AllowDynamicEnergy, dp.SetAllowDynamicEnergy},
		// AllowNewResourceModel skipped — no setter on DynamicProperties
		{forks.AllowEnergyAdjustment, dp.SetAllowEnergyAdjustment},
		{forks.AllowTvmBigInteger, dp.SetAllowTvmBigInteger},
		{forks.AllowTvmBlob, dp.SetAllowTvmBlob},
		{forks.AllowTvmCancun, dp.SetAllowTvmCancun},
	}
	for _, tc := range testCases {
		tc.setter(true)
		if !forks.IsActive(tc.flag, 0, dp) {
			t.Fatalf("IsActive(%v) should be true after enabling", tc.flag)
		}
		tc.setter(false)
		if forks.IsActive(tc.flag, 0, dp) {
			t.Fatalf("IsActive(%v) should be false after disabling", tc.flag)
		}
	}
}
